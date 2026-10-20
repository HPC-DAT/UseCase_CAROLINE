"""
This script runs the original PS DePSI workflow as originally implemented in Matlab.
"""

import atexit
import logging
import os
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import fsspec
import numpy as np
import xarray
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
from dcachefs import dCacheFileSystem
from depsi.arc_estimation import periodogram
from depsi.atmosphere_estimation import estimate_atmosphere_phase
from depsi.classification import network_stm_selection, ps_selection
from depsi.densification import densification
from depsi.io import (
    export_convex_hull_to_shapefile,
    export_to_csv,
    export_to_shapefile,
    read_slc_stack,
)
from depsi.model_estimation import MODEL_NAMES_PARAMS, estimate_model_params
from depsi.network import form_network, spatial_integration
from depsi.point_quality import compute_spatiotemporal_consistency, detect_side_lobes
from depsi.postprocessing import stm_point_filter
from depsi.transformations import radar_to_latlonh
from depsi.utils import (
    add_stm_time_deltas,
    convert_geographic_coords_to_euclidean,
    crop_slc_spacetime,
    stm_compute_single_time_differences,
)
from depsi.viewing_geometry import add_local_viewing_geometry

import sarxarray

# Make a logger to log the stages of processing
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()  # create console handler
ch.setLevel(logging.INFO)
logger.addHandler(ch)

##### Environment configurations #####
## Configuration 1: Use threading or SLURM Dask distributed cluster
# USE_SLURM_CLUSTER = False # Use threading
USE_SLURM_CLUSTER = True  # useSLURM cluster


# Uncomment the following if USE_SLURM_CLUSTER = True
# Uncomment start for USE_SLURM_CLUSTER = True
def get_free_port():
    """Get a non-occupied port number."""
    sock = socket.socket()
    sock.bind(("", 0))  # Bind a port, it will be busy now
    freesock = sock.getsockname()[1]  # get the port number
    sock.close()  # Free the port, so it can be used later
    return freesock


N_WORKERS = 32  # Number of Dask workers to launch
EXPECTED_WORKERS = 18  # Expected number of Dask workers to be available
TIMEOUT_MIN = 30  # Timeout in minutes for workers to become available
CORES_PER_WORKER = 4  # Number of cores per Dask worker
PARTITION_WORKER = "normal"  # Name of the node partition on your SLURM system
WALLTIME_WORKER = "0:10:00"  # Walltime for each Dask worker
worker_job_script = []  # By default, no additional job script commands are provided
FREE_SOCKET = get_free_port()  # Get a free port to host the Dask dashboard
PYTHON = sys.executable  # Path to the Python interpreter to use for Dask workers
# Uncomment end for USE_SLURM_CLUSTER = True

# Configuration 2: Use Python env from a common .venv, or tarball-based virtual environment
# This configuration is only relevant when using a SLURM cluster
# When using threading, the virtual environment is handled by SLURM job, so USE_TAR_VENV = False
# USE_TAR_VENV = False # Use common .venv
USE_TAR_VENV = True  # Use tarball-based virtual environment
# Uncomment the following lines to extract the virtual environment on each worker
# Uncomment start for USE_TAR_VENV = True
PATH_VENV_TAR = (
    "/home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/DePSI/env.tar"
)
SHM_BASE = "/dev/shm/caroline-oku"
RUN_ID = os.environ.get("SLURM_JOB_ID")
PATH_VENV_SHM = f"{SHM_BASE}/venv-{RUN_ID}"
LOCK_FILE = f"{SHM_BASE}/.venv_{RUN_ID}.lock"
PYTHON = (
    f"{PATH_VENV_SHM}/bin/python"  # Point Python to the extracted virtual environment, they are in the same path per worker node
)
worker_job_script = [
    f'PATH_VENV_TAR="{PATH_VENV_TAR}"',
    f'SHM_BASE="{SHM_BASE}"',
    f'RUN_ID="{RUN_ID}"',
    'PATH_VENV_SHM="${SHM_BASE}/venv-${RUN_ID}"',
    f'PYTHON="{PYTHON}"',
    'READY_FILE="${PATH_VENV_SHM}/.ready"',
    'LOCK_FILE="${SHM_BASE}/.venv_${RUN_ID}.lock"',
    'mkdir -p "${SHM_BASE}"',
    'exec 9>"${LOCK_FILE}"',
    'flock -w 300 9 || { echo "Failed to acquire lock"; exit 1; }',
    'if [ ! -f "${READY_FILE}" ]; then rm -rf "${PATH_VENV_SHM}"; mkdir -p "${PATH_VENV_SHM}"; tar -xf "${PATH_VENV_TAR}" -C "${PATH_VENV_SHM}"; touch "${READY_FILE}"; fi',
]
# Uncomment end for USE_TAR_VENV = True


##### INPUT VARIABLES

start_at_checkpoint = 0  # 0 from start, 1 post atmosphere estimation, 2 post densification, 3 for output generation

# Path to the SLC stack
# Option 1: Use a stack stored on project space
# slc_path = "/project/caroline/Share/stacks_zarr/nl_groningen_s1_dsc_t037/nl_groningen_s1_dsc_t037.zarr"
# Option 2: Use a stack stored on dCache
slc_path_dcache = "dcache://disk/Data/ou_dcache_benchmark/nl_groningen_s1_dsc_t037.zarr"
slc_path = fsspec.get_mapper(
    slc_path_dcache,
    api_url="https://dcacheview.grid.surfsara.nl:22880/api/v1",
    webdav_url="https://webdav.grid.surfsara.nl:443/",
)

# Crop in space
aoi_file = "/home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/groningen-city-aoi/groningen-city.shp"

# Output dir
output_dir = Path(
    "/home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/output_400k_dcache"
)
output_dir.mkdir(exist_ok=True)

# metadata
metadata_path = (
    "/project/caroline/Share/stacks/nl_groningen_s1_dsc_t037/stack/20200328/slave.res"
)
satellite = "s1"
track = 37
direction = "dsc"

# Crop in time
first_date = datetime(2020, 1, 1)
last_date = datetime(2021, 1, 1)

mother_epoch = datetime(2020, 3, 28)
reference_point_index = None

# PS selection method
ps_selection_method = "nad"
threshold = 0.45  # smaller = more strict quality threshold
chunks_ps_selection = 10000
start_date_ps_selection = None
end_date_ps_selection = None

# Sidelobes
do_sidelobe_detection = False
max_pixel_dist = 2
min_correlation = 0.90

# Network formation
network_crs = "radar"
network_x_crds = "azimuth"
network_y_crds = "range"
min_point_distance = 500  # meters
max_arc_length = 0.045  # degrees (lat/lon)
network_formation_method = "redundant"
min_network_links = 16
network_partition_number = 8
network_dphase_method = "subtract"
min_periodogram_iterations = 10
arc_quality_threshold = 0.5  # ensemble coherence
model_types = ["offset", "velocity", "height"]


# atmosphere
euclidean_epsg_code_number = 28992  # 28992 is Dutch RD, for other AoIs visit https://epsg.io/ , units must be meters
atmo_unmodeled_displacement_filter_length = 1
atmo_unmodeled_displacement_sampling_rate = 50
atmo_unmodeled_displacement_filter_type = "gaussian"

atmo_kriging_n_nearest_neighbours = None
atmo_kriging_backend = "vectorized"

atmo_empirical_variogram_method = "standard"
atmo_empirical_variogram_nlags = 50
atmo_empirical_variogram_cutoff_distance = 10000.0

atmo_variogram_model = "gaussian"
atmo_variogram_drift_terms = "regional_linear"

# Densification
chunks_densification = (
    2000  # Chunkzise before densification to facilitate parallel processing
)
n_densification_connections = 1

# Spatio-temporal consistency
stc_min_dist = 30  # meters
stc_max_dist = 100  # meters

# # Viewing geometry
# Skipped because cannot find the file
# orbit_mode = "IWS"
# orbit_resolution = 0.01
# orbit_file = "/Users/sanvandiepen/PycharmProjects/workingEnvironment2/GitHub_repos/DePSI_group/config/drama/S1_XTI.cfg"

# output settings
filter_dict = {"h": [-2000, 2000]}
output_types = ["csv_web_portal", "csv", "shapefile", "convex_hull"]
# csv, csv_web_portal, shapefile, convex_hull, zarr

# csv export
csv_save_path = output_dir / "nl_groningen_s1_dsc_t037.csv"
csv_save_path = csv_save_path.as_posix()
csv_ts_proj = "los"
csv_point_annotation_label = f"nl_groningen_{satellite}_{direction}_t{track:0>3d}"

# csv web portal export
csv_web_save_path = output_dir / "nl_groningen_s1_dsc_t037_portal_val.csv"
csv_web_save_path = csv_web_save_path.as_posix()
csv_web_ts_proj = "los"
csv_web_point_annotation_label = f"nl_groningen_{satellite}_{direction}_t{track:0>3d}"

# shapefile export
shape_save_path = output_dir / "nl_groningen_s1_dsc_t037.shp"
shape_save_path = shape_save_path.as_posix()
shape_projection = "RD"  # RD or WGS84
shape_point_annotation_label = f"nl_groningen_{satellite}_{direction}_t{track:0>3d}"

# convex hull export
chull_save_path = output_dir / "nl_groningen_s1_dsc_t037_chull.shp"
chull_save_path = chull_save_path.as_posix()
chull_projection = "RD"  # RD or WGS84

# zarr export & checkpoints (NEEDS {}!)
stm_save_path = output_dir / "nl_groningen_s1_dsc_t037_{}.zarr"
stm_save_path = stm_save_path.as_posix()

# Processing part
if __name__ == "__main__":
    if USE_SLURM_CLUSTER:
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Initiating dask distributed cluster..."
        )
        # Cluster configuration
        cluster = SLURMCluster(
            name="dask-worker",  # Name of the Slurm job
            queue=PARTITION_WORKER,  # Name of the node partition on your SLURM system
            cores=CORES_PER_WORKER,  # Number of cores per worker
            processes=1,  # Number of Python processes per worker
            walltime=WALLTIME_WORKER,  # Reserve each worker for X hour
            scheduler_options={
                "dashboard_address": f":{FREE_SOCKET}"
            },  # Host Dashboard in a free socket
            python=PYTHON,  # force worker interpreter
            job_script_prologue=worker_job_script,
        )
        cluster.scale(jobs=N_WORKERS)
        client = Client(cluster)
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Initiated {N_WORKERS} workers, expected {EXPECTED_WORKERS}, timeout {TIMEOUT_MIN} minutes"
        )
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Dask dashboard is available remotely at port {FREE_SOCKET}. You can access it by forwarding the Dask dashboard to your local. Example: ssh -N -L 8889:localhost:{FREE_SOCKET} user@spider.surf.nl , where 8889 is the local port."
        )
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} waiting for workers... ")
        client.wait_for_workers(n_workers=EXPECTED_WORKERS, timeout=f"{TIMEOUT_MIN}m")

        if USE_TAR_VENV:

            def cleanup_distributed_resources():
                """cleanup tared virtual environment on cluster nodes after the distributed computation is done"""
                print(
                    f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Closing dask client and cluster..."
                )
                client.close()
                cluster.close()

                print(
                    f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Purging extracted venv..."
                )
                os.makedirs(SHM_BASE, exist_ok=True)
                subprocess.run(
                    [
                        "bash",
                        "-lc",
                        f'exec 8>"{LOCK_FILE}"; flock -w 60 8; rm -rf "{PATH_VENV_SHM}"; rm -f "{LOCK_FILE}"',
                    ],
                    check=False,
                )

            # Cleanup distributed resources on exit
            atexit.register(cleanup_distributed_resources)

    # Read metadata
    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Reading Metadata...")
    metadata = sarxarray.read_metadata(metadata_path, driver="doris5")

    if start_at_checkpoint < 1:
        # 1. Project setup
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Reading SLC stack...")
        slcs = read_slc_stack(slc_path)

        # 2. Scatterer selection
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Start cropping...")
        cropped_slcs = crop_slc_spacetime(
            slcs, aoi_filename=aoi_file, start_date=first_date, end_date=last_date
        )

        # Simple test for data accessing
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Persisting data...")
        cropped_slcs = cropped_slcs.persist()
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Done!")

        # ######## POINT SELECTION WITH THE PARAMETERS ABOVE ############
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Starting PS selection..."
        )
        stm = ps_selection(
            cropped_slcs,
            method=ps_selection_method,
            threshold=threshold,
            ps_selection_start_date=start_date_ps_selection,
            ps_selection_end_date=end_date_ps_selection,
            output_chunks=chunks_ps_selection,
            mem_persist=False,
        )
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Selected {len(stm.space)} PS."
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Computing single differences and temporal baseline..."
        )
        stm = stm_compute_single_time_differences(
            stm, single_difference_mother=mother_epoch
        )
        stm = add_stm_time_deltas(stm)
        stm = stm.assign(
            {
                "temporal_baseline": (
                    ["time"],
                    stm.years_since_first_img.values
                    - stm.years_since_first_img.sel(time=stm.ps_sd_mother).values,
                )
            }
        )

        # sidelobe removal
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Removing sidelobes...")
        if do_sidelobe_detection:
            side_lobes_array, _ = detect_side_lobes(
                stm,
                max_pixel_dist,
                min_correlation,
                "sd_complex",
                "sd_amplitude_unnormalized",
            )

            mask_sidelobes = np.ones(len(stm.space), dtype=bool)
            mask_sidelobes[side_lobes_array] = False

        else:
            print(
                f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Skipping sidelobe detection..."
            )
            mask_sidelobes = np.ones(len(stm.space), dtype=bool)

        stm = stm.isel(space=mask_sidelobes)

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Removed {np.sum(~mask_sidelobes)} sidelobes from the STM, "
            f"{len(stm.space)} remain."
        )

        # 6a. Geocoding
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Starting geocoding...")
        stm = stm.assign_attrs({"wavelength": metadata["wavelength"]})

        latlonh = radar_to_latlonh(
            azimuth_coords=stm.azimuth.values,
            range_coords=stm.range.values,
            elevation=stm.h.values,
            metadata=metadata,
        )

        stm["lat"].data = latlonh[0].flatten()
        stm["lon"].data = latlonh[1].flatten()
        stm["h"].data = latlonh[2].flatten()

        # Add Euclidean coords in preparation for atmosphere estimation (here so it is also present in the first order network)
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Adding Euclidean coordinates..."
        )
        crd_x, crd_y = convert_geographic_coords_to_euclidean(
            stm["lon"].values,
            stm["lat"].values,
            target_crs=f"EPSG:{euclidean_epsg_code_number}",
        )
        stm = stm.assign_coords(
            {
                f"x_euclidean_proj_epsg{euclidean_epsg_code_number}": (
                    ["space"],
                    crd_x,
                ),
                f"y_euclidean_proj_epsg{euclidean_epsg_code_number}": (
                    ["space"],
                    crd_y,
                ),
            }
        )

        # 3a. Network construction
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Removing mother from network STM..."
        )
        mother_epoch_index = np.where(
            stm.time.values == stm.sel(time=stm.ps_sd_mother).time.values
        )[0][0]
        non_mother = [True] * len(stm.time.values)
        non_mother[mother_epoch_index] = False
        stm_without_mother_epoch = stm.isel(time=non_mother)

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Selecting first-order network points..."
        )
        stm_network_pnts = network_stm_selection(
            stm=stm_without_mother_epoch,
            min_dist=min_point_distance,
            crs=network_crs,
            azimuth_spacing=metadata["azimuth_pixel_spacing"],
            range_spacing=metadata["range_pixel_spacing"],
            sortby_var="time_selection_" + ps_selection_method,
            x_var=network_x_crds,
            y_var=network_y_crds,
            include_index=None,
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Forming first-order network..."
        )
        stm_network_arcs = form_network(
            stm_network_pnts,
            key_phase="sd_phase",
            key_h2ph="h2ph",
            key_Btemporal="temporal_baseline",
            max_length=max_arc_length,
            key_xcrds="lon",
            key_ycrds="lat",
            network_method=network_formation_method,
            min_links=min_network_links,
            num_partitions=network_partition_number,
            dphase_method=network_dphase_method,
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Formed first-order network with {len(stm_network_pnts.space)} "
            f"points and {len(stm_network_arcs.space)} arcs."
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Estimating ambiguities through periodogram..."
        )
        _, ambiguities, _, _, ens_coh = periodogram(
            stm_network_arcs,
            key_dphase="d_phase",
            key_h2ph="h2ph",
            key_Btemporal="Btemp",
            min_steps=min_periodogram_iterations,
        )
        stm_network_arcs["ambiguities"] = ambiguities
        stm_network_arcs["temp_coh"] = ens_coh

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Computing periodogram output..."
        )
        stm_network_pnts = stm_network_pnts.compute()
        stm_network_arcs = stm_network_arcs.compute()

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Integrating the spatial network..."
        )
        _, stm_pnts_output = spatial_integration(
            stm_network_pnts,
            stm_network_arcs,
            key_arc_quality="temp_coh",
            threshold_arc_quality=0.6,  # mannually updated here
            idx_refpnt=reference_point_index,
        )

        # 4. Atmosphere estimation
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Estimating the mother atmosphere..."
        )
        stm_pnts_output, _ = estimate_model_params(
            stm=stm_pnts_output,
            models=model_types,
            key_observations="unwrapped_phase",
            key_h2ph="sd_h2ph",
            key_time="temporal_baseline",
        )

        stm_pnts_output = stm_pnts_output.rename_vars(
            {
                "pnt_offset": "mother_atmosphere",
            }
        )
        # Rename the space dimension since it will otherwise clash with the space dimension of the predicted coordinates
        stm_pnts_output = stm_pnts_output.rename_dims({"space": "space_fon"})

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Creating the atmosphere prediction coordinate dataset..."
        )
        atmosphere_prediction_coords = xarray.Dataset(
            coords={
                f"x_euclidean_proj_epsg{euclidean_epsg_code_number}": (
                    "space",
                    stm[f"x_euclidean_proj_epsg{euclidean_epsg_code_number}"].values,
                ),
                f"y_euclidean_proj_epsg{euclidean_epsg_code_number}": (
                    "space",
                    stm[f"y_euclidean_proj_epsg{euclidean_epsg_code_number}"].values,
                ),
            }
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Estimating the atmospheric phase screens..."
        )
        atmospheric_phase_screens = estimate_atmosphere_phase(
            stm=stm_pnts_output,
            prediction_coords=atmosphere_prediction_coords,
            psc_phase_residuals="phase_residuals",
            atmosphere_mother="mother_atmosphere",
            key_Btemporal="temporal_baseline",
            unmodeled_displacement_args={
                "filter_length": atmo_unmodeled_displacement_filter_length,
                "sampling_rate": atmo_unmodeled_displacement_sampling_rate,
                "filter_type": atmo_unmodeled_displacement_filter_type,
            },
            kriging_args={
                "n_nearest_neighbors": atmo_kriging_n_nearest_neighbours,
                "kriging_backend": atmo_kriging_backend,
                "empirical_variogram_args": {
                    "method": atmo_empirical_variogram_method,
                    "nlags": atmo_empirical_variogram_nlags,
                    "cutoff": atmo_empirical_variogram_cutoff_distance,
                },
                "variogram_args": {
                    "variogram_model": atmo_variogram_model,
                    "variogram_parameters": None,  # to be estimated from the empirical variogram
                    "drift_terms": atmo_variogram_drift_terms,
                },
            },
            stm_coords_metadata={
                "mode": "euclidean",
                "x_label": f"x_euclidean_proj_epsg{euclidean_epsg_code_number}",
                "y_label": f"y_euclidean_proj_epsg{euclidean_epsg_code_number}",
                "space_dim_name": "space_fon",
            },
            prediction_coords_metadata={
                "mode": "euclidean",
                "x_label": f"x_euclidean_proj_epsg{euclidean_epsg_code_number}",
                "y_label": f"y_euclidean_proj_epsg{euclidean_epsg_code_number}",
                "space_dim_name": "space",
            },
        )

        # create the zero layer for the mother atmosphere to be appended to the interferometric atmospheric phase screens
        mother_atmo_stm = xarray.Dataset(
            data_vars={
                "atmosphere_predicted": ("space", np.zeros((len(stm.space),))),
                "atmosphere_sigmasq": ("space", np.zeros((len(stm.space),))),
            },
            coords=stm["phase"].sel(time=stm.ps_sd_mother).coords,
        )

        atmosphere = xarray.concat(
            [atmospheric_phase_screens, mother_atmo_stm], dim="time"
        ).sortby("time")

        # Add the atmosphere into the original STM
        stm["atmosphere_predicted"] = atmosphere["atmosphere_predicted"].transpose(
            "space", "time"
        )
        stm["atmosphere_sigmasq"] = atmosphere["atmosphere_sigmasq"].transpose(
            "space", "time"
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Removing the atmospheric phase screens..."
        )
        stm["phase_minus_atmo"] = (
            stm["phase"] - stm["atmosphere_predicted"] + np.pi
        ) % (2 * np.pi) - np.pi
        stm["sd_phase_minus_atmo"] = (
            stm["phase_minus_atmo"]
            - stm["phase_minus_atmo"].sel(time=stm.ps_sd_mother)
            + np.pi
        ) % (2 * np.pi) - np.pi

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Checkpoint 1: saving to zarr..."
        )
        stm = stm.drop_encoding()
        stm = stm.chunk({"time": -1, "space": chunks_densification})
        stm.to_zarr(stm_save_path.format("4atmo"), mode="w")
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Checkpoint 1: Saved! Reloading..."
        )

    if start_at_checkpoint < 2:
        stm = xarray.open_zarr(stm_save_path.format("4atmo"))
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Checkpoint 1: Re-loaded!"
        )

        # 3b. Network construction
        mother_epoch_index = np.where(
            stm.time.values == stm.sel(time=stm.ps_sd_mother).time.values
        )[0][0]
        non_mother = [True] * len(stm.time.values)
        non_mother[mother_epoch_index] = False
        stm_atmo_corr_without_mother_epoch = stm.isel(time=non_mother)
        stm_atmo_corr_without_mother_epoch = stm_atmo_corr_without_mother_epoch.chunk(
            {"time": -1}
        ).unify_chunks()

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Selecting first-order network points..."
        )
        stm_network_pnts = network_stm_selection(
            stm=stm_atmo_corr_without_mother_epoch,
            min_dist=min_point_distance,
            crs=network_crs,
            azimuth_spacing=metadata["azimuth_pixel_spacing"],
            range_spacing=metadata["range_pixel_spacing"],
            sortby_var="time_selection_" + ps_selection_method,
            x_var=network_x_crds,
            y_var=network_y_crds,
            include_index=None,
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Forming first-order network..."
        )
        stm_network_arcs = form_network(
            stm_network_pnts,
            key_phase="sd_phase_minus_atmo",
            key_h2ph="h2ph",
            key_Btemporal="temporal_baseline",
            max_length=max_arc_length,
            key_xcrds="lon",
            key_ycrds="lat",
            network_method=network_formation_method,
            min_links=min_network_links,
            num_partitions=network_partition_number,
            dphase_method=network_dphase_method,
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Formed first-order network with {len(stm_network_pnts.space)} "
            f"points and {len(stm_network_arcs.space)} arcs."
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Estimating ambiguities through periodogram..."
        )
        _, ambiguities, _, _, ens_coh = periodogram(
            stm_network_arcs,
            key_dphase="d_phase",
            key_h2ph="h2ph",
            key_Btemporal="Btemp",
            min_steps=min_periodogram_iterations,
        )
        stm_network_arcs["ambiguities"] = ambiguities
        stm_network_arcs["temp_coh"] = ens_coh

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Computing periodogram output..."
        )

        stm_network_pnts = stm_network_pnts.compute()
        stm_network_arcs = stm_network_arcs.compute()

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Integrating the spatial network..."
        )
        _, stm_firstordernetwork = spatial_integration(
            stm_network_pnts,
            stm_network_arcs,
            key_arc_quality="temp_coh",
            threshold_arc_quality=arc_quality_threshold,
            skip_network_adaptation=True,
            idx_refpnt=reference_point_index,
        )

        # Force compute the network points
        stm_firstordernetwork = stm_firstordernetwork.compute()

        # 5. Densification
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Starting densification..."
        )
        stm_densified = densification(
            stm_atmo_corr_without_mother_epoch,
            stm_firstordernetwork,
            idx_refpnt=reference_point_index,
            n_connections=n_densification_connections,
            key_sdphase="sd_phase_minus_atmo",
            key_h2ph="sd_h2ph",
            key_Btemporal="temporal_baseline",
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Estimating the model parameters..."
        )
        stm_densified, model_parameter_layer_names = estimate_model_params(
            stm=stm_densified,
            models=model_types,
            key_observations="unwrapped_phase",
            key_h2ph="sd_h2ph",
            key_time="temporal_baseline",
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Checkpoint 2: saving to zarr..."
        )
        stm_densified = stm_densified.drop_encoding()
        stm_densified = stm_densified.chunk({"space": chunks_densification, "time": -1})
        stm_densified.to_zarr(stm_save_path.format("5dens"), mode="w")
        print(
            f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Checkpoint 2: Saved! Reloading..."
        )
