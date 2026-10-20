# Example DePSI Processing for Groningen

Expanded from: https://github.com/TUDelftGeodesy/DePSI/blob/main/scripts/depsi_main.py

This example script is summarized from a DePSI test case for benchmarking the performance of dCache vs project pace. The test is located on `/project/caroline/Share/users/caroline-oku/groningen-run`

## Expanded configuration on SURF Spider

1. Dask parallelizaton config: `USE_SLURM_CLUSTER`
    - `USE_SLURM_CLUSTER = False`: Use Dask threading. This means the computation will be constrained to 1 node with maximum 120 cores.
    - `USE_SLURM_CLUSTER = True`: Use a Dask SLURM distributed cluster. In this way, the computation can be distributed across multiple nodes, and for each Dask worker, a separate SLURM job will be submitted.

2. Virtual environment configuration: `USE_TAR_VENV`
    - `USE_TAR_VENV = False`: Use a common Python virtual environment located in `.venv`. This virtual environment is activated in the SLURM job script. 
        - When using a SLURM cluster (`USE_SLURM_CLUSTER = True` and `USE_TAR_VENV = False`), the python interpreter path needs to be passed to the SLURM cluster.
        
            ```python
                PYTHON = sys.executable # assume the SLURM job already activated the virtual environment
                cluster = SLURMCluster(
                    ...
                    python=PYTHON,  # force worker interpreter
                    ...
                )
            ```

    - `USE_TAR_VENV = True`: Use a tarball-based virtual environment. This allows the Python environment to be imported much faster, since the tarballed environment will be extracted to the shared memory space `/dev/shm/`. This is a path which is actually memory-backed, but looks like a disk space. This allows one to manage the virtual environment in a normal `.venv` directory (e.g., using `uv`), and only tarball it when the environment is updated.

    In the SLURM job script, the tarball will be extracted on the node where the main SLURM job is running. Then, depending on the configuration of `USE_SLURM_CLUSTER`, two scenarios can occur:
        - `USE_SLURM_CLUSTER = False`: When using threading, `sys.executable` will point to the extracted tarball-based virtual environment on the main SLURM job node.
        - `USE_SLURM_CLUSTER = True`: When using a SLURM cluster, each Dask worker will need its own extracted tarball-based virtual environment on the respective worker node. This is handled by the extended SLURM job script when initiating the SLURM cluster.
            ```python
                PATH_VENV_TAR = (
                    "PATH/TO/VENV_TAR"
                )
                SHM_BASE = "/dev/shm/XXXXXX"
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
            
                cluster = SLURMCluster(
                    ...
                    python=PYTHON,  # force worker interpreter
                    job_script_prologue=worker_job_script,
                    ...
                )
            ```
3. SLC stack path configuration

    The SLC stack can be stored either on the project space or on dCache. Depending on the storage location, the path configuration will differ:

    - Project space:
        ```python
        slc_path = "/project/caroline/Share/stacks_zarr/nl_groningen_s1_dsc_t037/nl_groningen_s1_dsc_t037.zarr"
        ```
    - dCache:
        Two libraries, `fsspec` and `dcachefs` are needed to access the SLC stack stored on dCache.
        ```python
        slc_path_dcache = "dcache://disk/Data/ou_dcache_benchmark/nl_groningen_s1_dsc_t037.zarr"
        slc_path = fsspec.get_mapper(
            slc_path_dcache,
            api_url="https://dcacheview.grid.surfsara.nl:22880/api/v1",
            webdav_url="https://webdav.grid.surfsara.nl:443/",
        )
        ```
## Further recommendations

- There is an known issue with the arc adjustment step in `spatial_integration`. For epochs with a lot of errors, the adjustment will never finish. See https://github.com/TUDelftGeodesy/DePSI/issues/125 . For now, I recommend:
    - Increase the quality threshold for network points and arcs.
    - Using a trial-and-error approach to identify problematic epochs and remove them from processing.