"""
Simple script to profile the Python dependency importing
"""
from datetime import datetime
import sarxarray
import depsi
import socket

from dask.distributed import Client
from dask_jobqueue import SLURMCluster

print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Python importing done in main process!")

slc_path = '/project/caroline/Share/stacks_zarr/nl_groningen_s1_dsc_t037/nl_groningen_s1_dsc_t037.zarr'

# metadata
metadata_path = '/project/caroline/Share/stacks/nl_groningen_s1_dsc_t037/stack/20200328/slave.res'

def get_free_port():
    """Get a non-occupied port number."""
    sock = socket.socket()
    sock.bind(("", 0))  # Bind a port, it will be busy now
    freesock = sock.getsockname()[1]  # get the port number
    sock.close()  # Free the port, so it can be used later
    return freesock

# Force all workers to use the same python env
venv = "/project/caroline/Share/users/caroline-oku/squashfs_debug/DePSI/venv_depsi/"
py_venv = f"{venv}/bin/python"

N_WORKERS = 2 # Manual input: number of workers to spin-up
EXPECTED_WORKERS = 2
TIMEOUT_MIN = 5  # Manual input: timeout in minutes for waiting for workers
FREE_SOCKET = get_free_port() # Get a free port
cluster = SLURMCluster(
    name="dask-worker",  # Name of the Slurm job
    queue="normal", # Name of the node partition on your SLURM system
    cores=4, # Number of cores per worker
    processes=1,  # Number of Python processes per worker
    walltime="0:10:00",  # Reserve each worker for X hour
    scheduler_options={"dashboard_address": f":{FREE_SOCKET}"},  # Host Dashboard in a free socket
    python=py_venv,  # force worker interpreter
)

# Processing part
if __name__ == "__main__":
    # Scale and connect to the cluster
    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Initiating dask cluster...")
    cluster.scale(jobs=N_WORKERS)
    client = Client(cluster)
    client.wait_for_workers(n_workers=EXPECTED_WORKERS, timeout=f"{TIMEOUT_MIN}m")

    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Reading Metadata...")
    metadata = sarxarray.read_metadata(metadata_path, driver="doris5")
    
    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Reading SLC stack...")
    slcs = depsi.read_slc_stack(slc_path)

    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Persisting 3 imgs...")
    slcs = slcs.isel(time=range(3)).persist()
    
    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Closing dask client and cluster...")
    cluster.close()
    client.close()
    
    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Done!")
    
    
