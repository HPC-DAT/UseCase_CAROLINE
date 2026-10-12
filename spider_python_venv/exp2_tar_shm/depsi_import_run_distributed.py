"""
Simple script to profile the Python dependency importing
"""
from datetime import datetime
import sarxarray
import depsi
import socket

import os
import shutil
import subprocess
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

# Force all workers to use the same python env by extract the tared venv to shared memory space
PATH_VENV_TAR = "/project/caroline/Share/users/caroline-oku/squashfs_debug/DePSI/env.tar"
SHM_BASE = "/dev/shm/caroline-oku"
RUN_ID = os.environ.get("SLURM_JOB_ID") # Use the SLURM-ID of the main job. Each worker will have a different ID, but no need for a new venv 
PATH_VENV_SHM = f"{SHM_BASE}/venv-{RUN_ID}"
LOCK_FILE = f"{SHM_BASE}/.venv_{RUN_ID}.lock"
PYTHON = f"{PATH_VENV_SHM}/bin/python"

# Extras in worker slurm jobs
worker_job_script = [
        f'PATH_VENV_TAR="{PATH_VENV_TAR}"',
        f'SHM_BASE="{SHM_BASE}"',
        f'RUN_ID="{RUN_ID}"',
        'PATH_VENV_SHM="${SHM_BASE}/venv-${RUN_ID}"',
        'PYTHON="${PATH_VENV_SHM}/bin/python"',
        'READY_FILE="${PATH_VENV_SHM}/.ready"',
        'LOCK_FILE="${SHM_BASE}/.venv_${RUN_ID}.lock"',
        'mkdir -p "${SHM_BASE}"',
        'exec 9>"${LOCK_FILE}"',
        'flock -w 300 9 || { echo "Failed to acquire lock"; exit 1; }',
        'if [ ! -f "${READY_FILE}" ]; then rm -rf "${PATH_VENV_SHM}"; mkdir -p "${PATH_VENV_SHM}"; tar -xf "${PATH_VENV_TAR}" -C "${PATH_VENV_SHM}"; touch "${READY_FILE}"; fi',
    ]

N_WORKERS = 2 # Manual input: number of workers to spin-up
EXPECTED_WORKERS = 2
TIMEOUT_MIN = 5  # Manual input: timeout in minutes for waiting for workers
FREE_SOCKET = get_free_port() # Get a free port
cluster = SLURMCluster(
    python=PYTHON,
    job_script_prologue=worker_job_script,
)

client = Client(cluster)

# Processing part
if __name__ == "__main__":
    # Scale and connect to the cluster
    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Initiating dask cluster...")
    cluster.scale(jobs=N_WORKERS)
    client = Client(cluster)
    client.wait_for_workers(n_workers=EXPECTED_WORKERS, timeout=f"{TIMEOUT_MIN}m")
    try:
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Reading Metadata...")
        metadata = sarxarray.read_metadata(metadata_path, driver="doris5")
        
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Reading SLC stack...")
        slcs = depsi.read_slc_stack(slc_path)
    
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Persisting 3 imgs...")
        slcs = slcs.isel(time=range(3)).persist()
    finally:
        # stop workers/scheduler first
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Closing dask client and cluster")
        client.close()
        cluster.close()
    
        # then purge venv
        print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Purging venv")
        os.makedirs(SHM_BASE, exist_ok=True) # if venv not created, purge an empty folder; prevent error for purging
        subprocess.run(
            ["bash", "-lc", f'exec 8>"{LOCK_FILE}"; flock -w 60 8; rm -rf "{PATH_VENV_SHM}"; rm -f "{LOCK_FILE}"'],
            check=False,
        )
    
    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Done!")
    
    
