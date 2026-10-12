"""
Simple script to profile the Python dependency importing
"""
from datetime import datetime
import sarxarray
import depsi
import socket

import os
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

# Mounting cmd for all workers
ENV_SQSH = "/project/caroline/Share/users/caroline-oku/squashfs_debug/DePSI/env.sqsh"
PIXIDIR="/project/caroline/Share/users/caroline-oku/squashfs_debug"
TOP_RUN_ID = os.environ.get("SLURM_JOB_ID") # Get SLURM job ID for top level SLURM. To prevent multiple mounting

MOUNT_BASE = f"/tmp/{os.environ.get('USER', 'unknown')}" # By default mount to /tmp/$USER
MOUNT_DIR = f"{MOUNT_BASE}/venv-{TOP_RUN_ID}"
PYTHON = f"{MOUNT_DIR}/bin/python"

# Extras in worker slurm jobs
worker_job_script = [
        f'ENV_SQSH="{ENV_SQSH}"',
        f'TOP_RUN_ID="{TOP_RUN_ID}"',
        f'PIXIDIR="{PIXIDIR}"',
        f'MOUNT_BASE="{MOUNT_BASE}"',
        f'MOUNT_DIR="{MOUNT_DIR}"',
        'LOCK_FILE="${MOUNT_BASE}/.venv_${TOP_RUN_ID}.lock"',
        'mkdir -p "${MOUNT_BASE}" "${MOUNT_DIR}"',
        'exec 9>"${LOCK_FILE}"',
        'flock -w 300 9 || { echo "Could not acquire lock ${LOCK_FILE}"; exit 1; }',
        'if ! mountpoint -q "${MOUNT_DIR}"; then ( cd "${PIXIDIR}" && pixi run squashfuse "${ENV_SQSH}" "${MOUNT_DIR}" ); fi',
        'cleanup_mount(){ if mountpoint -q "${MOUNT_DIR}"; then fusermount -u "${MOUNT_DIR}" || fusermount -uz "${MOUNT_DIR}" || true; fi; rmdir "${MOUNT_DIR}" 2>/dev/null || true; }',
        'trap cleanup_mount EXIT',
        '"${MOUNT_DIR}/bin/python" -V',
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
    
    print(f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')} Done!")
    
    
