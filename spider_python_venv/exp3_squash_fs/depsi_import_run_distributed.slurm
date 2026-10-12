#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0:20:00
#SBATCH --cpus-per-task=4
#SBATCH --partition=normal

# Mannual input for tarred venv path
ENV_SQSH="/project/caroline/Share/users/caroline-oku/squashfs_debug/DePSI/env.sqsh"
PIXIDIR="/project/caroline/Share/users/caroline-oku/squashfs_debug" # We need pixi to run squash fuse

# Mount the image 
RUN_ID="${SLURM_JOB_ID}"
MOUNT_BASE="/tmp/$USER"
MOUNT_DIR="${MOUNT_BASE}/venv-${RUN_ID}"
LOCK_FILE="${MOUNT_BASE}/.venv_${RUN_ID}.lock"
PYTHON="${MOUNT_DIR}/bin/python"

# Make a folder for the user in the shm dir
mkdir -p "${MOUNT_BASE}" "${MOUNT_DIR}"

# Unmount and remove the lock file
cleanup() {
  # unmount if mounted
  if mountpoint -q "${MOUNT_DIR}"; then
    fusermount -u "${MOUNT_DIR}" || fusermount -uz "${MOUNT_DIR}" || true
  fi
  rmdir "${MOUNT_DIR}" 2>/dev/null || true
  rm -f "${LOCK_FILE}"
}
trap cleanup EXIT

# add a time out guard for mount
exec 9>"${LOCK_FILE}"
flock -w 300 9 || { echo "Could not acquire lock ${LOCK_FILE}"; exit 1; }

start_time=$(date +%s)
start_label=$(date)
echo "Start: ${start_label}"

if ! mountpoint -q "${MOUNT_DIR}"; then
  ( cd "${PIXIDIR}" && pixi run squashfuse "${ENV_SQSH}" "${MOUNT_DIR}" )
fi

"${PYTHON}" /home/caroline-oku/share-oku/squashfs_debug/exp3_squash_fs/depsi_import_run_distributed.py

end_time=$(date +%s)
end_label=$(date)
echo "End:   ${end_label}"

duration=$((end_time - start_time))
echo "Duration: ${duration}s"

