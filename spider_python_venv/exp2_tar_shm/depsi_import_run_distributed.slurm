#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0:20:00
#SBATCH --cpus-per-task=4
#SBATCH --partition=normal

# Mannual input for tarred venv path
PATH_VENV_TAR="/project/caroline/Share/users/caroline-oku/squashfs_debug/DePSI/env.tar"

# Extract the tarred venv
# One extract per slurm-job per location
SHM_BASE="/dev/shm/caroline-oku"
RUN_ID="${SLURM_JOB_ID}"
PATH_VENV_SHM="${SHM_BASE}/venv-${RUN_ID}" # Per venv extract per SLURM job
PYTHON="${PATH_VENV_SHM}/bin/python"
READY_FILE="${PATH_VENV_SHM}/.ready"
LOCK_FILE="${SHM_BASE}/.venv_${RUN_ID}.lock"

# Make a folder for the user in the shm dir
mkdir -p "${SHM_BASE}"

# Purge the extracted venv upon finish
cleanup() {
  # lock again during cleanup to avoid race with late starters
  exec 8>"${LOCK_FILE}" || true
  flock -w 60 8 || true
  rm -rf "${PATH_VENV_SHM}"
  rm -f "${LOCK_FILE}"
}
trap cleanup EXIT

# setup section (locked)
exec 9>"${LOCK_FILE}"
flock -w 300 9 || { echo "Failed to acquire lock"; exit 1; }

start_time=$(date +%s)
start_label=$(date)
echo "Start: ${start_label}"

# Check the ready file, if exists, no need to extract
if [ ! -f "${READY_FILE}" ]; then
  rm -rf "${PATH_VENV_SHM}"
  mkdir -p "${PATH_VENV_SHM}"
  tar -xf "${PATH_VENV_TAR}" -C "${PATH_VENV_SHM}"
  touch "${READY_FILE}"
fi

"${PYTHON}" /home/caroline-oku/share-oku/squashfs_debug/exp2_tar_shm/depsi_import_run_distributed.py

end_time=$(date +%s)
end_label=$(date)
echo "End:   ${end_label}"

duration=$((end_time - start_time))
echo "Duration: ${duration}s"

