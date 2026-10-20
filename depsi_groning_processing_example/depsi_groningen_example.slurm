#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=8:00:00
#SBATCH --cpus-per-task=8
#SBATCH --partition=interactive

# Setup Python environment

echo "Setting up Python environment..."

# Option 1: Use an existing Python virtual environment:
# PYTHON="/home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/DePSI/.venv/bin/python"

# Option 2: Tarball-based virtual environment setup
PATH_VENV_TAR="/home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/DePSI/env.tar"
SHM_BASE="/dev/shm/caroline-oku"
RUN_ID="${SLURM_JOB_ID}"
PATH_VENV_SHM="${SHM_BASE}/venv-${RUN_ID}"
PYTHON="${PATH_VENV_SHM}/bin/python"
READY_FILE="${PATH_VENV_SHM}/.ready"
LOCK_FILE="${SHM_BASE}/.venv_${RUN_ID}.lock"

mkdir -p "${PATH_VENV_SHM}"

cleanup() {
  exec 8>"${LOCK_FILE}" || true
  flock -w 60 8 || true
  rm -rf "${PATH_VENV_SHM}"
  rm -f "${LOCK_FILE}"
}
trap cleanup EXIT

exec 9>"${LOCK_FILE}"
flock -w 300 9 || { echo "Failed to acquire lock"; exit 1; }

if [ ! -f "${READY_FILE}" ]; then
  rm -rf "${PATH_VENV_SHM}"
  mkdir -p "${PATH_VENV_SHM}"
  tar -xf "${PATH_VENV_TAR}" -C "${PATH_VENV_SHM}"
  touch "${READY_FILE}"
fi

echo "Using Python environment at ${PYTHON}"
echo "Python environment setup complete."


echo "Running the Python script..."
${PYTHON} /home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/depsi_groningen_example.py
