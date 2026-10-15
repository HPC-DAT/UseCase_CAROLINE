#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=64
#SBATCH --partition=normal

source /home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/DePSI/.venv/bin/activate

# Use the Python from the env where depsi is installed
py-spy record --output /home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/profiling_results/profiling_$(date +%Y%m%d_%H%M%S).json --idle --rate 2 --subprocesses --format speedscope python /home/caroline-oku/caroline/Share/users/caroline-oku/groningen-run/depsi_groningen_oneyear_dcache_40k.py
