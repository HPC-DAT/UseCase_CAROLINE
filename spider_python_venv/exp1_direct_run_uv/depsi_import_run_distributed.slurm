#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=0:20:00
#SBATCH --cpus-per-task=4
#SBATCH --partition=normal

source /project/caroline/Share/users/caroline-oku/squashfs_debug/DePSI/venv_depsi/bin/activate

start_time=$(date +%s)
start_label=$(date)
echo "Start: ${start_label}"

python /home/caroline-oku/share-oku/squashfs_debug/exp1_direct_run_uv/depsi_import_run_distributed.py

end_time=$(date +%s)
end_label=$(date)
echo "End:   ${end_label}"

duration=$((end_time - start_time))
echo "Duration: ${duration}s"

