# Spider Python Virtual Environment experiments

For all three experiments, we activate the same Python virtual environment using different methods. Then perform the same set of tasks:
1. Spin up a Dask Slurm scheduler and workers.
2. Persist an Zarr stack to memory.

## Experiments

1. `exp1_direct_run_uv`: directly load the Python virtual environment from the specified path. (~3 mins to import the Python dependencies)
2. `exp2_tar_shm`: Compress the Python virtual environment into a tarball and extract it to shared memory for both main Python process and workers. (~9 secs to import the Python dependencies)
3. `exp3_squash_fs`: Create a SquashFS image from the Python virtual environment and mount it for both main Python process and workers. (~4 secs to import the Python dependencies). This method needs Pixi to run the `squashfuse` command to mount the SquashFS image.

