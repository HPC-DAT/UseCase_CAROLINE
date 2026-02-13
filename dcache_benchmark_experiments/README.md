# Benchmark caroline processing on Project Space and dCache

This repository sets up experiments to benchmark the performance of part the CAROLINE processing operating on the Project Space and dCache storage systems. The idea is to compare identical processing on the two storage systems, and to identify potential bottlenecks caused by different FS.

## Testing data

Two Zarr stacks on project space have been copied to dCache for testing:

| Stack name | Project Space path | dCache path | Size |
| --- | --- | --- | --- |
| nl_amsterdam_s1_asc_t161| `/project/caroline/Share/stacks_zarr/nl_amsterdam_s1_asc_t161/nl_amsterdam_s1_asc_t161.zarr` | `maca_caroline:/disk/Data/ou_dcache_benchmark/nl_amsterdam_s1_asc_t161.zarr` | 70MB |
| nl_assendelft_s1_dsc_t037 | `/project/caroline/Share/stacks_zarr/nl_assendelft_s1_dsc_t037/nl_assendelft_s1_dsc_t037.zarr/` | `maca_caroline:/disk/Data/ou_dcache_benchmark/nl_assendelft_s1_dsc_t037.zarr` | 4.4GB |

**For the large stack nl_assendelft_s1_dsc_t037, a Dask SLURMCluster with 6 workers = 24 cores + 192GB mem is used for processing.**

## Processing steps

The following processing steps are implemented in the Jupyter notebooks:

Step 1: Load the Zarr stack (Lazy or Eager loading depending on )

Step 2: two processing operations: operation 1) select pixels from the Zarr stack; (need to execute dask graph) operation 2) select network point (lazy)

Step 3: save the selected pixels in the previous step to a new Zarr store.

## Experiments:

8 experiments are set up with a permutation of three sets of settings:

1. The source of data: Project Space vs dCache
2. The loading method: Lazy vs Eager loading
3. The processing method: Either load the whole Zarr stack into memory in Step 1