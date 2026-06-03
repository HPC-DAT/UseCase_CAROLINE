# Data loading benchmark for dCache

In this experiment, we setup experiments to compare the data loading time between dCache and project space. Three zarr datasets with different sizes (70MB, 4.4GB and 220GB) are copied to both project space and dCache. We used `xr.open_zarr` to first lazily load the datasets, and then call `.compute()` to trigger the actual data loading. The time taken for these two steps by `%%timeit` are recorded and compared between the two storage systems.

Note that for the 220GB dataset, we only loaded one data variable of ~87GB. This should be sufficient to demonstrate the performance though.

## Results

|   | Small Data (70MB)| Large Data (4.4GB) | Huge Data (87GB) |
|---|---|---|---|
|Project Space | 5.04 s ± 186 ms per loop (mean ± std. dev. of 7 runs, 1 loop each) | 1min 9s ± 5.88 s per loop (mean ± std. dev. of 7 runs, 1 loop each) | 9min 52s ± 1min 56s per loop (mean ± std. dev. of 7 runs, 1 loop each) |
|dCache | 47.5 s ± 3.18 s per loop (mean ± std. dev. of 7 runs, 1 loop each) | 2min 20s ± 6.16 s per loop (mean ± std. dev. of 7 runs, 1 loop each)| 9min 48s ± 5.19 s per loop (mean ± std. dev. of 7 runs, 1 loop each) |
