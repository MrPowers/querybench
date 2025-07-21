#!/bin/bash

# h20
## groupby
uv run -m querybench.run_groupby 1e7
uv run -m querybench.run_groupby 1e8

## join
uv run -m querybench.run_join 1e7
uv run -m querybench.run_join 1e8

# clickbench
uv run -m querybench.run_clickbench $clickbench

# querybench
uv run -m querybench.run_single_table 1e7
uv run -m querybench.run_single_table 1e8

