workloads=(c)
# workloads=(cc c aa)
# datasets=(books_200M_uint64)
datasets=(fb_200M_uint64 books_200M_uint64 osm_cellids_200M_uint64 wiki_ts_200M_uint64 lognormal_200M_uint64 normal_200M_uint64 uniform_dense_200M_uint64 uniform_sparse_200M_uint64)

for workload in ${workloads[@]}; do
  for dataset in ${datasets[@]}; do
    echo "Running $workload on $dataset"
    gramine-sgx pgm $workload $dataset >> results.txt
  done
done