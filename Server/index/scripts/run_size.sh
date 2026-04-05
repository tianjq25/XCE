workloads=(cc)
datasets=(books_200M_uint64 osm_cellids_200M_uint64)
epsilon=(16 64 128 256 512 1024)
# datasets=(fb_200M_uint64 books_200M_uint64 osm_cellids_200M_uint64 wiki_ts_200M_uint64 lognormal_200M_uint64 normal_200M_uint64 uniform_dense_200M_uint64 uniform_sparse_200M_uint64)

for workload in ${workloads[@]}; do
  for dataset in ${datasets[@]}; do
    for eps in ${epsilon[@]}; do
      echo "Running $workload on $dataset with epsilon $eps"
      gramine-sgx pgm $workload $dataset $eps >> results.txt
    done
  done
done