#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export PYTHONPATH="${SCRIPT_DIR}/..${PYTHONPATH:+:$PYTHONPATH}"

dir=AR_models
mkdir -p $dir

# Initialize the GPU ID
gpu=0
# Counter used to control the number of concurrent jobs
count=0
# Set the maximum number of concurrent jobs
# (If you have 4 GPUs, 4 is recommended; if memory is tight, set it to 2 or 1)
MAX_JOBS=4

echo "Starting training with $MAX_JOBS parallel jobs..."

for table in $(ls ./datasets/imdb/*.csv); do
    # Logic for extracting the filename
    # (slightly improved for better robustness)
    filename=$(basename -- "$table")
    z="${filename%.*}"
    
    echo "[Scheduling] Table: $z on GPU: $gpu"
    log=$dir/train_imdb_${z}.log
    
    # 1. Launch the job in the background (&)
    # Note: sleep 2 is added here to stagger Ray startup peaks and avoid port conflicts
    (sleep 2 && CUDA_VISIBLE_DEVICES=$gpu python -W ignore AR/run.py --run imdb-single-${z} > $log) &
    
    # 2. Update the GPU ID (0 -> 1 -> 2 -> 3 -> 0 ...)
    gpu=$(($gpu+1))
    gpu=$(($gpu%$MAX_JOBS))
    
    # 3. Update the counter
    count=$(($count+1))
    
    # 4. Core logic: if MAX_JOBS tasks have already been launched,
    # pause and wait until all of them finish
    if [ "$count" -ge "$MAX_JOBS" ]; then
        echo ">>> Batch full ($MAX_JOBS jobs running). Waiting for them to finish..."
        wait
        echo ">>> Batch finished. Cleaning up and cooling down..."
        
        # ⚠️ Key point: after each batch finishes, briefly clean up the environment
        # to avoid leftover Ray processes
        # If your script already includes ray.shutdown(), this may be skipped,
        # but it is kept here for safety
        sleep 5 
        
        # Reset the counter and start the next batch
        count=0
    fi
done

# Wait for the last batch of jobs to finish
# (i.e., the final batch with fewer than 4 jobs)
wait
echo "All tasks finished successfully."
