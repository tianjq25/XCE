import re
from collections import defaultdict

def process_multi_run_log(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Split the file into five execution log segments
    # Assume that each segment ends with "Total Time for executing"
    segments = re.split(r"Total Time for executing \d+ queries: [\d.]+ sec", content)
    # Remove empty segments
    segments = [s for s in segments if "-th query finished in" in s]

    query_times = defaultdict(list)
    pattern = re.compile(r"(\w+)-th query finished in ([\d.]+) sec")

    for segment in segments:
        matches = pattern.findall(segment)
        for q_id, duration in matches:
            query_times[q_id].append(float(duration))

    # 2. Compute the average value
    # (remove the maximum and minimum values)
    averages = {}
    for q_id, times in query_times.items():
        if len(times) >= 3:
            # Formula: avg = (sum - max - min) / (n - 2)
            trimmed = sorted(times)[1:-1]
            averages[q_id] = sum(trimmed) / len(trimmed)
        else:
            averages[q_id] = sum(times) / len(times)
            
    return averages

def compare_logs(file_path_old, file_path_new):
    imdb_old = process_multi_run_log(file_path_old)
    imdb_new = process_multi_run_log(file_path_new)
    
    all_queries = sorted(set(imdb_old.keys()) | set(imdb_new.keys()))
    
    print(f"{'Query ID':<10} | {'Old Version (s)':<12} | {'New Version (s)':<12} | {'Improvement (%)':<12}")
    print("-" * 55)
    
    for q_id in all_queries:
        t_old = imdb_old.get(q_id, 0)
        t_new = imdb_new.get(q_id, 0)
        
        if t_old > 0 and t_new > 0:
            # Compute the speed improvement: (old - new) / old
            improvement = (t_old - t_new) / t_old * 100
            print(f"{q_id:<10} | {t_old:<12.4f} | {t_new:<12.4f} | {improvement:>+10.2f}%")
        else:
            print(f"{q_id:<10} | {'Missing Data':<12} | {'Missing Data':<12} | -")

if __name__ == "__main__":
    pass