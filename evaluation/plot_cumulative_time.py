"""
Plot a cumulative runtime line chart.
X-axis: number of queries; Y-axis: total runtime (cumulative).
Read two log files, and process the points in the same way as handle_send_log
(multi-run averaging with the maximum and minimum values removed).
"""

import re
import numpy as np
import matplotlib.pyplot as plt

def process_multi_run_log_cumulative(file_path):
    """
    Use the same segmentation and parsing logic as handle_send_log,
    and return (x, y), where x is the number of queries [1,2,...,n],
    and y is the corresponding average cumulative total runtime.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    segments = re.split(r"Total Time for executing \d+ queries: [\d.]+ sec", content)
    segments = [s for s in segments if "-th query finished in" in s]

    pattern = re.compile(r"(\w+)-th query finished in ([\d.]+) sec")

    # Each run produces one cumulative time series
    cumulative_per_run = []
    for segment in segments:
        matches = pattern.findall(segment)
        durations = [float(d) for _, d in matches]
        cumsum = np.cumsum(durations)  # [t1, t1+t2, t1+t2+t3, ...]
        cumulative_per_run.append(cumsum)

    if not cumulative_per_run:
        return np.array([]), np.array([])

    n_queries = len(cumulative_per_run[0])
    # Align lengths
    # (if one segment is shorter, truncate all runs to the minimum length)
    min_len = min(len(r) for r in cumulative_per_run)
    cumulative_per_run = [r[:min_len] for r in cumulative_per_run]
    n_queries = min_len

    # For each query count n, collect the cumulative times of all runs
    # and compute the average according to the handle_send_log rule
    avg_cumulative = []
    for i in range(n_queries):
        times = [run[i] for run in cumulative_per_run]
        if len(times) >= 3:
            trimmed = sorted(times)[1:-1]
            avg_cumulative.append(sum(trimmed) / len(trimmed))
        else:
            avg_cumulative.append(sum(times) / len(times))

    x = np.arange(1, n_queries + 1)
    y = np.array(avg_cumulative)
    return x, y


def main():
    # Two log files and their corresponding legend labels
    # (can be modified here)
    log_files = [
        ("send_online_eval_asm_estimates_test.log", "ASM"),
        ("send_online_eval_XCE_estimates.log", "ASM+XCE"),
    ]

    fig, ax = plt.subplots(figsize=(6, 4))

    # Use the same color scheme as plot_eval_time
    colors = ["#E07A5F", "#81B29A"]  # coral red, sage green
    linestyles = ["-", "--"]
    # markers = ["o", "s"]

    for i, (file_path, label) in enumerate(log_files):
        x, y = process_multi_run_log_cumulative(file_path)
        if len(x) == 0:
            print(f"Warning: {file_path} contains no valid data")
            continue
        ax.plot(
            x,
            y,
            label=label,
            color=colors[i],
            linestyle=linestyles[i],
            # marker=markers[i],
            markersize=4,
            markevery=max(1, len(x) // 30),  # Draw one marker every certain number of points to avoid clutter
            linewidth=2,
        )

    ax.set_xlabel("Number of Queries", fontsize=20)
    ax.set_ylabel("Total Runtime (s)", fontsize=20)
    ax.legend(loc="upper left", fontsize=16)
    ax.tick_params(axis="both", labelsize=16)
    ax.grid(axis="y", linestyle="--", color="gray", alpha=0.7)
    ax.set_axisbelow(True)
    ax.set_xlim(left=0)

    plt.tight_layout()
    plt.savefig("cumulative_time_comparison.png", dpi=150, bbox_inches="tight")
    plt.savefig("cumulative_time_comparison.pdf", bbox_inches="tight")
    print("Figures have been saved to cumulative_time_comparison.png and .pdf")
    # plt.show()


if __name__ == "__main__":
    main()