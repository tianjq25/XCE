"""
Plot a comparative bar chart of Evaluation Time for different methods.
Read four log files and obtain the runtime of each query using the same processing logic as handle_send_log.
"""

import re
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

# Reuse the processing logic of handle_send_log
def process_multi_run_log(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    segments = re.split(r"Total Time for executing \d+ queries: [\d.]+ sec", content)
    segments = [s for s in segments if "-th query finished in" in s]

    query_times = defaultdict(list)
    pattern = re.compile(r"(\w+)-th query finished in ([\d.]+) sec")

    for segment in segments:
        matches = pattern.findall(segment)
        for q_id, duration in matches:
            query_times[q_id].append(float(duration))

    averages = {}
    for q_id, times in query_times.items():
        if len(times) >= 3:
            trimmed = sorted(times)[1:-1]
            averages[q_id] = sum(trimmed) / len(trimmed)
        else:
            averages[q_id] = sum(times) / len(times)

    return averages


def main():
    log_dir = "imdb_log"
    # File paths and corresponding method names
    # Note: the user mentioned send_query_ASM4.log, while the actual file name is send_query_ASM_4.log
    log_files = {
        "ASM": f"{log_dir}/send_query_ASM.log",
        "ASM+XCE": f"{log_dir}/send_query_XCE.log",
        "TrueCard": f"{log_dir}/send_query_true_card.log",
        "Postgres": f"{log_dir}/send_original_query.log",
    }

    # Read the runtime of each method
    method_times = {}
    for method_name, file_path in log_files.items():
        method_times[method_name] = process_multi_run_log(file_path)

    # Use the query set of true_card as the reference, excluding 33c
    true_card_queries = set(method_times["TrueCard"].keys())
    if "33c" in true_card_queries:
        true_card_queries.remove("33c")
    # Ensure that all methods retain only the queries existing in true_card (excluding 33c)
    common_queries = true_card_queries

    # Filter the data of each method, retaining only common_queries
    filtered_times = {}
    for method_name, times in method_times.items():
        filtered_times[method_name] = {q: times.get(q, 0) for q in common_queries}

    # Compute the maximum runtime of each query across methods for sorting
    methods = ["ASM", "ASM+XCE", "Postgres", "TrueCard"]
    query_max_time = {}
    for q_id in common_queries:
        max_t = max(filtered_times[m][q_id] for m in methods)
        # query_max_time[q_id] = max_t
        query_max_time[q_id] = filtered_times["ASM"][q_id]

    # Sort in descending order by maximum runtime and take the top 20
    sorted_queries = sorted(
        common_queries,
        key=lambda q: query_max_time[q],
        reverse=True
    )[:10]

    # Prepare plotting data
    n_queries = len(sorted_queries)
    n_methods = len(methods)

    x = np.arange(n_queries)
    width = 0.2  # Width of each group of bars

    # fig, ax = plt.subplots(figsize=(14, 3))
    fig, ax = plt.subplots(figsize=(14, 6))

    # Colors and hatch patterns; each bar has a hatch;
    # the second item, TrueCard, uses a lighter blue-gray
    colors = ["#E07A5F", "#81B29A", "#6B7B8C", "#F2CC8F"]  # coral red, medium gray-blue, beige yellow, sage green
    hatches = ["//", "..", "xx", "\\\\"]  # four different hatch patterns

    # Upper limit of the y-axis; values beyond this limit are truncated
    Y_MAX = 50  # can be adjusted as needed

    for i, method in enumerate(methods):
        times = [filtered_times[method].get(q, 0) for q in sorted_queries]
        offset = (i - n_methods / 2 + 0.5) * width
        
        # Keep the original bar chart drawing logic
        bars = ax.bar(
            x + offset,
            [min(t, Y_MAX) for t in times],
            width,
            label=method,
            color=colors[i],
            hatch=hatches[i],
            edgecolor="black",
            linewidth=0.5,
        )

        # Beautify only the part exceeding Y_MAX
        for j, t in enumerate(times):
            if t > Y_MAX:
                curr_x = x[j] + offset
                
                # 1. Draw truncation marks:
                # draw two diagonal slashes (//) with white border and black center on top of the bar
                # Use transform=ax.transData to ensure the lines scale with the axes
                slash_h = Y_MAX * 0.02  # relative height of the slash
                slash_w = width * 0.4   # relative width of the slash
                
                for dy in [-slash_h, slash_h]:  # draw two parallel diagonal lines
                    ax.plot([curr_x - slash_w, curr_x + slash_w], 
                            [Y_MAX + dy - 0.5, Y_MAX + dy + 0.5], 
                            color='white', linewidth=2.5, zorder=4)  # white background
                    ax.plot([curr_x - slash_w, curr_x + slash_w], 
                            [Y_MAX + dy - 0.5, Y_MAX + dy + 0.5], 
                            color='black', linewidth=0.8, zorder=5)  # black line

                # 2. Annotate the actual runtime value above the bar
                ax.text(
                    curr_x, 
                    Y_MAX + 1, 
                    f"{t:.1f}", 
                    ha='center', 
                    va='bottom', 
                    fontsize=11, 
                    fontweight='bold',
                    color=colors[i]  # keep the annotation color consistent with the bar
                )

    # Slightly increase the axis upper limit to avoid clipping the numbers on top
    ax.set_ylim(0, Y_MAX + 7)

    ax.set_ylabel("End-to-End Time (s)", fontsize=14)
    ax.set_xlabel("Query ID", fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(sorted_queries, rotation=45, ha="right", fontsize=12)
    # ax.legend(loc="upper right", fontsize=12)
    ax.legend(loc="best", fontsize=12)
    ax.tick_params(axis="y", labelsize=12)
    # ax.set_ylim(bottom=0, top=Y_MAX)
    ax.grid(axis="y", linestyle="--", color="gray", alpha=0.7)
    ax.set_axisbelow(True)

    plt.tight_layout()
    plt.savefig("imdb_log/eval_time_comparison.png", dpi=150, bbox_inches="tight")
    plt.savefig("imdb_log/eval_time_comparison.pdf", bbox_inches="tight")
    print("Figures have been saved to imdb_log/eval_time_comparison.png and .pdf")
    # plt.show()


if __name__ == "__main__":
    main()