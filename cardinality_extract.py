import re, os
from collections import defaultdict

def parse_cardinality_log(log_content):
    """
    Parse the content of a cardinality estimation log file.
    """
    parsed_results = []
    
    # 1. Split by 'query:'
    # re.split preserves the content around the separator, so we filter out empty strings
    # Assume that query: appears at the beginning of a line
    raw_blocks = re.split(r'(?:^|\n)query:\s*', log_content.strip())
    
    for block in raw_blocks:
        if not block.strip():
            continue
            
        lines = block.strip().split('\n')
        query_id = lines[0].strip()
        
        query_data = {
            "query_id": query_id,
            "tables": []
        }
        
        # 2. Search for evaluate_one_tree blocks within each Query block
        # We use a simple state-machine-style logic to process the lines sequentially
        current_table = None
        
        for line in lines[1:]:
            line = line.strip()
            
            # Match the table declaration line
            # evaluate_one_tree: an, aka_name
            table_match = re.match(r'evaluate_one_tree:\s*(\w+),\s*(\w+)', line)
            if table_match:
                # If there is a table currently being processed, save it first
                # (although here it is appended by reference in real time,
                # logically this starts a new one)
                alias, table_name = table_match.groups()
                current_table = {
                    "alias": alias,
                    "table_name": table_name,
                    "raw_columns": {}, # Store the raw extracted column probabilities
                    "final_prob": None
                }
                query_data["tables"].append(current_table)
                continue
            
            # If we are not currently processing a table in evaluate_one_tree
            # (for example, if we have entered the infer_table or first get_P stage),
            # skip this line
            if current_table is None:
                continue
            
            # Stop condition:
            # encountering first get_P or infer_table usually means that
            # the current tree evaluation has ended
            if line.startswith("first get_P") or line.startswith("infer_table"):
                current_table = None
                continue

            # 3. Extract column probability - Pattern A (intermediate probability)
            # [Intermediate Probability] Column: name_fact_0, Average Probability P(Col|Context): 0.959984
            prob_match_a = re.match(
                r'\[Intermediate Probability\]\s*Column:\s*([\w_]+),\s*Average Probability P\(Col\|Context\):\s*([\d\.]+)',
                line
            )
            if prob_match_a:
                col_name, prob = prob_match_a.groups()
                current_table["raw_columns"][col_name] = float(prob)
                continue

            # 3. Extract column probability - Pattern B (column selectivity)
            # [Column Selectivity] Column: kind, Selectivity: 0.250000
            prob_match_b = re.match(
                r'\[Column Selectivity\]\s*Column:\s*([\w_]+),\s*Selectivity:\s*([\d\.]+)',
                line
            )
            if prob_match_b:
                col_name, prob = prob_match_b.groups()
                current_table["raw_columns"][col_name] = float(prob)
                continue

            # 4. Extract the final table probability - Pattern A
            # ========== [Final Result] Average Probability of P(Query): 0.0093928743 ==========
            final_match_a = re.search(
                r'\[Final Result\]\s*Average Probability of P\(Query\):\s*([\d\.]+)',
                line
            )
            if final_match_a:
                current_table["final_prob"] = float(final_match_a.group(1))
                continue

            # 4. Extract the final table probability - Pattern B
            # [Total Selectivity] Total Table Selectivity: 0.250000
            final_match_b = re.match(
                r'\[Total Selectivity\]\s*Total Table Selectivity:\s*([\d\.]+)',
                line
            )
            if final_match_b:
                current_table["final_prob"] = float(final_match_b.group(1))
                continue

        # 5. Merge factorized columns
        # (name_fact_0 * name_fact_1 -> name)
        for tbl in query_data["tables"]:
            merged_columns = {}
            temp_fact_probs = defaultdict(lambda: 1.0) 
            fact_cols_found = set()

            for col, prob in tbl["raw_columns"].items():
                # Check whether this is a factorized column
                # (for example, containing _fact_)
                if "_fact_" in col:
                    # Extract the base name, e.g., name_fact_0 -> name
                    base_name = col.split("_fact_")[0]
                    temp_fact_probs[base_name] *= prob
                    fact_cols_found.add(base_name)
                else:
                    merged_columns[col] = prob
            
            for base_name in fact_cols_found:
                merged_columns[base_name] = temp_fact_probs[base_name]
            
            tbl["merged_columns"] = merged_columns
            del tbl["raw_columns"]

        parsed_results.append(query_data)
        
    return parsed_results

def print_formatted_results(results):
    """
    Print the results in a formatted way.
    """
    for q in results:
        print(f"Query ID: {q['query_id']}")
        print("-" * 50)
        for tbl in q['tables']:
            print(f"Table: {tbl['table_name']} ({tbl['alias']})")
            
            if not tbl['merged_columns'] and tbl['final_prob'] is None:
                print("  (No probability data found)")
                continue

            # Print column probabilities
            for col, prob in tbl['merged_columns'].items():
                # Determine whether the column is merged
                # (i.e., the name does not exist in the raw data but exists here)
                # A simple print is sufficient here
                print(f"  Column: {col:<15} | Prob: {prob:.8f}")
            
            # Print the final probability
            final_p = tbl['final_prob'] if tbl['final_prob'] is not None else 0.0
            print(f"  >> Table Final Prob: {final_p:.10f}")
            print("")
        print("=" * 50 + "\n")


def process_log_file(file_path):
    """
    Read the content from a file and process it.
    """
    # print(f"Processing file: {file_path} ...\n")
    
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return

    content = ""
    try:
        # Try reading the file using utf-8
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        print("Warning: UTF-8 decoding failed. Trying to read with latin-1...")
        # If special characters are present, try latin-1
        with open(file_path, 'r', encoding='latin-1') as f:
            content = f.read()
    except Exception as e:
        print(f"An unknown error occurred while reading the file: {e}")
        return

    # Call the parsing function
    results = parse_cardinality_log(content)
    
    # Print the results
    # print_formatted_results(results)
    return results

if __name__ == "__main__":
    pass