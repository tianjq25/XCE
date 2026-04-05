from tqdm import tqdm
import names
import random
from table_meta import TPCH_SCHEMA, employees, ENC_COLS
import struct
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

def encrypt_int64(value, row_id, key):
    iv = struct.pack("<Q", row_id) + b'\x00\x00\x00\x00'
    value_bytes = struct.pack("<Q", value)
    aesgcm = AESGCM(key)
    encrypted = aesgcm.encrypt(iv, value_bytes, None)
    return encrypted

def decrypt_int64(encrypted, row_id, key):
    iv = struct.pack("<Q", row_id) + b'\x00\x00\x00\x00'
    
    aesgcm = AESGCM(key)
    
    decrypted = aesgcm.decrypt(iv, encrypted, None)
    
    value = struct.unpack("<Q", decrypted)[0]
    
    return value



def generate_table(lines=10000, key=None, table_meta=None):
    file_name = f"tables/{table_meta['table_name']}.table"
    f = open(file_name, "wb")

    # generate unique names
    unique_names = set()

    with tqdm(total=lines, desc="Generating unique names") as pbar:
        while len(unique_names) < lines:
            full_name = names.get_full_name()
            while (len(full_name) > 50):
                full_name = names.get_full_name()
            unique_names.add(full_name)
            if len(unique_names) > pbar.n:
                pbar.update(len(unique_names) - pbar.n)

    manager_num = min(0.1 * lines + 1, 10)
    random_managers = random.sample(unique_names, int(manager_num))
    # convert unique_names to map, value is id
    unique_names = list(unique_names)
    employees = {}

    # generate len(unique_names) random ids
    random_ids = set()
    with tqdm(total=len(unique_names), desc="Generating random IDs") as pbar:
        while len(random_ids) < len(unique_names):
            random_ids.add(random.randint(0, 500000000))
            if len(random_ids) > pbar.n:
                pbar.update(len(random_ids) - pbar.n)
    random_ids = list(random_ids)

    for i in range(len(unique_names)):
        employees[unique_names[i]] = random_ids[i]

    # write to a csv
    csv_file = open(f"{table_meta['table_name']}.csv", "w")
    csv_file.write("name,id,manager_id,department_id,role\n")

    # write to table file
    # write total number of rows
    f.write(len(unique_names).to_bytes(8, byteorder='little'))
    for i in tqdm(range(len(unique_names)), desc="Writing data"):
        # write name=unique_names[i], padding to 50 char
        f.write(unique_names[i].encode())
        f.write(b'\0' * (50 - len(unique_names[i])))

        # write id=random_ids[i], id type is bigint
        enc_id = encrypt_int64(random_ids[i], i, key)
        f.write(enc_id)
        # f.write(random_ids[i].to_bytes(8, byteorder='little'))

        # write manager_id, bigint
        mng_id = employees[random.choice(random_managers)]
        enc_mng_id = encrypt_int64(mng_id, i, key)
        f.write(enc_mng_id)
        # f.write(mng_id.to_bytes(8, byteorder='little'))

        # write department_id, bigint
        department_id = random.randint(0, 10)
        f.write(department_id.to_bytes(8, byteorder='little'))
        # write role, padding to 50 char
        f.write("staff".encode())
        f.write(b'\0' * (50 - len("staff")))
        csv_file.write(f"{unique_names[i]},{random_ids[i]},{mng_id},{department_id},staff\n")
    
    # write to index load data file
    index_file = open(f"tables/{table_meta['table_name']}_{table_meta['index_cols'][0]}.col", "wb")
    index_file.write(len(unique_names).to_bytes(8, byteorder='little'))
    for i in range(len(unique_names)):
        index_file.write(random_ids[i].to_bytes(8, byteorder='little'))
        index_file.write(i.to_bytes(8, byteorder='little'))

    csv_file.close()
    f.close()
    index_file.close()

def read_from_employees_csv(file_name, key):
    lines = 150000000
    count = 0
    with open(file_name, "r") as f:
        with open("tables/employees.table", "wb") as f_table:
            f_table.write(lines.to_bytes(8, byteorder='little'))
            for _ in tqdm(range(lines)):
            # read one line per time because the file is too large
                line = f.readline()
                count += 1
                # parse into employee cols
                name, id, manager_id, department_id, role = line.strip().split(",")
                f_table.write(name.encode())
                f_table.write(b'\0' * (50 - len(name)))
                f_table.write(int(id).to_bytes(8, byteorder='little'))
                enc_mng_id = encrypt_int64(int(manager_id), 0, key)
                f_table.write(enc_mng_id)
                f_table.write(int(department_id).to_bytes(8, byteorder='little'))
                f_table.write(role.encode())
                f_table.write(b'\0' * (50 - len(role)))
    print(f"Total lines: {count}")
            

def convert_tpch(schema, index_col, tbl_path, table_path, key):
    os.makedirs(os.path.dirname(table_path), exist_ok=True)

    with open(tbl_path + schema + ".tbl", "r") as tbl_file, \
         open(table_path + schema + ".table", "wb") as table_file, \
         open(f"{table_path}{schema}_{index_col}.col", "wb") as index_file:


        index_file.write(b"\x00" * 8)
        table_file.write(b"\x00" * 8)
        index_data = bytearray()

        line_count = 0
        for line in tqdm(tbl_file, desc="Converting lineitem"):
            fields = line.strip().split("|")[:-1]
            if len(fields) != len(TPCH_SCHEMA[schema]):
                continue

            row_data = bytearray()
            index_value = None

            for i, field in enumerate(fields):
                col_def = TPCH_SCHEMA[schema][i]
                
                if col_def["name"] == index_col:
                    index_value = int(field)
                
                if col_def["name"] in ENC_COLS:
                    packed = encrypt_int64(int(field), line_count, key)
                    row_data.extend(packed)
                    continue

                if col_def["type"] == "int":
                    packed = struct.pack("<Q", int(field))
                elif col_def["type"] == "double":
                    packed = struct.pack("<d", float(field))
                elif col_def["type"] == "str":
                    max_len = col_def["bytes"]
                    encoded = field.encode("ascii")[:max_len]
                    packed = encoded.ljust(max_len, b"\x00")
                
                row_data.extend(packed)

            table_file.write(row_data)
            
            if index_value is not None:
                index_data.extend(struct.pack("<QQ", index_value, line_count))
            
            line_count += 1

        index_file.seek(0)
        index_file.write(struct.pack("<Q", line_count))
        index_file.write(index_data)
        table_file.seek(0)
        table_file.write(struct.pack("<Q", line_count))


if __name__ == "__main__":
    # read key from key file
    with open("key.bin", "rb") as key_file:
        enc_key = key_file.read()


    generate_table(lines=100000, key=enc_key, table_meta=employees)


    convert_tpch("nation", "N_NATIONKEY", "TPC-H/", "tables/", enc_key)
    convert_tpch("supplier", "S_SUPPKEY", "TPC-H/", "tables/", enc_key)
    convert_tpch("customer", "C_CUSTKEY", "TPC-H/", "tables/", enc_key)
    convert_tpch("orders", "O_ORDERKEY", "TPC-H/", "tables/", enc_key)
    convert_tpch("lineitem", "L_ORDERKEY", "TPC-H/", "tables/", enc_key)