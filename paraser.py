#!/usr/bin/env python3
import re
import json
import time
import os

# --- FLOAT PRECISION TOOLS ---
class PreciseJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float):
            return format(obj, '.12f').rstrip('0').rstrip('.')
        return super().default(obj)

# --- THE PARSER ---
def parse_query_block(block):
    if not block.strip(): return None
    
    entry = {
        'Time': None, 'User@Host': None, 'Id': None, 'Schema': None, 'Last_errno': 0, 'Killed': 0,
        'Query_time': 0.0, 'Lock_time': 0.0, 'Rows_sent': 0, 'Rows_examined': 0, 'Rows_affected': 0,
        'Bytes_sent': 0, 'Tmp_tables': 0, 'Tmp_disk_tables': 0, 'Tmp_table_sizes': 0,
        'QC_Hit': None, 'Full_scan': None, 'Full_join': None, 'Tmp_table': None, 'Tmp_table_on_disk': None,
        'Filesort': None, 'Filesort_on_disk': None, 'Merge_passes': 0,
        'InnoDB_IO_r_ops': 0, 'InnoDB_IO_r_bytes': 0, 'InnoDB_IO_r_wait': 0.0,
        'InnoDB_rec_lock_wait': 0.0, 'InnoDB_queue_wait': 0.0, 'InnoDB_pages_distinct': 0,
        'Log_slow_rate_type': None, 'Log_slow_rate_limit': 0,
        'SQL': ""
    }

    lines = block.split('\n')
    sql_lines = []
    
    for line in lines:
        line = line.strip()
        if not line: continue
        
        if line.startswith('# Time:'):
            entry['Time'] = line.replace('# Time:', '').strip()
            
        elif line.startswith('# User@Host:'):
            u_match = re.search(r'User@Host:\s*(.*?)\s+Id:', line)
            if u_match: 
                entry['User@Host'] = u_match.group(1).strip()
            id_m = re.search(r'Id:\s*(\d+)', line)
            if id_m: entry['Id'] = int(id_m.group(1))
            
        elif 'Schema:' in line:
            m = re.search(r'Schema:\s*(\S+)', line)
            if m: entry['Schema'] = m.group(1)
            e = re.search(r'Last_errno:\s*(\d+)', line)
            if e: entry['Last_errno'] = int(e.group(1))
            k = re.search(r'Killed:\s*(\d+)', line)
            if k: entry['Killed'] = int(k.group(1))
            
        elif 'Query_time:' in line:
            qt = re.search(r'Query_time:\s*([\d.]+)', line)
            lt = re.search(r'Lock_time:\s*([\d.]+)', line)
            rs = re.search(r'Rows_sent:\s*(\d+)', line)
            re_ = re.search(r'Rows_examined:\s*(\d+)', line)
            ra = re.search(r'Rows_affected:\s*(\d+)', line)
            if qt: entry['Query_time'] = float(qt.group(1))
            if lt: entry['Lock_time'] = float(lt.group(1))
            if rs: entry['Rows_sent'] = int(rs.group(1))
            if re_: entry['Rows_examined'] = int(re_.group(1))
            if ra: entry['Rows_affected'] = int(ra.group(1))
            
        elif 'Bytes_sent:' in line:
            bs = re.search(r'Bytes_sent:\s*(\d+)', line)
            if bs: entry['Bytes_sent'] = int(bs.group(1))
            tt = re.search(r'Tmp_tables:\s*(\d+)', line)
            if tt: entry['Tmp_tables'] = int(tt.group(1))
            td = re.search(r'Tmp_disk_tables:\s*(\d+)', line)
            if td: entry['Tmp_disk_tables'] = int(td.group(1))
            ts = re.search(r'Tmp_table_sizes:\s*(\d+)', line)
            if ts: entry['Tmp_table_sizes'] = int(ts.group(1))
            
        elif 'QC_Hit:' in line:
            for key in ['QC_Hit', 'Full_scan', 'Full_join', 'Tmp_table', 'Tmp_table_on_disk']:
                m = re.search(f'{key}:\\s*(\\S+)', line)
                if m: entry[key] = m.group(1)
                
        elif 'Filesort:' in line:
            for key in ['Filesort', 'Filesort_on_disk', 'Merge_passes']:
                m = re.search(f'{key}:\\s*(\\S+)', line)
                if m:
                    val = m.group(1)
                    entry[key] = int(val) if val.isdigit() else val
                    
        elif 'InnoDB_' in line:
            innodb_keys = ['InnoDB_IO_r_ops', 'InnoDB_IO_r_bytes', 'InnoDB_IO_r_wait', 'InnoDB_rec_lock_wait', 'InnoDB_queue_wait', 'InnoDB_pages_distinct']
            for key in innodb_keys:
                match = re.search(f'{key}:\\s*([\\d.]+)', line)
                if match:
                    val = match.group(1)
                    entry[key] = float(val) if '.' in val else int(val)
                    
        elif 'Log_slow_rate' in line:
            m = re.search(r'Log_slow_rate_type:\s*(\S+)', line)
            if m: entry['Log_slow_rate_type'] = m.group(1)
            m = re.search(r'Log_slow_rate_limit:\s*(\d+)', line)
            if m: entry['Log_slow_rate_limit'] = int(m.group(1))
            
        elif not line.startswith('#'):
            if not (line.startswith('use ') or line.startswith('SET timestamp=')):
                sql_lines.append(line)
                
    entry['SQL'] = " ".join(sql_lines).strip()
    return entry

# --- THE TAIL LOGIC ---
def tail_slow_log(filepath, output_json):
    print(f"Monitoring {filepath}... Outputting to {output_json}")
    with open(filepath, 'r') as f:
        f.seek(0)
        current_block = []
        while True:
            line = f.readline()
            if not line:
                if current_block:
                    entry = parse_query_block("\n".join(current_block))
                    if entry:
                        with open(output_json, 'a') as out:
                            out.write(json.dumps(entry, cls=PreciseJSONEncoder) + "\n")
                    current_block = []
                time.sleep(0.1)
                continue
            if line.startswith('# Time:') and current_block:
                entry = parse_query_block("\n".join(current_block))
                if entry:
                    with open(output_json, 'a') as out:
                        out.write(json.dumps(entry, cls=PreciseJSONEncoder) + "\n")
                current_block = []
            current_block.append(line)

if __name__ == "__main__":
    LOG_FILE = "/home/tadas/Dev/SQL-parser/log.txt"
    OUT_FILE = "/home/tadas/Dev/SQL-parser/output.json"
    try:
        tail_slow_log(LOG_FILE, OUT_FILE)
    except KeyboardInterrupt:
        print("\nStopping...")
