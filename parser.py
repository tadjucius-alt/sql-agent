#!/usr/bin/env python3
import re, json, time, os, requests

# --- CONFIG ---
STATE_FILE = "/home/tadas/Dev/SQL-parser/parser_state.json"
ES_URL = "http://localhost:9200/sql-slowlog/_doc"

# --- TOOLS ---
# PreciseEncoder: Forces Python to write small decimals fully (0.000031) instead of scientific notation (3.1e-05)
class PreciseEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float):
            return format(obj, '.12f').rstrip('0').rstrip('.')
        return super().default(obj)

def parse_query_block(block):
    """Parses a single block of log text into a clean JSON dictionary"""
    if not block.strip(): return None
    
    # Initialize the template
    entry = {'SQL': "", 'Time': None, 'User@Host': None}
    
    # Pattern Map for automated extraction of fields
    patterns = {
        'Id': r'Id:\s*(\d+)', 
        'Schema': r'Schema:\s*(\S+)', 
        'Last_errno': r'Last_errno:\s*(\d+)', 
        'Killed': r'Killed:\s*(\d+)',
        'Query_time': r'Query_time:\s*([\d.]+)', 
        'Lock_time': r'Lock_time:\s*([\d.]+)',
        'Rows_sent': r'Rows_sent:\s*(\d+)', 
        'Rows_examined': r'Rows_examined:\s*(\d+)',
        'Rows_affected': r'Rows_affected:\s*(\d+)', 
        'Bytes_sent': r'Bytes_sent:\s*(\d+)',
        'Tmp_tables': r'Tmp_tables:\s*(\d+)', 
        'Tmp_disk_tables': r'Tmp_disk_tables:\s*(\d+)',
        'QC_Hit': r'QC_Hit:\s*(\S+)', 
        'Full_scan': r'Full_scan:\s*(\S+)',
        'Log_slow_rate_limit': r'Log_slow_rate_limit:\s*(\d+)'
    }

    sql_lines = []
    for line in block.split('\n'):
        line = line.strip()
        if not line: continue
        
        # 1. Handle the start of the block
        if line.startswith('# Time:'): 
            entry['Time'] = line.replace('# Time:', '').strip()
        
        # 2. FIXED: Handle User@Host and Id correctly on the same line
        elif line.startswith('# User@Host:'):
            # Extract User info but STOP before 'Id:' (using lookahead)
            u_match = re.search(r'User@Host:\s*(.*?)(?=\s+Id:|$)', line)
            if u_match:
                entry['User@Host'] = u_match.group(1).strip()
            
            # Extract Id from this same line
            id_match = re.search(r'Id:\s*(\d+)', line)
            if id_match:
                entry['Id'] = int(id_match.group(1))
        
        # 3. Handle all other metrics in comment lines
        elif line.startswith('#'):
            for key, pat in patterns.items():
                if key in entry and entry[key] is not None: continue # Skip if already found (like Id)
                match = re.search(pat, line)
                if match:
                    val = match.group(1)
                    entry[key] = float(val) if '.' in val else (int(val) if val.isdigit() else val)
            
            # Special check for InnoDB fields (flattened to top level)
            if 'InnoDB_' in line:
                for k in ['IO_r_ops', 'IO_r_bytes', 'IO_r_wait', 'rec_lock_wait', 'queue_wait', 'pages_distinct']:
                    m = re.search(f'InnoDB_{k}:\\s*([\\d.]+)', line)
                    if m: entry[f'InnoDB_{k}'] = float(m.group(1)) if '.' in m.group(1) else int(m.group(1))
        
        # 4. Handle SQL lines (exclude metadata)
        else:
            if not any(line.startswith(x) for x in ['use ', 'SET timestamp=']):
                sql_lines.append(line)

    entry['SQL'] = " ".join(sql_lines).strip()
    return entry

def output_data(entry, output_json):
    """Sends the result to the local JSON file and Elasticsearch"""
    if not entry: return
    data = json.dumps(entry, cls=PreciseEncoder)
    
    # Save to file (Append mode)
    with open(output_json, 'a') as f: 
        f.write(data + "\n")
    
    # Send to Elasticsearch (Ignore errors to keep the parser running)
    try: 
        requests.post(ES_URL, data=data, headers={'Content-Type':'application/json'}, timeout=2)
    except: 
        pass 

# --- THE TAIL LOGIC ---
def tail_log(log_path, out_json):
    """Main loop: resumes from bookmark, handles rotations, and tails new lines"""
    # Load last bookmark (Inode and Byte Position)
    st = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {"inode": 0, "pos": 0}
    curr_inode = os.stat(log_path).st_ino
    pos = st['pos'] if st['inode'] == curr_inode else 0
    
    with open(log_path, 'r') as f:
        f.seek(pos)
        block = []
        while True:
            # Check if file changed on disk (Logrotate support)
            if os.stat(log_path).st_ino != curr_inode: 
                break 
            
            line = f.readline()
            
            # If end of file reached
            if not line:
                # EOF Flush: process any query sitting in the buffer
                if block: 
                    output_data(parse_query_block("\n".join(block)), out_json)
                    block = []
                # Save position state
                with open(STATE_FILE, 'w') as sf: 
                    json.dump({"inode": curr_inode, "pos": f.tell()}, sf)
                time.sleep(0.1)
                continue
            
            # If a new query begins, process the old one
            if line.startswith('# Time:') and block:
                output_data(parse_query_block("\n".join(block)), out_json)
                block = []
            
            block.append(line)

if __name__ == "__main__":
    L, O = "/home/tadas/Dev/SQL-parser/log.txt", "/home/tadas/Dev/SQL-parser/output.json"
    print(f"Agent started. Monitoring {L}...")
    while True:
        try: 
            tail_log(L, O)
        except KeyboardInterrupt: 
            print("\nShutting down...")
            break
        except Exception as e: 
            # Re-try in case of file locks during rotation
            time.sleep(1)
