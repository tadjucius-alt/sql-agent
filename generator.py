#!/usr/bin/env python3
import time
import datetime
import random

# Path to your log file
LOG_FILE = "/home/tadas/Dev/SQL-parser/log.txt"

# Your three distinct log templates
SAMPLES = [
    # Sample 1: Standard InnoDB query
    """# Time: {iso_time}
# User@Host: app[app] @ localhost []  Id: 4251168
# Schema: vt_db  Last_errno: 0  Killed: 0
# Query_time: 0.000220  Lock_time: 0.000106  Rows_sent: 1  Rows_examined: 45  Rows_affected: 0
# Bytes_sent: 56  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# QC_Hit: No  Full_scan: Yes  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
#   InnoDB_IO_r_ops: 0  InnoDB_IO_r_bytes: 0  InnoDB_IO_r_wait: 0.000000
#   InnoDB_rec_lock_wait: 0.000000  InnoDB_queue_wait: 0.000000
#   InnoDB_pages_distinct: 1
# Log_slow_rate_type: query  Log_slow_rate_limit: 500
use vt_db;
SET timestamp={unix_time};
select /* replica_read */ COUNT(*) from alive where fingerprint = 'host' limit 10001;""",

    # Sample 2: Performance Schema (No InnoDB stats)
    """# Time: {iso_time}
# User@Host: [] @ localhost []  Id: 4643062
# Schema:   Last_errno: 0  Killed: 0
# Query_time: 0.007591  Lock_time: 0.000044  Rows_sent: 171  Rows_examined: 171  Rows_affected: 0
# Bytes_sent: 15315  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# QC_Hit: No  Full_scan: Yes  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
# No InnoDB statistics available for this query
# Log_slow_rate_type: query  Log_slow_rate_limit: 500
SET timestamp={unix_time};
SELECT OBJECT_SCHEMA, OBJECT_NAME, ifnull(INDEX_NAME, 'NONE') as INDEX_NAME,
            COUNT_FETCH, COUNT_INSERT, COUNT_UPDATE, COUNT_DELETE,
            SUM_TIMER_FETCH, SUM_TIMER_INSERT, SUM_TIMER_UPDATE, SUM_TIMER_DELETE
          FROM performance_schema.table_io_waits_summary_by_index_usage
          WHERE OBJECT_SCHEMA NOT IN ('mysql', 'performance_schema');""",

    # Sample 3: Long query with multi-line IN clause
    """# Time: {iso_time}
# User@Host: app[app] @ localhost []  Id: 4249484
# Schema: vt_db  Last_errno: 0  Killed: 0
# Query_time: 0.002977  Lock_time: 0.000199  Rows_sent: 1  Rows_examined: 24  Rows_affected: 0
# Bytes_sent: 1833  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
#   InnoDB_IO_r_ops: 0  InnoDB_IO_r_bytes: 0  InnoDB_IO_r_wait: 0.000000
#   InnoDB_rec_lock_wait: 0.000000  InnoDB_queue_wait: 0.000000
#   InnoDB_pages_distinct: 1046
# Log_slow_rate_type: query  Log_slow_rate_limit: 500
SET timestamp={unix_time};
select /*replica_read*/ tx.* from tx where tx.`status` = 230 and tx.id not in (296, 373, 949, 807, 252, 005, 278, 497, 857, 326, 600, 109, 981, 192, 150, 297, 195, 079, 165, 174, 151, 091, 582, 508, 868, 800, 065, 939, 054, 316,
008, 937, 408, 100, 093, 327, 357, 335, 815, 433, 759, 784, 793, 902, 215, 653, 213, 889, 410, 4462, 401, 133, 620, 645, 490, 483, 628, 506, 961, 151, 525, 271, 024, 083, 488, 944, 269, 386, 4468, 512, 4932758682, 431, 299, 376, 4936650143, 913, 037, 4931205796, 259, 187, 4928516691, 092, 307, 701, 287, 960, 609, 09
7, 084, 118, 523, 920, 181, 127, 199, 409, 964, 075, 4956345212, 146) and status_updated_at >= '2023-01-01 03:53:25' order by tx.status_updated_
at desc limit 10001  /*revision:0000000000000000,request_id:000000000,transaction_tag:tag1*/;"""
]

def generate_logs():
    print(f"Starting log generation in {LOG_FILE}...")
    try:
        while True:
            # 1. Get current time in required formats
            now = datetime.datetime.utcnow()
            iso_time = now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            unix_time = int(time.time())

            # 2. Pick a random sample and format it
            sample = random.choice(SAMPLES)
            log_entry = sample.format(iso_time=iso_time, unix_time=unix_time)

            # 3. Append to file
            with open(LOG_FILE, "a") as f:
                f.write(log_entry + "\n")
            
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Wrote 1 entry to {LOG_FILE}")
            
            # 4. Wait 1 second
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping log generator...")

if __name__ == "__main__":
    generate_logs()
