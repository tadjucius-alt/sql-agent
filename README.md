# sql-agent

Tail the logs from a specific directory and can output them to file in JSON format.

1. Clone repo
2. In parser.py locate at the end input and ouptut files and change them.
        LOG_FILE = "/home/tadas/Dev/SQL-parser/log.txt"
        OUT_FILE = "/home/tadas/Dev/SQL-parser/output.json"
3. Enable systemd service for the script to constantly run and tail the log entries.
  3.1. vim /etc/systemd/system/sql-parser.service
  3.2. paste 
[Unit]
Description=MySQL Slow Log Parser Agent
After=network.target

[Service]
User=root
WorkingDirectory=/home/tadas/Dev/SQL-parser
ExecStart=/usr/bin/python3 /parser.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target

4. systemctl daemon-reload
5. systemctl enable sql-parser.service
6. systemctl start sql-parser.service
