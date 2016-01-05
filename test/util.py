import json
import sqlite3

def create_status_table(server, config_file):
    with open(config_file) as f:
        db = json.loads(f.read())[server]['statusdb']
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS status (
        name TEXT NOT NULL, display_name TEXT,
        last_ran INTEGER, start_time INTEGER NOT NULL,
        status TEXT, num_lines INTEGER,
        PRIMARY KEY (display_name, start_time)
    )''')
    conn.commit()
    conn.close()
