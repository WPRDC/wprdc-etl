import os
import sqlite3
import argparse
import json

HERE = os.path.abspath(os.path.dirname(__file__))

parser = argparse.ArgumentParser(description='Create a new status DB')
parser.add_argument('--server', help='Name of the server')
parser.add_argument('--drop', help='Drop the existing status table', dest='drop', action='store_true')
parser.set_defaults(drop=False)

server = parser.parse_args().server
drop = parser.parse_args().drop

if not server:
    raise RuntimeError('You must specify a server with --server')

with open(os.path.join(HERE, 'settings.json')) as f:
    settings = json.loads(f.read())[server]

conn = sqlite3.connect(settings['statusdb'])
cur = conn.cursor()

if drop:
    print('Dropping table...')
    cur.execute('''DROP TABLE IF EXISTS status''')
    conn.commit()

print ('Creating table...')
cur.execute('''
CREATE TABLE IF NOT EXISTS
status (
    name TEXT NOT NULL,
    display_name TEXT,
    last_ran INTEGER,
    start_time INTEGER NOT NULL,
    status TEXT,
    num_lines INTEGER,
    PRIMARY KEY (display_name, start_time)
)
''')
conn.commit()
