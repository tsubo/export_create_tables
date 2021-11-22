#!/usr/bin/env python

import os
import MySQLdb
from dotenv import load_dotenv

OUTPUT_DIR = './output'


def connect_db():
    return MySQLdb.connect(
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        db=os.getenv('DB'),
        use_unicode=True,
        charset='utf8'
    )


def show_tables(cur):
    cur.execute('show tables')
    rows = cur.fetchall()
    return [cols[0] for cols in rows]


def show_crate_table(cur, table):
    cur.execute(f'show create table {table}')
    return cur.fetchall()


def export_create_table(table_name, rows):
    filename = f'{OUTPUT_DIR}/{table_name}.sql'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        for cols in rows:
            for col in cols[1:]:  # １カラム目はテーブル名なのでスキップ
                f.write(col)


load_dotenv()
with connect_db() as conn:
    cur = conn.cursor()

    table_names = show_tables(cur)
    for table_name in table_names:
        rows = show_crate_table(cur, table_name)
        export_create_table(table_name, rows)
