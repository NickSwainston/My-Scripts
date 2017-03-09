#!/usr/bin/env python
import os, datetime
import sqlite3 as lite
from optparse import OptionParser

DB_FILE = os.environ['CMD_DB_FILE']

parser = OptionParser(usage = "usage: %prog rownum" +
"""
Delete erroneous row in database
rownum - Row number in database
""")


opts, args = parser.parse_args()
if len(args) != 1:
    parser.error("incorrect number of arguments")

con = lite.connect(DB_FILE)
with con:
    cur = con.cursor()
    cur.execute("DELETE from Commands WHERE Rownum=?", (args[0],))
