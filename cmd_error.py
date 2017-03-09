#!/usr/bin/env python
import os, datetime, logging
import sqlite3 as lite
from optparse import OptionParser #NB zeus does not have argparse!
from dateutil.parser import parse

DB_FILE = os.environ['CMD_DB_FILE']

parser = OptionParser(usage = "usage: %prog N command <options>" +
"""
Identify tasks in an array that have not successfully completed with output 0

N        - number of jobs in array (tasks are assumed to span 1 - N
command  - e.g. wsclean
""")
parser.add_option("-s", "--startrow", dest="startrow", default=0, type=int, help="ignore any row earlier than this one")
parser.add_option("-e", "--endrow", dest="endrow", default=None, type=int, help="ignore any row later than this one")
parser.add_option("--sep", dest="sep", default=',', type=str, metavar="SEPARATOR", help="Separator [default=%default]")

opts, args = parser.parse_args()
if len(args) != 2:
    parser.error("incorrect number of arguments")
n = int(args[0])
task = args[1]
all = set(range(n))

con = lite.connect(DB_FILE)
with con:
    cur = con.cursor()
    query = "SELECT TaskId FROM Commands WHERE Command is ? AND Rownum>? AND Exit is 0"
    options = [task, opts.startrow]
    if opts.endrow is not None:
        query += " and Rownum<?"
        options.append(opts.endrow)
    cur.execute(query, options)
    good = set(i[0] for i in cur.fetchall())
bad = all - good

print opts.sep.join((str(b) for b in bad))
