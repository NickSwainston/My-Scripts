#!/usr/bin/env python
import os, datetime, logging
import sqlite3 as lite
from optparse import OptionParser #NB zeus does not have argparse!
from dateutil.parser import parse

DB_FILE = os.environ['CMD_DB_FILE']

parser = OptionParser(usage = "usage: %prog <options>" +
"""
Set multiple commands to have stopped without completing at time given by error logfile
""")
parser.add_option("--logdir", dest="logdir", default="~/log", type=str, metavar="LOGDIR", help="directory containing logfiles")
parser.add_option("--prefix", dest="prefix", default="", type=str, metavar="PREFIX", help="path of filename before JobId [default '']")
parser.add_option("--suffix", dest="suffix", default=".err", type=str, metavar="PREFIX", help="path of filename after JobId [default %default]")
parser.add_option("--startrow", dest="startrow", default=0, type=int, help="ignore any row earlier than this one")
parser.add_option("--endrow", dest="endrow", default=None, type=int, help="ignore any row later than this one")

opts, args = parser.parse_args()
if len(args) != 0:
    parser.error("incorrect number of arguments")

con = lite.connect(DB_FILE)
with con:
    cur = con.cursor()
    query = "SELECT Rownum,JobId FROM Commands WHERE Ended is NULL AND Rownum>?"
    options = [opts.startrow]
    if opts.endrow is not None:
        query += " and Rownum<?"
        options.append(opts.endrow)
    cur.execute(query, options)
    logging.debug("rownum, errfile, end_time")
    for rownum, jobid in cur.fetchall():
        errfile = os.path.join(os.path.expanduser(opts.logdir), opts.prefix + str(jobid) + opts.suffix)
        end_time = datetime.datetime.fromtimestamp(os.path.getmtime(errfile))
        logging.debug("%d, %s, %s" % (rownum, errfile, end_time))
        cur.execute("UPDATE Commands SET Ended=? WHERE Rownum=?", (end_time, rownum))
