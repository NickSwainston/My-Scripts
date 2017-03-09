#!/usr/bin/env python
import os, datetime, logging
import sqlite3 as lite
from optparse import OptionParser #NB zeus does not have argparse!
from dateutil.parser import parse

DB_FILE = os.environ['CMD_DB_FILE']

parser = OptionParser(usage = "usage: %prog rownum [exit_code] <options>" +
"""
A script to be run immediately after a substantial command (e.g. obsdownload, wsclean, cotter etc.) is run.

The time that the task completed is recorded along with the exit code.
""")
parser.add_option("-e", "--exit", dest="exit", default=None, type=str, help="Command exit code [default=None]. Should not be set if task did not complete")
parser.add_option("-t", "--time", dest="time", default=None, type=str, metavar="time string", help="Command completion time [default=NOW]")
parser.add_option("--errfile", dest="errfile", default=None, type=str, metavar="FILE", help="Set completion time to modification time of FILE (useful if job has timed out)")
parser.add_option("--stopall", dest="stopall", default=None, type=str, metavar="LOGDIR", help="Set completion time to corresponding errorfile LOGDIR/JobId.err")
parser.add_option("--startrow", dest="startrow", default=None, type=int, help="ignore any row earlier than this one")
parser.add_option("--endrow", dest="endrow", default=None, type=int, help="ignore any row later than this one")
parser.add_option("-f", "--force", dest="force", action="store_true", help="don't check existing completion time")
parser.add_option("-n", "--no-overwrite", dest="no_overwrite", action="store_true", help="don't overwrite if completion time is already set")

opts, args = parser.parse_args()
if len(args) != 1:
    parser.error("incorrect number of arguments")
if opts.time and opts.errfile:
    logging.warn("--time and --errfile both set. --time will be used")

if opts.time:
    end_time = parse(opts.time)
elif opts.errfile:
    opts.errfile = os.path.expanduser(os.path.expandvars(opts.errfile))
    end_time = datetime.datetime.fromtimestamp(os.path.getmtime(opts.errfile))
else:
    end_time = datetime.datetime.now()

con = lite.connect(DB_FILE)
with con:
    cur = con.cursor()
    if not opts.force:
        cur.execute("SELECT Ended FROM Commands WHERE Rownum=?", (args[0],))
        ended = cur.fetchone()[0]
        if ended is not None:
            if opts.no_overwrite:
                raise RuntimeError, "Ended is already set"
            else:
                logging.warn("Overwriting existing completion time: %s" % ended)
    cur.execute("UPDATE Commands SET Ended=?, Exit=? WHERE Rownum=?", (end_time, opts.exit, args[0]))
