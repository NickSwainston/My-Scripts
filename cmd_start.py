#!/usr/bin/env python
import os, datetime, logging #NB Zeus does not have argparse!
import sqlite3 as lite
from optparse import OptionParser

try:
	JOB_ID=os.environ['SLURM_JOB_ID']
except KeyError:
	logging.warning("SLURM_JOB_ID variable could not be found")
	JOB_ID=None

try:
	TASK_ID=os.environ['SLURM_ARRAY_TASK_ID']
except KeyError:
	logging.info("SLURM_ARRAY_TASK_ID variable could not be found")
	TASK_ID=None

DB_FILE = os.environ['CMD_DB_FILE']

parser = OptionParser(usage="Usage: %prog <options> command" +
"""

A script to be run immediately before a substantial command (e.g. obsdownload,
wsclean, cotter etc.) is run. It prints a single rownum (and only a single
rownum) to standard out. This should be caputured (e.g. by `backticks`) to pass
to stop_job.py recording when the command is complete.
""")

parser.add_option("-d", "--datadir", dest="datadir", default=None, type=str)
parser.add_option("-p", "--project", dest="project", default=None, type=str)
parser.add_option("-o", "--obsid", dest="obsid", type=int)
parser.add_option("-a", "--arguments", dest="arguments", default=None, type=str)
parser.add_option("-c", "--chans", dest="chans",  default=None, type=str)
opts, args = parser.parse_args()

if not len(args) == 1:
    parser.error("command must be set")

#Die if not all options are set (probably obsid and command are the absolute minimum)
if opts.obsid is None:
    parser.error("obsid must be set")

con = lite.connect(DB_FILE)
with con:
    cur = con.cursor()
    cur.execute("INSERT INTO Commands (Datadir, Project, Obsid, JobId, TaskId, Command, Channels, Arguments, UserId, Started) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (opts.datadir, opts.project, opts.obsid, JOB_ID, TASK_ID, args[0], opts.chans, opts.arguments, os.environ['USER'], datetime.datetime.now()))
    print cur.lastrowid
