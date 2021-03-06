#!/usr/bin/env python
import os, datetime, logging
import sqlite3 as lite
from optparse import OptionParser #NB zeus does not have argparse!
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

DB_FILE = os.environ['CMD_DB_FILE']

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

parser = OptionParser(usage = "usage: %prog <options>" +
"""
Print rows from the job database
""")

con = lite.connect(DB_FILE)
#con = lite.connect(DB_FILE, detect_types=lite.PARSE_DECLTYPES|lite.PARSE_COLNAMES) # return datetime as datetime objects
con.row_factory = dict_factory
parser.add_option("-u", "--unfinished", dest="unfinished", action="store_true", help="print only jobs with no endtime")
parser.add_option("-r", "--recent", dest="recent", metavar="HOURS", default=None, type=float, help="print only jobs started in the last N hours")
parser.add_option("-n", "--number", dest="n", metavar="N", default=20, type=int, help="number of jobs to print [default=%default]")
parser.add_option("-a", "--all", dest="all", action="store_true", help="print all lines of the database")
parser.add_option("-A", "--args", dest="args", action="store_true", help="print command arguments")
parser.add_option("-s", "--startrow", dest="startrow", default=0, type=int, help="ignore any row earlier than this one")
parser.add_option("-e", "--endrow", dest="endrow", default=None, type=int, help="ignore any row later than this one")
opts, args = parser.parse_args()

if len(args) != 0:
    parser.error("Incorrect number of arguments")

query = "SELECT * FROM Commands"

if opts.unfinished:
    query += " WHERE Ended IS NULL"

if opts.recent is not None:
    query += ''' WHERE Started > "%s"''' % str(datetime.datetime.now() - relativedelta(hours=opts.recent))
    logging.debug(query)

with con:
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()

if opts.startrow or opts.endrow:
    rows = rows[opts.startrow:]
    if opts.endrow is not None:
        rows = rows[:opts.endrow+1]
elif not (opts.all or opts.unfinished or opts.recent):
    rows = rows[-opts.n:]

print 'RowNum','JobId','TaskId','Datadir','Project','Obsid','Channels','Start','End','Exit','User'

for row in rows:
    print str(row['Rownum']).rjust(4),
    print str(row['JobId']).rjust(7),
    print str(row['TaskID']).rjust(4),
    print row['Datadir'] + os.sep,
    print row['Project'] + os.sep,
    print row['Obsid'],
    if row['Channels'] is not None:
        print row['Channels'].ljust(7),
    else:
        print '-------',
    print row['Started'][:19],
    if row['Ended'] is not None:
        print row['Ended'][11:19].ljust(8),
    else:
        print '--------',
    print row['Command'].ljust(15),
    if row['Exit'] is not None:
        print str(row['Exit']).rjust(3),
    else:
        print "---",
    print row['UserId'].ljust(10),
    if opts.args:
        print row['Arguments']
    print "\n"
