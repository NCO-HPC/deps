#!/usr/bin/env python3

import os, sys
import argparse
import sqlite3

parser = argparse.ArgumentParser(description='Modify and query WCOSS operational data dependency database.')
group = parser.add_mutually_exclusive_group()
group.add_argument('--add', '-a', nargs=4, help='add dependency: <model> <ver> <file path> <priority (primary-crit/primary-opp/backup)>')
group.add_argument('--query-model', '-m', nargs=2, help='Query by model: <model> <ver>')
group.add_argument('--query-file', '-f', type=str, help='Query by file path: <file path>')
group.add_argument('--get-models', '-g', type=str, help='Query by file path and return list of models: <file path>')
group.add_argument('--change-version','-c', nargs=3, help='Change version for a given model')
group.add_argument('--remove','-r', nargs=3, help='Remove row(s) based on model version path')
group.add_argument('--createdb', action='store_true', help=argparse.SUPPRESS)
parser.add_argument('--debug', action='store_true', help=argparse.SUPPRESS)
parser.add_argument('--delimiter','-d',type=str,default='|')
parser.add_argument('--priority','-p',choices=["primary-crit","primary-opp","backup"],help='Filter by priority')
args = parser.parse_args()

# Load database (location defined in env module):
DEPS_DB = os.getenv("DEPS_DB")
assert DEPS_DB, "DEPS_DB environment variable not defined. Did you load the 'deps' module?"
try: os.mkdir(os.path.dirname(DEPS_DB))
except FileExistsError: pass
con = sqlite3.connect(DEPS_DB)
cur = con.cursor()

def validate_version(ver):
  assert ver[0]=="v" and ver.count(".")==1, "Model version number must start with 'v' and be two digits"

def create_empty_db():
  input(f"Are you sure you want to attempt to create an empty database file at {DEPS_DB} ?\nEnter to continue, Ctrl-c to quit")
  cur.execute("CREATE TABLE dcom (model text, version text, path text, priority text, PRIMARY KEY(model,version,path))")
  con.commit()

def add_dependency(arglist):
  validate_version(arglist[1])
  assert arglist[3] in ("primary-crit","primary-opp","backup"), "Priority (fourth argument) must be 'primary-crit' 'primary-opp' or 'backup'"
  input("Confirm values:\n  model: %s\n  version: %s\n  file path: %s\n  priority: %s\nEnter to confirm, Ctrl-c to quit"%tuple(arglist))
  newvalues = ",".join(["'"+arg+"'" for arg in arglist])
  try:
    cur.execute(f"INSERT INTO dcom VALUES ({newvalues})")
    con.commit()
    print("Values successfully added.")
  except sqlite3.IntegrityError:
    print("ERROR: This entry (model/version/file path combination) already exists. Database will not be modified.")
    sys.exit(1)

def remove_dependency(arglist):
  model = arglist[0] ; version = arglist[1] ; path = arglist[2]
  remove_cmd = f"DELETE FROM dcom WHERE model LIKE '{model}' AND version LIKE '{version}' AND path LIKE '{path}'"
  if args.debug: print(remove_cmd)
  cur.execute(remove_cmd)
  con.commit()

def query(query_cmd):
  if args.debug: print(query_cmd)
  query_output = cur.execute(query_cmd)
  for row in query_output:
    print(args.delimiter.join(row))

def query_by_model(arglist):
  query_cmd = "SELECT * FROM dcom WHERE model LIKE '%s' AND version LIKE '%s'%s"%(tuple(arglist)+(prioritytext,))
  query(query_cmd)

def query_by_file(path):
  query_cmd = "SELECT * FROM dcom WHERE path LIKE '%s'%s"%(path,prioritytext)
  query(query_cmd)

def get_models(path):
  query_cmd = "SELECT model,version FROM dcom WHERE path LIKE '%s'%s GROUP BY model,version"%(path,prioritytext)
  query(query_cmd)

def change_version(arglist):
  model = arglist[0] ; oldversion = arglist[1] ; newversion = arglist[2]
  update_cmd = f"UPDATE dcom SET version = '{newversion}' WHERE model LIKE '{model}' AND version LIKE '{oldversion}'"
  if args.debug: print(update_cmd)
  cur.execute(update_cmd)
  con.commit()
  print("Updated entries:")
  query_by_model([model,newversion])
  print("Checking for residual entries:")
  query_by_model([model,oldversion])

if args.priority: prioritytext = " AND priority LIKE '%s'"%args.priority
else: prioritytext = ""

if args.createdb: create_empty_db()

if args.add: add_dependency(args.add)

if args.query_model: query_by_model(args.query_model)

if args.query_file: query_by_file(args.query_file)

if args.get_models: get_models(args.get_models)

if args.change_version: change_version(args.change_version)

if args.remove: remove_dependency(args.remove)

con.close()
