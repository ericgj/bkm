import sys

import db

def run(args):
  task = (
    ( db.connect() 
         >> db.get_and_del_link(args.url) )
  )

  task.fork(handle_error, handle_success)


def handle_success(n):
  if n == 0:
    sys.stdout.write("Link not found")
  else:
    sys.stdout.write("Link deleted")
  sys.stdout.write("\n")

def handle_error(e):
  sys.stderr.write("An internal error occurred.\n\n")
  sys.stderr.write(str(e))
  sys.stderr.write("\n")
  sys.exit(1)


