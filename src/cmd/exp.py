import sys

from f import fapply, curry
from taskutil import pass_through
import db
import xmlexport as xml


def run(args):
  task = None
  writer = string_writer

  if args.format == 'xml':
    task = get_links(args) >> xml.build
    writer = xml_writer
  
  if task is None:
    sys.stderr.write("No such format found: %s\n" % args.format)
    sys.exit(1)

  task.fork(handle_error, handle_success(args.out,writer))


@curry
def handle_success(out,write,result):
  if len(out) == 0:
    write(result,sys.stdout)
  else:
    with open(out,'wb') as f:
      write(result,f)

def handle_error(e):
  sys.stderr.write(str(e))
  sys.stderr.write("\n")
  sys.exit(1)

def get_links(args):
  return (
    (( db.connect() 
         >> pass_through(db.find_all(-1)) )
         >> fapply(db.get_links) )
  )

def string_writer(s,f):
  f.write(s)

def xml_writer(tree,f):
  tree.write(f, encoding="utf-8", xml_declaration=True, method="xml")
  
