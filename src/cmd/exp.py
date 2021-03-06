import sys
import json

from f import curry, flip
import db
import xmlexport as xml


def run(args):
  task = None
  writer = string_writer
  format = 'json' if args.format is None else args.format

  if format == 'xml':
    task = get_links(args) >> xml.build
    writer = xml_writer
  
  elif format == 'json':
    task = get_links(args)
    writer = json_writer

  if task is None:
    sys.stderr.write("No such format found: %s\n" % format)
    sys.exit(1)

  task.fork(handle_error, handle_success(args.out,writer))


@curry
def handle_success(out,write,result):
  if len(out) == 0:
    write(result,sys.stdout)
  else:
    with open(out,'wb') as f:
      write(result,f)
  sys.stdout.write("\n")

def handle_error(e):
  sys.stderr.write(str(e))
  sys.stderr.write("\n")
  sys.exit(1)

def get_links(args):
  def _get(cnn):
    return db.find_all(-1,cnn) >> flip(db.get_links)(cnn)
  return db.connect() >> _get 

def string_writer(s,f):
  f.write(s)

def json_writer(obj,f):
  json.dump(obj, f, encoding="utf-8", indent=2)

def xml_writer(tree,f):
  tree.write(f, encoding="utf-8", xml_declaration=True, method="xml")
  
