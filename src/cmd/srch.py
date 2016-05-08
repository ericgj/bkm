import sys
import json

from f import curry, flip
from search import normalize, tokenize
import db
import xmlexport as xml


def run(args):
  task = None
  writer = string_writer
  format = 'json' if args.format is None else args.format
  terms = args.terms
  
  if format == 'xml':
    task = search_links(terms) >> xml.build
    writer = xml_writer
  
  elif format == 'json':
    task = search_links(terms)
    writer = json_writer

  if task is None:
    sys.stderr.write("No such format found: %s\n" % format)
    sys.exit(1)

  task.fork(handle_error, handle_success(writer))


@curry
def handle_success(write,result):
  write(result,sys.stdout)
  sys.stdout.write("\n")

def handle_error(e):
  sys.stderr.write(str(e))
  sys.stderr.write("\n")
  sys.exit(1)
 
def string_writer(s,f):
  f.write(s)

def json_writer(obj,f):
  json.dump(obj, f, encoding="utf-8", indent=2)

def xml_writer(tree,f):
  tree.write(f, encoding="utf-8", xml_declaration=True, method="xml")


def search_links(terms):
  def _search(cnn):
    return db.find_phrases_all(terms,cnn) >> flip(db.get_links)(cnn)
  return db.connect() >> _search
 
