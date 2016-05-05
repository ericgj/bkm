import sys
from time import time

import db
from taskutil import pass_through
from maybeutil import with_default

def run(args):
  dt = int(time())
  sys.stderr.write("---> Enter comment, CTRL+D to finish <---\n")
  inp = sys.stdin.readlines()

  task = (
    (( db.connect() 
         >> pass_through(db.upsert_link( build_link(args,inp,dt) )) )
         >> (lambda (cnn,_): db.get_link(cnn, args.url)) )
  )

  task.fork(handle_error, handle_success)



def handle_success(link):
  sys.stdout.write( with_default(view_no_link(), link.fmap(view_link)) )
  sys.stdout.write("\n")

def handle_error(e):
  sys.stderr.write("An internal error occurred.\n\n")
  sys.stderr.write(str(e))
  sys.stderr.write("\n")
  sys.exit(1)

def view_no_link():
  return "No link found, perhaps it wasn't saved correctly?\nThat's weird. Try it again."

def view_link(link):
  title = link.get('title','')
  url = link.get('link','')
  txt = link.get('comment','')
  tags = ", ".join(link.get('tags',[]))
  return "\n".join(
    [
      "=" * len(title),
      title,
      "=" * len(title),
      "",
      url,
      "",
      txt,
      "",
      "(no tags)" if len(tags)==0 else "tags: " + tags
    ]
  )

def parse_input(inp,title='',tags=[]):
  def _parse((section,title,comment,tags),line):
    if len(line.strip()) == 0:
      return (section+1,title,comment,tags)
    else:
      if section==0:
        title = ("%s\n%s" % (title,line.strip())).strip()
      elif section==1:
        comment = ("%s\n%s" % (comment,line.strip())).strip()
      elif section==2:
        tags = tags + line.strip().split(",")
    return (section,title,comment,tags)

  section = 0 if len(title) == 0 else 1
  return reduce(_parse, inp, (section, title, '', tags))[1:]


def build_link(args,inp,dt):
  title, comment, tags = parse_input(inp,args.title,args.tags)
  link = args.url
  priv = args.private == True
  return {
    'link': link,
    'title': title,
    'tags': tags,
    'comment': comment,
    'date': dt,
    'private': priv
  }

