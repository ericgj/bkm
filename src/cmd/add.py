import sys
import os
import os.path
from time import time
from tempfile import NamedTemporaryFile
import subprocess
import re

from f import merge, always, flip
import db
from taskmonad import Task
from maybeutil import with_default
import err

COMMENT_PATTERN = re.compile('^#')

class Aborted(Exception):
  pass

def run(args):
  dt = int(time())
  title = args.title
  tags = args.tags
  url = args.url
  input = (
    edit_link(title,url,'',tags)
      .fmap(parse_link_form)
      .fmap(merge({u'date': dt, u'link': url}))
  )

  task = (
    db.connect() 
      >> ( lambda cnn: (
             ( input 
                 >> flip(db.upsert_link)(cnn) )
                 >> always(db.get_link(url,cnn)) )
         )
  )

  task.fork(handle_error, handle_success)


def edit_link(title,link,comment,tags):
  def _rm(tmp):
    if os.path.exists(tmp):
      os.remove(tmp)

  def _edit(rej,res):
    try:
      buf = NamedTemporaryFile(prefix='link-', delete=False)
      buf.write( template(title,link,comment,tags) )
      buf.close()

      ed = os.environ.get('EDITOR','vim')
      subprocess.call([ed, buf.name], stdin=None, stdout=None, stderr=None)

      lines = []
      with open(buf.name) as f:
        lines = f.readlines()
      
      _rm(buf.name)

      if all( empty_or_comment_line(l) for l in lines ):
        rej(Aborted('Link not added, file is empty.'))
      else:
        res(lines)

    except Exception as e:
      try:
        _rm(buf.name)
      except:
        pass
      rej(err.build(e))

  return Task(_edit)


def template(title,link,comment,tags):
  header = [
    "# Enter a title, longer comment, and tags for this link",
    "# Any lines beginning with '#' and blank lines will be ignored",
    "#",
    "# " + link,
    "#"
  ]
  titlelines = [
    "# Title:",
    "",
    title
  ]
  commentlines = [
    "# Longer Comment:",
    "",
    comment,
    ""
  ]
  tagslines = [
    "# Enter tags separated by commas, or on separate lines:",
    "",
    ", ".join(tags),
    ""
  ]
  lines = header + titlelines + commentlines + tagslines
  return "\n".join(lines)


def parse_link_form(lines):
  def _parse((section,last,title,comment,tags),line):
    if empty_or_comment_line(line):
      return (section,False,title,comment,tags)
    else:
      section = section if last else section+1
      if section==0:
        title = ("%s\n%s" % (title,line.strip())).strip()
      elif section==1:
        comment = ("%s\n%s" % (comment,line.strip())).strip()
      elif section==2:
        tags = tags + [t.strip() for t in line.split(",")]
    return (section, True, title, comment, tags)

  (title,comment,tags) = reduce(_parse, lines, (-1, False, '', '', []))[2:]
  return {
    u'title': title,
    u'comment': comment,
    u'tags': tags
  }


def empty_or_comment_line(line):
  line = line.strip()
  return len(line)==0 or COMMENT_PATTERN.match(line)


def handle_success(link):
  sys.stdout.write( with_default(view_no_link(), link.fmap(view_link)) )
  sys.stdout.write("\n")

def handle_error(e):
  if not isinstance(e,Aborted):
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
      "",
      "# " + url,
      "",
      title,
      "",
      txt,
      "",
      "(no tags)" if len(tags)==0 else "tags: " + tags,
      ""
    ]
  )

