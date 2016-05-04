from xml.etree import cElementTree as ET

from taskmonad import Task
import err

# List Link -> Task Error ElementTree
def build(links):
  def _build(rej,res):
    try:
      root = ET.Element('body')
      dl = ET.SubElement(root,'dl')
      build_links(dl, links)
      res(ET.ElementTree(root))
    except Exception as e:
      rej(err.build(e))

  return Task(_build)


def build_links(el, links):
  for link in links:
    build_link(el, link)

def build_link(el, link):
  href = link.get('link').decode('utf-8')
  tags = u",".join(link.get('tags',[])).decode('utf-8')
  ts = unicode(link.get('date','')).decode('utf-8')
  private = '1' if link.get('private',True) else '0'
  title = link.get('title',None)
  txt = link.get('comment',None)

  dt = ET.SubElement(el, 'dt')
  a = ET.SubElement(dt, 'a', href=href, tags=tags, add_date=ts, private=private)
  if not title is None:
    a.text = title.decode('utf-8')
  if not txt is None:
   dd = ET.SubElement(el, 'dd')
   dd.text = txt.decode('utf-8')

