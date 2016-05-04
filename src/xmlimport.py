
def parse_link((last,acc), el):
  if el.tag == 'DD':
    last['comment'] = parse_text(''.join(el.itertext()))
    return (last, acc)

  if el.tag == 'DT':
    if len(last) > 0:
      acc.append(last)
    a = el.find('A')
    next = {
      'link': parse_text(None if a is None else a.attrib.get('HREF',None))
    , 'title': parse_text(None if a is None else ''.join(a.itertext()))
    , 'tags': parse_tags(None if a is None else a.attrib.get('TAGS',None))
    , 'private': parse_private(None if a is None else a.attrib.get('PRIVATE',None))
    , 'date': parse_date(None if a is None else a.attrib.get('ADD_DATE',None)) 
    }
    return (next, acc)

def parse_text(s):
  if s is None:
    return ''
  else:
    return s

def parse_date(d):
  if d is None:
    return 0
  else:
    try:
      return int(d)
    except:
      return 0

def parse_private(p):
  if p is None:
    return 1
  else:
    return p

def parse_tags(tags):
  if tags is None:
    return []
  else:
    return map(lambda s: s.strip(), tags.split(','))

def parse_links(root):
  (last, head) = reduce(parse_link, root, ({},[]))
  return head + [last]


