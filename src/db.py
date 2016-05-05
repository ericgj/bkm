import redis

from f import curry, compose, identity, zipapply, flatten, flip, always
from pymonad.Maybe import Just, Nothing
from maybeutil import with_default, all_of
from taskmonad import Task
import taskutil
import err

from search import tokenize, normalize, index_key, index, nwords

def dbkey_links():
  return u'links'

def dbkey_link(url):
  return u'link|%s' % url

def dbkey_link_tags(url):
  return u'link|%s|tags' % url

def dbkey_tags():
  return u'tags'

def dbkey_tag(tag):
  return u'tag|%s' % tag

def dbkey_search_freq(tokens):
  return u'search|freq|%s' % u','.join(tokens)

def dbkey_search_results_all(phrases):
  return u'search|results|all|%s' % u';'.join([u','.join(ph) for ph in phrases])

def dbkey_search_results_any(phrases):
  return u'search|results|any|%s' % u';'.join([u','.join(ph) for ph in phrases])


def connect(**kwargs):
  def _connect(rej,res):
    try:
      res(redis.StrictRedis(**kwargs))

    except Exception as e:
      rej(err.build(e))

  return Task(_connect)


@curry
def add_link(link, cnn):
  def _count(results):
    return 1

  def _add(rej,res):
    pipe = cnn.pipeline()
    url = link.get(u'link')
    title = link.get(u'title','')
    tags = link.get(u'tags',[])
    comment = link.get(u'comment','')
    dt = link.get(u'date',0)
    idx = (
      index(
        10, 
        tags, 
        normalize(tokenize(title + " " + comment))
      )
    )
    
    try:
      pipe.zadd(dbkey_links(), int(dt), url)
      pipe.hmset(dbkey_link(url), link)
      if len(tags) > 0:
        pipe.sadd(dbkey_tags(), *tags)
        pipe.sadd(dbkey_link_tags(url), *tags) 
      
      for tag in tags:
        pipe.sadd(dbkey_tag(tag), url)

      for (tokens, freq) in idx:
        pipe.zadd(dbkey_search_freq(tokens), freq, url)

      res(pipe.execute())

    except Exception as e:
      rej(err.build(e))

  return Task(_add).fmap(_count)


@curry
def del_link(link, cnn):
  def _count(x):
    return 1

  def _del(rej,res):
    pipe = cnn.pipeline()
    url = link.get(u'link')
    title = link.get(u'title','')
    tags = link.get(u'tags',[])
    comment = link.get(u'comment','')
    dt = link.get(u'date',0)
    idx = (
      index(
        10, 
        tags, 
        normalize(tokenize(title + " " + comment))
      )
    )
    
    try:
      pipe.zrem(dbkey_links(), url)
      pipe.hdel(dbkey_link(url), *link.keys())
      if len(tags) > 0:
        pipe.srem(dbkey_tags(), *tags)
        pipe.srem(dbkey_link_tags(url), *tags)

      for tag in tags:
        pipe.srem(dbkey_tag(tag), url)

      for (tokens, freq) in idx:
         pipe.zrem(dbkey_search_freq(tokens), url)

      res(pipe.execute())

    except Exception as e:
      rej(err.build(e))

  return Task(_del).fmap(_count)


@curry
def upsert_link(link, cnn):
  def _maybe_del(mlink):
    return (
      with_default(
        taskutil.resolve(Nothing), 
        mlink.fmap(flip(del_link)(cnn))
      )
    )
  
  return (
    (( get_link(cnn, link.get('link'))
         >> _maybe_del )
         >> always( add_link(link, cnn) ) )
  )


@curry
def add_links(links, cnn):
  execs = [add_link(link,cnn) for link in links]
  return taskutil.all(execs).fmap(lambda rs: len(rs))

@curry
def upsert_links(links, cnn):
  execs = [upsert_link(link,cnn) for link in links]
  return taskutil.all(execs).fmap(lambda rs: len(rs))


# Connection -> Url -> Task Error (Maybe Link)
@curry
def get_link(cnn, url):
  def _emptystr(x):
    return '' if x is None else unicode(x)

  def _get(rej,res):
    try:
      link = cnn.hmget(dbkey_link(url), *fields)
      tags = cnn.smembers(dbkey_link_tags(url))
      if all( x is None for x in link ):
        res(Nothing)
      else:
        keys = fields + [u'tags']
        vals = zipapply( parses + [list], link + [tags] )
        res( Just( dict(zip(keys,vals)) ) )

    except Exception as e:
      rej(err.build(e))

  fields = [u'link',   u'date', u'comment', u'private',             u'title'  ]
  parses = [_emptystr, int,     _emptystr,  lambda v: not v == "0", _emptystr ]
  return Task(_get)


# Connection -> List Url -> Task Error (List Link)
@curry
def get_links(cnn, urls):
  return taskutil.all( map(get_link(cnn),urls) ).fmap(all_of)


# Int -> Connection -> Task Error (List Url)
@curry
def find_all(limit,cnn):
  def _get(rej,res):
    try:
      res( cnn.zrevrange( dbkey_links(), 0, limit ) )
    except Exception as e:
      rej(err.build(e))

  return Task(_get)
      
# List String -> Connection -> Task Error (List Url)
@curry
def find_tagged_all(tags, cnn):
  def _get(rej,res):
    try:
      if len(tags) == 1:
        res( cnn.smembers(dbkey_tag(tags[0])) ) 
      else:
        res( cnn.sinter( *[dbkey_tag(t) for t in tags] ) )
    
    except Exception as e:
      rej(err.build(e))

  return Task(_get)


# List String -> Connection -> Task Error (List Url)
@curry
def find_tagged_any(tags, cnn):
  def _get(rej,res):
    try:
      if len(tags) == 1:
        res( cnn.smembers(dbkey_tag(tags[0])) ) 
      else:
        res( cnn.sunion( *[dbkey_tag(t) for t in tags] ) )
    
    except Exception as e:
      rej(err.build(e))

  return Task(_get)


# List String -> Connection -> Task Error (List Url)
@curry
def find_words_all(words, cnn):
  def _get(rej,res):
    try:
      if len(words) == 1:
        res( cnn.zrevrange(dbkey_search_freq([words]), 0, -1) )
      
      else:
        rkey = dbkey_search_results_all([[w] for w in words])
        r = cnn.zrevrange(rkey, 0, -1)
        if len(r) == 0:
          pipe = cnn.pipeline()
          pipe.zinterstore(rkey, [dbkey_search_freq([w]) for w in words] )
          pipe.expire(rkey,60)
          pipe.execute()
          r = cnn.zrevrange(rkey, 0, -1)
        res( r )  

    except Exception as e:
      rej(err.build(e))

  words = normalize(words)
  return Task(_get)


# List (List String) -> Connection -> Task Error (List Url)
@curry
def find_phrases_all(phrases, cnn):
  def _get(rej,res):
    try:
      rkey = dbkey_search_results_all(phrases)
      r = cnn.zrevrange(rkey, 0, -1)
      if len(r) == 0:
        sterms = flatten([ nwords(min(3,len(ph)),ph) for ph in phrases ])
        # print sterms
        pipe = cnn.pipeline()
        pipe.zinterstore(rkey, [dbkey_search_freq(index_key(term)) for term in sterms])
        pipe.expire(rkey,60)
        pipe.execute()
        r = cnn.zrevrange(rkey, 0, -1)
      res( r )  

    except Exception as e:
      rej(err.build(e))

  phrases = [normalize(tokenize(ph)) for ph in phrases]
  return Task(_get)

