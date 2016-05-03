import redis

from f import curry, compose, identity, zipapply, flatten
from pymonad.Maybe import Just, Nothing
from maybeutil import all_of
from taskmonad import Task
import taskutil
import err

from search import tokenize, normalize, index_key, index, nwords

def dbkey_link(url):
  return u'link|%s' % url

def dbkey_link_tags(url):
  return u'link|%s|tags' % url

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
    url = link.get('link')
    tags = link.get('tags')
    comment = link.get('comment')
    idx = (
      index(
        10, 
        tags, 
        normalize(tokenize('' if comment is None else comment))
      )
    )
    print idx
    
    try:
      pipe.hmset(dbkey_link(url), link)
      if len(tags) > 0:
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
def add_links(links, cnn):
  execs = [add_link(link,cnn) for link in links]
  return taskutil.all(execs).fmap(lambda rs: len(rs))


# Connection -> Url -> Task Error Link
@curry
def get_link(cnn, url):
  def _get(rej,res):
    try:
      link = cnn.hmget(dbkey_link(url), *fields)
      tags = cnn.smembers(dbkey_link_tags(url))
      if link is None:
        res(Nothing)
      else:
        keys = fields + [u'tags']
        vals = zipapply( parses + [list], link + [tags] )
        res( Just( dict(zip(keys,vals)) ) )

    except Exception as e:
      rej(err.build(e))

  fields = [u'link',   u'date', u'comment', u'private']
  parses = [ identity, int,   identity,  lambda v: v == "1" ]
  return Task(_get)


# Connection -> List Url -> Task Error (List Link)
@curry
def get_links(cnn, urls):
  return taskutil.all( map(get_link(cnn),urls) ).fmap(all_of)


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

