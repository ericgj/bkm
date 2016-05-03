# functional toolbelt

# TODO: reimplement curry here
from pymonad.Reader import curry

def identity(x):
  return x

def always(x):
  return lambda _: x

def fst((x,_)): 
  return x

def snd((_,x)): 
  return x

def flip(fn):
  def _fn(a,b,*args,**kwargs):
    return fn(b,a,*args,**kwargs)
  return curry(_fn)

@curry
def ap(fns,vals):
  def _concat(acc, f):
    return acc + map(f,vals)
  return reduce(_concat, fns, [])

def zipapply(fns,vals):
  return [ f(v) for (f,v) in zip(fns,vals) ]

@curry
def applyf(args,fn):
  return fn(*args)

fapply = flip(applyf)

@curry
def tuple2(a,b):
  return (a,b)

def compose(*fs):
  if fs:
    pair = lambda f,g: lambda *a,**kw: f(g(*a,**kw))
    return reduce(pair, fs, identity)
  else:
    return identity

@curry
def fmap(f,it):
  return map(f,it) 

def merge(d2,d1):
  return dict( d1.items() + d2.items() )

def assoc(k,v,d):
  return merge({k:v},d)

def dissoc(k,d):
  return dict( [(k0,v) for (k0,v) in d.items() if not k == k0] )

def pick(ks,d):
  def _pick(acc,k):
    if d.has_key(k):
      acc[k] = d[k]
    return acc
  return reduce(_pick,ks,{})

def flatten(l):
  def _flat(acc,xs):
    for x in xs:
      acc.append(x)
    return acc
  return reduce(_flat, l, [])

