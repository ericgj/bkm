
from f import curry
from pymonad.Maybe import Nothing, Just
from taskmonad import Task

@curry
def with_default(val,maybe):
  if isinstance(maybe,Just): 
    return maybe.getValue() 
  elif maybe == Nothing:
    return val
  else:
    raise TypeError("Not a Maybe")

@curry
def to_task(e,maybe):
  return with_default(
    Task( lambda rej,_: rej(e) ),
    maybe.fmap( lambda x: Task( lambda _,res: res(x) ) )
  )

@curry
def all_of(maybes):
  def _filter(m):
    if isinstance(m,Just):
      return True
    elif m == Nothing:
      return False
    else:
      raise TypeError("Not a Maybe")

  return [ m.getValue() for m in filter(_filter,maybes) ]

