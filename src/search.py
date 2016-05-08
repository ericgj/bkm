import re

EXCLUDES = [
'a','an','and','are','as','at','but','by','for','if','in','is','it','no','not',
'of','on','or','that','the','then','there','these','they','this','to','was',
'will','with'
]

CLEAN_PATTERN = re.compile('[,:;!?%+*<>"\'\(\)\[\]\{\}\\\/]') 
DOT_PATTERN = re.compile('[.]\s+')

def tokenize(text):
  return re.split('\s+', clean(text))

def clean(text):
  text = re.sub(CLEAN_PATTERN,'',text)
  return re.sub(DOT_PATTERN, ' ',text).lower()  # to avoid cleaning domain names

def normalize(words):
  return (
    [ clean(w) for w in words if (
        (not clean(w) in EXCLUDES) and (not clean(w) == '') ) 
    ]
  )


"""
Index given text as normalized token sequences with factor = 1
"""
def index_text(text):
  return index_seq(1,{},normalize(tokenize(text))).items()



def nonseq_list(factor,words):
  return (factor,False,words)

def seq_list(factor,words):
  return (factor,True,words)

"""
Index multiple word-lists specified as (factor, is-sequential, words)
"""
def index(wlists):
  def _index(acc,(fac,isseq,words)):
    if isseq:
      return index_seq(fac,acc,words)
    else:
      return index_nonseq(fac,acc,words)
  return reduce(_index, wlists, {}).items()


def index_nonseq(factor,init,words):
  def _index(acc,word):
    k = index_key([word])
    if acc.has_key(k):
      acc[k] = acc[k] + factor
    else:
      acc[k] = factor
    return acc

  return reduce(_index, words, init)

def index_seq(factor,init,words):
  def _index_grp(acc,grp):
    k = index_key(grp)
    if acc.has_key(k):
      acc[k] = acc[k] + factor
    else:
      acc[k] = factor
    return acc

  def _index(acc,n):
    grps = nwords(n+1,words)
    return reduce(_index_grp, grps, acc)

  return reduce(_index, range(0,3), init)


def index_key(words):
  return tuple(words)

def nwords(n,words):
  max = len(words)
  return [ words[i:i+n] for i in range(0, max) if i < (max - n + 1) ]

