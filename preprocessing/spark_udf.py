import  pyspark.sql.functions  as F
from  pyspark.sql.types  import *
import jieba


@F.udf(returnType=StringType())
def clean_text(doc):
    import re
    num='remove'
    pa = "[a-zA-Z1-9]"
    doc=re.sub(pa,'',doc)
    doc=re.sub('[\/\.\:@\-\ ]','',doc)
    if num is 'spell':
        doc = doc.replace(u'0', u' 零 ')
        doc = doc.replace(u'1', u' 一 ')
        doc = doc.replace(u'2', u' 二 ')
        doc = doc.replace(u'3', u' 三 ')
        doc = doc.replace(u'4', u' 四 ')
        doc = doc.replace(u'5', u' 五 ')
        doc = doc.replace(u'6', u' 六 ')
        doc = doc.replace(u'7', u' 七 ')
        doc = doc.replace(u'8', u' 八 ')
        doc = doc.replace(u'9', u' 九 ')
    elif num is 'substitute':
        # All numbers are equal. Useful for embedding (countable words) ?
        doc = re.sub(u'(\\d+)', ' NUM ', doc)
    elif num is 'remove':
        # Numbers are uninformative (they are all over the place). Useful for bag-of-words ?
        # But maybe some kind of documents contain more numbers, e.g. finance.
        # Some documents are indeed full of numbers. At least in 20NEWS.
        doc = re.sub('[0-9]', '', doc)
    # Remove everything except a-z characters and single space.
    doc = doc.replace(u'$', u' 美元 ')
#     doc = ' '.join(doc.split())  # same as doc = re.sub('\s{2,}', ' ', doc)
    return doc



@F.udf(returnType=ArrayType(StringType()))
def textCut(row):
    return jieba.lcut(row)

@F.udf(returnType=StringType())
def toSpaceSplit(row):
    words=' '.join(map(lambda word:diction.value.get(word,''),row))
    return words

@F.udf(returnType=StringType())
def concat_uid(row):
    return ' '.join(row)
