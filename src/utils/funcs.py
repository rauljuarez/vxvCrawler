'''
Created on Sep 5, 2009

@author: eorbe
'''

import unicodedata

def strip_accents(s):
    try:
        return unicodedata.normalize('NFKD', s).encode('ASCII','ignore')
    except:
        try:
            return unicodedata.normalize('NFKD',unicode(s)).encode('ASCII','ignore')
        except:
            return unicodedata.normalize('NFKD',unicode(s,'utf8')).encode('ASCII','ignore')
