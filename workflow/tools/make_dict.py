# -*- coding: utf-8 -*- 
import os, re, sys, codecs
from gensim import corpora, models, similarities
reload(sys)
sys.setdefaultencoding('utf8')

'''
def strict_handler(exception):
	print exception
    	return u"", exception.end
 
codecs.register_error("strict", strict_handler)
'''

def make_dict(in_fd, out_fd, no_below, no_above, keep_n):
	for line in in_fd:
		print line
		line = line.decode('utf-8')
	'''
	dictionary = corpora.Dictionary(line.decode('utf-8', errors='ignore').lower().split() for line in in_fd)
        dictionary.filter_extremes(no_below=no_below, no_above=no_above, keep_n=keep_n)
	dictionary.compactify()
        print dictionary
	dictionary.save(out_fd)
	'''