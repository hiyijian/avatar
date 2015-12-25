# -*- coding: utf-8 -*- 
import os, re, sys
from gensim import corpora, models, similarities
reload(sys)
sys.setdefaultencoding('utf8')

def make_dict(in_fd, out_fd, no_below, no_above, keep_n):
	dictionary = corpora.Dictionary(line.lower().split() for line in in_fd)
        dictionary.filter_extremes(no_below=no_below, no_above=no_above, keep_n=keep_n)
	dictionary.compactify()
        print dictionary
	dictionary.save(out_fd)
