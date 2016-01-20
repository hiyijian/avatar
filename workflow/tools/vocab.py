# -*- coding: utf-8 -*- 
import os, re, sys, logging, time, inspect
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
        sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

class Vocab:
	def __init__(self, df):
		self.vocab = {}
		self.ndoc = 0
		for doc in df:
			self.__add_doc(doc)
			if self.ndoc % 10000 == 0:	
				print "\rprocessed %d documents" % (self.ndoc),
				sys.stdout.flush()
			self.ndoc += 1
		print "vocab builded with %d words" % len(self.vocab)

	def __add_doc(self, doc):
		bows = doc.split(' ')
		for i in xrange(len(bows) / 2):
			word = bows[i * 2]
			freq = int(bows[i * 2 + 1])
			if self.vocab.has_key(word):
				self.vocab[word][0] += freq
				self.vocab[word][1] += 1
			else:
				self.vocab[word] = [freq, 1]

	def trim(self, no_below, no_above, keep_n):
		no_above *= self.ndoc
		for k, v in self.vocab.items():
			if v[1] < no_below or v[1] > no_above:
				del self.vocab[k]
		self.vocab = dict(sorted(self.vocab.iteritems(), key=lambda entry: entry[1][1], reverse = True)[:keep_n])
		print "reserve %d words after trim" % len(self.vocab)
	
	def save(self, vocab_fd):
		id = 0
		for k, v in self.vocab.items():
			print >> vocab_fd, "%d\t%s\t%s" % (id, k, v[0])
			id += 1

class WordSet(object):
        def __init__(self, vocab_fn):
		with open(vocab_fn) as vocab_fd:
                	self.wordset = set([line.split('\t')[1] for line in vocab_fd])

        def filter_bows(self, line):
                bows = line.split(' ')
                filtered = []
                for i in xrange(len(bows) / 2):
                        word = bows[i * 2]
                        freq = bows[i * 2 + 1]
                        if word in self.wordset:
                                filtered.append(word)
                                filtered.append(freq)
                return " ".join(filtered)
