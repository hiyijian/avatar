#!/usr/bin.python
# -*- coding: utf-8 -*-
import os, sys, inspect, json, re, random, time
from gensim import corpora, models, similarities
from contrib.corpus import FeaCorpus 
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.2f')

def readIDs(fn):
	ids = []
	with open(fn, 'r') as fd:
		for line in fd:
			id = line.split('\t')[0][2:]
			ids.append(id)
		return ids

def print_rec(out_fd, user_idx, rec, uids, docids, threshold):
	jrlist = []
	[jrlist.append({"id" : docids[t[0]], "s" : t[1]}) for t in rec if t[1] > threshold]
	if len(jrlist) > 0:
		line = uids[user_idx] + '\t' + json.dumps(jrlist)
		print >> out_fd, line

def recommend(out_fd, user_fn, doc_fn, index_fn, topk, batch_size, threshold):	
	uids = readIDs(user_fn)
	docids = readIDs(doc_fn)
	users = FeaCorpus(user_fn)
	index = similarities.docsim.Similarity.load(index_fn)
	index.num_best = topk
	batch = []
	user_idx = 0
	total_t = 0
	for user in users:
		batch.append(user)
		if len(batch) >= batch_size:
			s = time.time()
			for rec in index[batch]:
				print_rec(out_fd, user_idx, rec, uids, docids, threshold)
				user_idx += 1
			e = time.time()
			total_t += (e - s)
			batch = []
			print '\rrecommend %d[batch=%d], cost %fs/user' % (user_idx, batch_size, 1.0 * total_t / user_idx),
			sys.stdout.flush()
	if len(batch) > 0:
		s = time.time()
		for rec in index[batch]:
			print_rec(out_fd, user_idx, rec, uids, docids, threshold)
			user_idx += 1
		e = time.time()
		total_t += (e - s)
		print '\rrecommend %d[batch=%d], cost %fs/user' % (user_idx, batch_size, 1.0 * total_t / user_idx),
		sys.stdout.flush()
	print '\n'

