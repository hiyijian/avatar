#!/usr/bin.python
# -*- coding: utf-8 -*-
import os, sys, inspect, json, re, random, time
from gensim import corpora, models, similarities
from contrib.corpus import FeaCorpus 
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.2f')

def print_rec(out_fd, userid, rec, docids, threshold):
	jrlist = []
	[jrlist.append({"id" : docids[t[0]], "s" : t[1]}) for t in rec if t[1] > threshold]
	if len(jrlist) > 0:
		line = userid + '\t' + json.dumps(jrlist)
		print >> out_fd, line

def recommend(out_fd, user_fn, docid_fn, index_fn, topk, batch_size, threshold):	
	uids = [uid.strip() for uid in FeaCorpus(user_fn, onlyID=True)]
	docids = [docid.strip() for docid open(docid_fn)]
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
				print_rec(out_fd, uids[user_idx], rec, docids, threshold)
				user_idx += 1
			e = time.time()
			total_t += (e - s)
			batch = []
			print '\rrecommend %d[batch=%d], cost %fs/user' % (user_idx, batch_size, 1.0 * total_t / user_idx),
			sys.stdout.flush()
	if len(batch) > 0:
		s = time.time()
		for rec in index[batch]:
			print_rec(out_fd, uids[user_idx], rec, docids, threshold)
			user_idx += 1
		e = time.time()
		total_t += (e - s)
		print '\rrecommend %d[batch=%d], cost %fs/user' % (user_idx, batch_size, 1.0 * total_t / user_idx),
		sys.stdout.flush()
	print '\n'

