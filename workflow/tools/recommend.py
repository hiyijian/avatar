#!/usr/bin.python
# -*- coding: utf-8 -*-
import os, sys, inspect, json, re, random, time
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
        sys.path.insert(0, pfolder)
import sframe as sf
from shutil import copyfile
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
	docids = [docid.strip() for docid in open(docid_fn)]
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

def merge_recommend(merged_rec_fn, latest_rec_fn):
	if not os.path.exists(merged_rec_fn):
		copyfile(latest_rec_fn, merged_rec_fn)
		return
	merged_df = sf.SFrame.read_csv(merged_rec_fn, delimiter="\t", column_type_hints=[str, list], header=False)
	merged_df.rename({"X1": "id", "X2": "rlist"})
	latest_df = sf.SFrame.read_csv(latest_rec_fn, delimiter="\t", column_type_hints=[str, list], header=False)
	latest_df.rename({"X1": "id", "X2": "rlist"})
	latest_id = latest_df.select_column("id")
	merged_df = merged_df.filter_by(latest_id, 'id', exclude=True)
	merged_df = merged_df.append(latest_df)
	merged_df.export_csv(merged_rec_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)

def merge_history(merged_history_fn, latest_uesr_fn):
	latest_df = sf.load_sframe(latest_uesr_fn)
	delete_cols = [col for col in latest_df.column_names() if col != "history" and col != "id"]
	latest_df.remove_columns(delete_cols)
	if not os.path.exists(merged_history_fn):
		latest_df.export_csv(merged_history_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)	
		return
	merged_df = sf.SFrame.read_csv(merged_history_fn, delimiter="\t", column_type_hints=[str, list], header=False)
	merged_df.rename({"X1": "id", "X2": "hisory"})
	latest_id = latest_df.select_column("id")
	merged_df = merged_df.filter_by(latest_id, 'id', exclude=True)
	merged_df = merged_df.append(latest_df)
	merged_df.export_csv(merged_history_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)	
