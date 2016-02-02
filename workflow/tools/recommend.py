#!/usr/bin.python
# -*- coding: utf-8 -*-
import os, sys, inspect, csv, json, time
import Queue, threading
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
        sys.path.insert(0, pfolder)
import sframe as sf
from shutil import copyfile
from gensim import corpora, models, similarities
from contrib.corpus import FeaCorpus, BatchFeaCorpus
from contrib.mr import check_mr_success, mr_cmd

class RecThread(threading.Thread):
	def __init__(self, queue, index, uids, docids, threshold, out_fd, lock):
		threading.Thread.__init__(self)
		self.queue = queue
		self.index = index
		self.uids = uids
		self.docids = docids
		self.threshold = threshold
		self.out_fd = out_fd
		self.lock = lock

	def run(self):
		while True:
			batch = self.queue.get()
			base_id = batch[0]
			s = time.time()
			batch_rec = self.index[batch[1]]
			self.lock.acquire()
			self.print_recs(base_id, batch_rec)
			e = time.time()
			t = (e - s)
			print 'recommend %d users, cost %.2fs/user' % (len(batch[1]), 1.0 * t / len(batch[1]))
			sys.stdout.flush()
			self.lock.release()
			self.queue.task_done()

	def print_recs(self, base_id, batch_rec):
		idx = 0
		for rec in batch_rec:
			jrlist = []
			[jrlist.append({"id" : self.docids[t[0]], "s" : round(t[1], 2)}) for t in rec if t[1] > self.threshold]
			if len(jrlist) > 0:
				line = self.uids[base_id + idx] + '\t' + json.dumps(jrlist)
				print >> self.out_fd, line
			idx += 1


def recommend(out_fd, user_fn, docid_fn, index_fn, topk, batch_size, threshold, thread_num):	
	uids = [uid.strip() for uid in FeaCorpus(user_fn, onlyID=True)]
	docids = [docid.strip() for docid in open(docid_fn)]
	user_batch = BatchFeaCorpus(user_fn, batch_size)
	index = similarities.docsim.Similarity.load(index_fn, mmap='r')
	index[[(0,1)]]
	index.num_best = topk
	queue = Queue.Queue()
	lock = threading.Lock()
	total_user = 0
	s = time.time()
	for batch in user_batch:
		queue.put(batch)
		total_user += len(batch[1])
	for i in range(thread_num):
		t = RecThread(queue, index, uids, docids, threshold, out_fd, lock)
		t.setDaemon(True)
		t.start()
	queue.join()
	e = time.time()
	t = (e - s)
	print 'recommend %d users, %.2fs/user' % (total_user, 1.0 * t / total_user)

def merge_recommend(merged_fn, latest_rec_fn, latest_user_fn, latest_topic_fn):
	#read recommend df
	latest_rec_df = sf.SFrame.read_csv(latest_rec_fn, delimiter="\t", column_type_hints=[str, list], header=False)
	latest_rec_df.rename({"X1": "id", "X2": "rlist"})
	#read history df
	latest_history_df = sf.load_sframe(latest_user_fn)
	delete_cols = [col for col in latest_history_df.column_names() if col != "history" and col != "id"]
	latest_history_df.remove_columns(delete_cols)
	#read topic df	
	latest_topic_df = sf.SFrame.read_csv(latest_topic_fn, delimiter="\t", column_type_hints=[str, str], header=False)
	latest_topic_df.rename({"X1": "id", "X2": "fea"})
	#join all
	latest_df = latest_history_df.join(latest_rec_df, on='id', how='left').join(latest_topic_df, on='id', how='left')

	if not os.path.exists(merged_fn):
		lasted_df.save(merged_fn)
	else:	
		merged_df = sf.load_sframe(merged_fn)
		latest_id = latest_df.select_column("id")
		merged_df = merged_df.filter_by(latest_id, 'id', exclude=True)
		merged_df = merged_df.append(latest_df)
		merged_df.save(merged_fn)


def to_hbase(data_path, jarbin): 
	mr_conf_fn = '%s/DataCreate.xml' % os.path.dirname(jarbin)
	with open(mr_conf_fn, 'w') as conf_fd:
		conf_str = '''<?xml version="1.0" encoding="UTF-8"?>
		<jobs>
		<hbase name="recommendation 2 hbase" hbasename="user_recommendation_info" regions="10" input="%s" method="create_hfile"/>
		</jobs>'''
		conf_str = conf_str % (data_path)
		print >> conf_fd, conf_str
	exit_code = mr_cmd(jarbin, "DataCreateBehavior")
	if exit_code != 0:
		raise Exception('failed to write recommendation to hbase')
