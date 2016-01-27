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

def merge_topic(merged_topic_fn, latest_topic_fn):
	if not os.path.exists(merged_topic_fn):
		copyfile(latest_topic_fn, merged_topic_fn)
		return
	merged_df = sf.SFrame.read_csv(merged_topic_fn, delimiter="\t", column_type_hints=[str, str], header=False)
	merged_df.rename({"X1": "id", "X2": "fea"})
	latest_df = sf.SFrame.read_csv(latest_topic_fn, delimiter="\t", column_type_hints=[str, str], header=False)
	latest_df.rename({"X1": "id", "X2": "fea"})
	latest_id = latest_df.select_column("id")
	merged_df = merged_df.filter_by(latest_id, 'id', exclude=True)
	merged_df = merged_df.append(latest_df)
	merged_df.export_csv(merged_topic_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)	

def merge_history(merged_history_fn, latest_uesr_fn):
	latest_df = sf.load_sframe(latest_uesr_fn)
	delete_cols = [col for col in latest_df.column_names() if col != "history" and col != "id"]
	latest_df.remove_columns(delete_cols)
	if not os.path.exists(merged_history_fn):
		latest_df.export_csv(merged_history_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)	
		return
	merged_df = sf.SFrame.read_csv(merged_history_fn, delimiter="\t", column_type_hints=[str, list], header=False)
	merged_df.rename({"X1": "id", "X2": "history"})
	latest_id = latest_df.select_column("id")
	merged_df = merged_df.filter_by(latest_id, 'id', exclude=True)
	merged_df = merged_df.append(latest_df)
	merged_df.export_csv(merged_history_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)	
