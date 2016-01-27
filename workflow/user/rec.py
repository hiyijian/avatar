#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect
from shutil import copyfile
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
        sys.path.insert(0, pfolder)
from prepare.get_train_data import MakeTrainingDict
reload(sys)
sys.setdefaultencoding('utf8')

from tools.recommend import recommend, merge_recommend, merge_topic, merge_history
from user.infer import InferUser
from doc.index import IndexDoc
from prepare.get_user_data import GetUser

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs

class Rec(luigi.Task):
	conf = luigi.Parameter()
	
        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
                self.batch = parser.getint("rec", "batch")
                self.threshold = parser.getfloat("rec", "threshold")
                self.thread_num = parser.getint("rec", "cpu_core_num")
                self.topk = parser.getint("rec", "topk")
                self.rec = '%s/data/user/user.rec' % root
	
	def requires(self):
		return [InferUser(self.conf), IndexDoc(self.conf)]

	def output(self):
		return luigi.LocalTarget(self.rec)

	def run(self):
		with self.output().open('w') as out_fd:
			recommend(out_fd, self.input()[0].fn, 
				self.input()[1]['ids'].fn, self.input()[1]['index'].fn, 
				self.topk, self.batch, self.threshold, self.thread_num)

class MergeRec(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
                self.merged_rec = '%s/data/user/user.rec.merged' % root
		self.merged_topic = '%s/data/user/user.topic.merged' % root
		self.merged_history = '%s/data/user/user.history.merged' % root
                self.version = '%s/data/user/version' % root
                self.done = '%s/data/user/done' % root

	def requires(self):
		return [Rec(self.conf), InferUser(self.conf), GetUser(self.conf)]
	
	def output(self):	
		return {"rec": luigi.LocalTarget(self.merged_rec),
			"topic": luigi.LocalTarget(self.merged_topic),
			"history": luigi.LocalTarget(self.merged_history),
			"version": luigi.LocalTarget(self.version),
			"done": luigi.LocalTarget(self.done)}
	def run(self):
		merge_recommend(self.output()['rec'].fn, self.input()[0].fn)
		merge_topic(self.output()['topic'].fn, self.input()[1].fn)
		merge_history(self.output()['history'].fn, self.input()[2]['user'].fn)
		copyfile(self.input()[2]['version'].fn, self.output()['version'].fn)
		open(self.output()['done'].fn, 'w').close()

if __name__ == "__main__":
    luigi.run()
