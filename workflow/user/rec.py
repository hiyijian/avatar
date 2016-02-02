#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, csv, uuid
from shutil import copyfile
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
        sys.path.insert(0, pfolder)
from prepare.get_train_data import MakeTrainingDict
reload(sys)
sys.setdefaultencoding('utf8')

from tools.recommend import recommend, merge_recommend, to_hbase
from tools.mysql.import_history import import_history
from tools.mysql.import_rlist import import_rlist
from tools.mysql.import_user_topic import import_user_topic
from user.infer import InferUser
from doc.infer import InferDoc
from doc.index import IndexDoc
from prepare.get_user_data import GetUser
import sframe as sf

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
                self.version = '%s/data/user/version' % root
                self.done = '%s/data/user/done' % root

	def requires(self):
		return [Rec(self.conf), InferUser(self.conf), GetUser(self.conf)]
	
	def output(self):	
		return {"rec": luigi.LocalTarget(self.merged_rec),
			"version": luigi.LocalTarget(self.version),
			"done": luigi.LocalTarget(self.done)}
	def run(self):
		merge_recommend(self.output()['rec'].fn, 
			self.input()[0].fn, 
			self.input()[2]['user'].fn,
			self.input()[1].fn)
		copyfile(self.input()[2]['version'].fn, self.output()['version'].fn)
		open(self.output()['done'].fn, 'w').close()

class Rec2HBase(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
		root = parser.get("basic", "root")
                self.bin = '%s/mr/%s' % (root, parser.get("external", "bin"))
		self.local_csv = '%s/data/temp/user.rec.csv' % root
		root = parser.get("external", "root")
		task_id = uuid.uuid4()
		self.hbase_input_path = '%s/user/workspace/%s' % (root, task_id)	

	def requires(self):
		return [MergeRec(self.conf)]

	def output(self):
		return None

	def run(self):
		hdfs = luigi.contrib.hdfs.hadoopcli_clients.create_hadoopcli_client()
		df = sf.load_sframe(self.input()[0]['rec'].fn)
		delete_cols = [col for col in df.column_names() if col != "history" and col != "id" and col != "rlist"]
		df.remove_columns(delete_cols)
		df.export_csv(self.local_csv, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)	
		hbase_input_csv = "%s/user.rec.csv" % self.hbase_input_path
		hdfs.mkdir(self.hbase_input_path)	
		hdfs.put(self.local_csv, hbase_input_csv)
		os.remove(self.local_csv)
		to_hbase(self.hbase_input_path, self.bin)
		hdfs.remove(self.hbase_input_path)

class Rec2Mysql(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
		root = parser.get("basic", "root")
		self.history_fn = "%s/data/temp/history.csv" % root
		self.rlist_fn = "%s/data/temp/user.rec.csv" % root
		self.fea_fn = "%s/data/temp/user.fea.csv" % root
		self.host = parser.get("mysql", "host")
		self.db = parser.get("mysql", "db")
		self.user = parser.get("mysql", "user")
		self.passwd = parser.get("mysql", "password")

	def requires(self):
		return [MergeRec(self.conf)]

	def output(self):
		return None

	def run(self):
		merged_rec_df = sf.load_sframe(self.input()[0]['rec'].fn)
		#import history
		print "import history"
		history_df = merged_rec_df.select_columns(['id', 'history'])
		history_df.export_csv(self.history_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)
		import_history(self.history_fn, self.host, self.db, self.user, self.passwd)		
		os.remove(self.history_fn)
		#import rlist
		print "import recommendation"
		rlist_df = merged_rec_df.select_columns(['id', 'rlist'])
		rlist_df.export_csv(self.rlist_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)
		import_rlist(self.rlist_fn, self.host, self.db, self.user, self.passwd)		
		os.remove(self.rlist_fn)
		#import user-topic-table
		print "import user-topic-table"
		fea_df = merged_rec_df.select_columns(['id', 'fea'])
		fea_df.export_csv(self.fea_fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)
		import_user_topic(self.fea_fn, self.host, self.db, self.user, self.passwd)		
		os.remove(self.fea_fn)

if __name__ == "__main__":
    luigi.run()
