#!/usr/bin.python
# -*- coding: utf-8 -*-
import os, sys, inspect, csv, time
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
from tools.vocab import Vocab, WordSet
reload(sys)
sys.setdefaultencoding('utf8')

from get_paper_data import GetPaper
from ConfigParser import SafeConfigParser
import sframe as sf
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from contrib.target import MRHdfsTarget
	
class SampleTraining(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()  	
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.train_fraction = parser.getfloat("basic", "train_fraction")
		self.train_field = parser.get("basic", "train_field")
		self.sample_training = '%s/data/train/paper.sampled.sf' % root
		
	def output(self):
		return luigi.LocalTarget(self.sample_training)

	def requires(self):
		return [GetPaper(self.conf)]

	def run(self):
		df = sf.load_sframe(self.input()[0].fn)
		sampled_df = df.sample(self.train_fraction)
		print "sampled %d documents" % sampled_df.num_rows()
		delete_cols = [col for col in sampled_df.column_names() if col != self.train_field]
		sampled_df.remove_columns(delete_cols)
		sampled_df.save(self.output().fn)
	
class MakeTrainingDict(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.train_field = parser.get("basic", "train_field")
		self.keep_n = parser.getint("basic", "dict_keep_n")
		self.no_below = parser.getint("basic", "no_below")
		self.no_above = parser.getfloat("basic", "no_above")
		self.dict = '%s/data/train/paper.sampled.dict' % root

	def output(self):
		return luigi.LocalTarget(self.dict)
	
	def requires(self):
		return [SampleTraining(self.conf)]

	def run(self):
		df = sf.load_sframe(self.input()[0].fn)[self.train_field]
		with self.output().open('w') as out_fd:
			print "start to make vocab"
			vocab = Vocab(df)
			vocab.trim(self.no_below, self.no_above, self.keep_n)
	  		vocab.save(out_fd)
		
class Training2LDA(luigi.Task):
	conf = luigi.Parameter()

	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.train_field = parser.get("basic", "train_field")
		self.trim_training_plda = '%s/data/train/paper.sampled.plda' % root

	def output(self):
		return luigi.LocalTarget(self.trim_training_plda)

	def requires(self):
		return [SampleTraining(self.conf), MakeTrainingDict(self.conf)]

	def run(self):
		df = sf.load_sframe(self.input()[0].fn)
		wordset = WordSet(self.input()[1].fn)
		df[self.train_field] = df[self.train_field].apply(wordset.filter_bows)
		df = df[df[self.train_field] != ""]
		df.export_csv(self.output().fn, delimiter="\t", quote_level=csv.QUOTE_NONE, header=False)
				
if __name__ == "__main__":
    luigi.run()	
