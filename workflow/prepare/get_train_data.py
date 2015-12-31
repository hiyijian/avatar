#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, json, re, random
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
from gensim import corpora, models, similarities
from tools.formatter import *
from tools.make_dict import make_dict
reload(sys)
sys.setdefaultencoding('utf8')

from get_paper_data import GetUnionPaper,PaperSegment
from ConfigParser import SafeConfigParser
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
		self.samper = "%s/sample/sample" % root
		self.max_train_num = parser.getint("basic", "max_train_num")
		self.sample_training = '%s/data/train/paper.remark.seg' % root
		
	def output(self):
		return luigi.LocalTarget(self.sample_training)

	def requires(self):
		return [PaperSegment(self.conf)]

	def run(self):
		cmd = '%s -k %d %s > %s'
		cmd = cmd % (self.samper, self.max_train_num, self.input()[0].fn, self.output().fn)
		os.system(cmd)
	
class MakeTrainingDict(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.keep_n = parser.getint("basic", "dict_keep_n")
		self.dict = '%s/data/train/paper.remark.dict' % root

	def output(self):
		return luigi.LocalTarget(self.dict)
	
	def requires(self):
		return [SampleTraining(self.conf)]

	def run(self):	
		with self.output().open('w') as out_fd:
			with self.input()[0].open('r') as in_fd:
				make_dict(in_fd, out_fd, 20, 0.01, self.keep_n)
		
class Training2LDA(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.trim_training_plda = '%s/data/train/paper.remark.plda.trim' % root

	def output(self):
		return luigi.LocalTarget(self.trim_training_plda)

	def requires(self):
		sample_task = SampleTraining(self.conf)
		make_dict_task = MakeTrainingDict(self.conf)
		self.sample_training_target = sample_task.output()
		self.dict_target = make_dict_task.output()
		return [sample_task, make_dict_task]

	def run(self):
		with self.output().open('w') as out_fd:
			with self.sample_training_target.open('r') as sample_training_fd:
				format_plda(sample_training_fd, out_fd, False, self.dict_target.fn)

class GetTraining(luigi.WrapperTask):
	conf = luigi.Parameter()

	def requires(self):
		yield Training2LDA(self.conf)
				
if __name__ == "__main__":
    luigi.run()	
