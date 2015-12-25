#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, json, re, random
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
from gensim import corpora, models, similarities
from tools.formatter import *
from tools.make_dict import make_dict
from get_train_data import MakeTrainingDict
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from contrib.target import MRHdfsTarget

class TargetPaper(luigi.ExternalTask):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.ExternalTask.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()  	
		parser.read(self.conf)
		self.target_paper = parser.get("basic", "target_paper_path")

	def output(self):
		return MRHdfsTarget(self.target_paper)

class TargetSegment(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()	
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.target_segment = '%s/data/temp/paper.join.seg' % root
		
	def output(self):
		return luigi.LocalTarget(self.target_segment)


	def requires(self):	
		return [TargetPaper(self.conf)]

	def run(self):
		with self.output().open('w') as out_fd:
			for t in self.input():
				with t.open('r') as in_fd:
					format_join(in_fd, out_fd, True)
	
class Target2LDA(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.trim_target_plda = '%s/data/target/paper.join.plda.trim' % root

	def output(self):
		return luigi.LocalTarget(self.trim_target_plda)

	def requires(self):
		segment_task = TargetSegment(self.conf)
		make_dict_task = MakeTrainingDict(self.conf)
		self.segment_target = segment_task.output()
		self.dict_target = make_dict_task.output()
		return [segment_task, make_dict_task]

	def run(self):
		with self.output().open('w') as out_fd:
			with self.segment_target.open('r') as segment_fd:
				format_plda(segment_fd, out_fd, True, self.dict_target.fn)
					

class GetTarget(luigi.WrapperTask):
	conf = luigi.Parameter()

	def requires(self):
		yield Target2LDA(self.conf)
				
if __name__ == "__main__":
    luigi.run()	
