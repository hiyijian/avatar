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

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from contrib.target import MRHdfsTarget

class GetUnionPaper(luigi.ExternalTask):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.ExternalTask.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()  	
		parser.read(self.conf)
		self.paper_path = parser.get("basic", "paper_path")

	def output(self):
		return MRHdfsTarget(self.paper_path)

class PaperSegment(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()	
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.paper_segment = '%s/data/temp/paper.remark.seg' % root
		
	def output(self):
		return luigi.LocalTarget(self.paper_segment)


	def requires(self):	
		return [GetUnionPaper(self.conf)]

	def run(self):
		with self.output().open('w') as out_fd:
			with self.input()[0].open() as in_fd:
				format_join(in_fd, out_fd, True)

if __name__ == "__main__":
    luigi.run()