#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, csv
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)

from tools.vocab import Vocab, WordSet
from get_paper_data import GetPaper
from get_train_data import MakeTrainingDict
import sframe as sf
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from contrib.target import MRHdfsTarget
	
class Target2LDA(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.target_field = parser.get("basic", "target_field")
		self.target_plda = '%s/data/target/paper.plda' % root

	def output(self):
		return luigi.LocalTarget(self.target_plda)

	def requires(self):
		return [GetPaper(self.conf), MakeTrainingDict(self.conf)]

	def run(self):
		df = sf.load_sframe(self.input()[0].fn)
		delete_cols = [col for col in df.column_names() if col != self.target_field and col != "id"]
		df.remove_columns(delete_cols)	
		wordset = WordSet(self.input()[1].fn)
		df[self.target_field] = df[self.target_field].apply(wordset.filter_bows)
		df = df[df[self.target_field] != ""]
		df.export_csv(self.output().fn, quote_level=csv.QUOTE_NONE, delimiter='\t', header=False)

if __name__ == "__main__":
    luigi.run()	
