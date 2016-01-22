#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, json, re, random
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from external import GetExternalPaper
from contrib.mr import get_mr_dir
from ConfigParser import SafeConfigParser
import sframe as sf
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs

class GetPaper(luigi.Task):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()  	
		parser.read(self.conf)
		root = parser.get("basic", "root")
		self.schema = [f.strip() for f in parser.get("basic", "paper_schema").split(',')]
		self.external = '%s/data/temp/paper.csv' % root
		self.paper = '%s/data/temp/paper.sf' % root

	def output(self):
		return luigi.LocalTarget(self.paper)
	
	def requires(self):	
		return [GetExternalPaper(self.conf)]

	def run(self):
		if os.path.exists(self.external):
			os.remove(self.external)
		with open(self.external, "w") as external_fd:
			get_mr_dir(self.input()[0].path, external_fd)		
		df = sf.SFrame.read_csv(self.external,
			column_type_hints=[str, str, str],
                        delimiter='\t', header=False)
		cols = {}
		for i in xrange(len(self.schema)):
			cols["X%d" % (i + 1)] = self.schema[i]
		df.rename(cols)
		df.save(self.output().fn)
		os.remove(self.external)
	

if __name__ == "__main__":
    luigi.run()
