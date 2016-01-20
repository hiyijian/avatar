#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, json, re, random
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
import sframe as sf
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from contrib.target import MRHdfsTarget

class ExternalPaper(luigi.ExternalTask):
	conf = luigi.Parameter()
	
	def __init__(self, *args, **kwargs):
		luigi.ExternalTask.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()  	
		parser.read(self.conf)
		self.paper_path = parser.get("basic", "paper_path")

	def output(self):
		return MRHdfsTarget(self.paper_path)

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
		return [ExternalPaper(self.conf)]

	def run(self):
		if os.path.exists(self.external):
			os.remove(self.external)
		with self.input()[0].open() as in_fd:
			with open(self.external, "w") as external_fd:
				for line in in_fd:
					external_fd.write(line)
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
