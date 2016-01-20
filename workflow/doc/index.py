#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, shutil
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import luigi.contrib.ssh

import socket
from doc.infer import InferDoc
from contrib.target import MRHdfsTarget
from contrib.corpus import FeaCorpus
from gensim import corpora, models, similarities
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class IndexDoc(luigi.Task):
	conf = luigi.Parameter()

	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
		self.topic_num = parser.getint('plda+', 'topic_num')
		self.shard_size = parser.getint('index', 'shard_size')
                self.index_prefix = '%s/data/target/index/index' % root
                self.index = '%s/data/target/paper.topic.index' % root
                self.ids = '%s/data/target/paper.id' % root
	
	def requires(self):
		return [InferDoc(self.conf)]
	
	def output(self):
		return {"index" : luigi.LocalTarget(self.index),
			"ids" : luigi.LocalTarget(self.ids)}

	def run(self):
		#get ids
		with self.output()['ids'].open('w') as ids_fd:
			corpus = FeaCorpus(self.input()[0].fn, onlyID=True)
			for id in corpus:
				print >> ids_fd, id
		#index features
		corpus = FeaCorpus(self.input()[0].fn)
		index_dir = os.path.dirname(self.index_prefix)
		if os.path.exists(index_dir):
			shutil.rmtree(index_dir)
		os.mkdir(index_dir)
		index = similarities.docsim.Similarity(self.index_prefix, corpus, num_features=self.topic_num, shardsize=self.shard_size)
		index.save(self.output()['index'].fn)
		
		
if __name__ == "__main__":
    luigi.run()
