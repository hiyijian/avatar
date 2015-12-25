#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, json, re, random, time
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
        sys.path.insert(0, pfolder)
from gensim import corpora, models, similarities
from tools.formatter import *
from tools.make_dict import make_dict
from prepare.get_train_data import MakeTrainingDict
reload(sys)
sys.setdefaultencoding('utf8')

from tools.inferer import infer_topic
from tools.recommend import recommend
from user.infer import InferUser
from doc.infer import InferDoc
from doc.index import IndexDoc

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from contrib.target import MRHdfsTarget

class Rec(luigi.Task):
	conf = luigi.Parameter()
	
        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
                self.batch = parser.getint("rec", "batch")
                self.threshold = parser.getfloat("rec", "threshold")
                self.topk = parser.getint("rec", "topk")
                self.rec = '%s/data/user/user.rec' % root
	
	def requires(self):
		return [InferUser(self.conf), InferDoc(self.conf), IndexDoc(self.conf)]

	def output(self):
		return luigi.LocalTarget(self.rec)

	def run(self):
		with self.output().open('w') as out_fd:
			recommend(out_fd, self.input()[0].fn, self.input()[1].fn, self.input()[2].fn, self.topk, self.batch, self.threshold)

if __name__ == "__main__":
    luigi.run()
