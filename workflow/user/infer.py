#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, json, re, random
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
from prepare.get_train_data import MakeTrainingDict
reload(sys)
sys.setdefaultencoding('utf8')

from tools.inferer import infer_topic
from doc.infer import PLDA
from prepare.get_user_data import User2LDA

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs


class InferUser(luigi.Task):
	conf = luigi.Parameter()
		
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		root = parser.get("basic", "root")	
		self.infer_topic = '%s/data/user/user.topic' % root

	def requires(self):
		plda_target_task = User2LDA(self.conf) 
		plda_model_task = PLDA(self.conf)
		self.plda_target = plda_target_task.output()
		self.plda_model_target = plda_model_task.output()
		return [plda_target_task, plda_model_task]
        
	def output(self):
                return luigi.LocalTarget(self.infer_topic)

	def run(self):
		infer_topic(self.plda_target.fn, self.plda_model_target.fn, self.output().fn, self.conf)
				
if __name__ == "__main__":
    luigi.run()	
