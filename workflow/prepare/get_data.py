#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, json, re, random
from gensim import corpora, models, similarities
from get_train_data import GetTraining
from get_target_data import GetTarget
reload(sys)
sys.setdefaultencoding('utf8')

from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs

class GetData(luigi.WrapperTask):
	conf = luigi.Parameter()

	def requires(self):
		yield GetTraining(self.conf)
		yield GetTarget(self.conf)
				
if __name__ == "__main__":
    luigi.run()	
