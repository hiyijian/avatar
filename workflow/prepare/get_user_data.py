#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, json, csv
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import sframe as sf
from external import GetExternalUser
from contrib.mr import get_mr_dir
from tools.vocab import Vocab, WordSet
from get_train_data import MakeTrainingDict

from ConfigParser import SafeConfigParser
from suds.client import Client as suds
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs

class UserArchive(luigi.Task):
	conf = luigi.Parameter()
	date = luigi.Parameter()
		
	def __init__(self, *args, **kwargs):
		luigi.Task.__init__(self, *args, **kwargs)
		parser = SafeConfigParser()
		parser.read(self.conf)
		self.wsdl = parser.get("user", "wsdl")
		self.page = parser.getint("user", "page")
		self.archive_dir = parser.get("user", "archive_dir")
	
	def output(self):
		out_dir = "%s/%s" % (self.archive_dir, self.date)
		return luigi.contrib.hdfs.HdfsTarget(out_dir)
	
	def requires(self):
		return []

	def run(self):
		condition = 'refer_url regexp "^.*cqvip\.com.*$" and not vip_is_spider_strict(user_browser) and catalog=1 and user_session_id!="" and article_id!="" and year=%s and month=%s and day=%s'
		condition = condition % tuple(self.date.split("-"))
		logsql = 'select user_session_id, article_id, unix_timestamp(visit_time) from view_down_infos where %s'
		logsql = logsql % (condition)
		countsql = 'select count(*) from view_down_infos where %s'
		countsql = countsql % (condition)
		ws = suds(self.wsdl)
		count = self.get_results(ws, countsql)
		count = int(count[0]["count(*)"])
		page_num = count / self.page if count % self.page == 0 else count / self.page + 1
		with self.output().open('w') as out_fd:
			self.report_progress(0, page_num)
			for i in range(page_num):
				logs = self.get_results(ws, "%s order by visit_time DESC limit %d offset %d" % (logsql, self.page, i * self.page))
				for log in logs:
					print >> out_fd, "%s\t%s\t%s" % (log["user_session_id"], log["article_id"], log["unix_timestamp(visit_time)"])
				self.report_progress(i + 1, page_num)
		
	def get_results(self, ws, sql):
		res = ws.service.query(sql)
		res = json.loads(res)
		if res["exception"]:
			raise RuntimeError("the runtime error raised")  
		else:
			return res["results"]
	
	def report_progress(self, i, page_num):
		bar = "\rprogress: %.2f%%(%d/%d) |%s%s|"
		bar = bar % (100.0 * i / page_num, i, page_num, "#" * i, " " * (page_num - i))
		print bar,
		sys.stdout.flush()

class GetUser(luigi.Task):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
		self.version = '%s/data/temp/user.version' % root
		self.schema = [f.strip() for f in parser.get("basic", "user_schema").split(',')]
		self.external_user = '%s/data/temp/user.csv' % root
                self.user = '%s/data/temp/user.sf' % root

        def output(self):
                return {"user": luigi.LocalTarget(self.user),
			"version": luigi.LocalTarget(self.version)}

        def requires(self):
                return [GetExternalUser(self.conf)]

        def run(self):
		#copy user data
		if os.path.exists(self.external_user):
                        os.remove(self.external_user)
		with open(self.external_user, "w") as ext_user:
			get_mr_dir(self.input()[0]['user'].path, ext_user)	
		#copy user data version
		hdfs = luigi.contrib.hdfs.hadoopcli_clients.create_hadoopcli_client()
		hdfs.get(self.input()[0]['version'].path, self.output()['version'].fn)
		#transform to sframe ans save
                df = sf.SFrame.read_csv(self.external_user, delimiter="\t", column_type_hints=[str, str, list], header=False)
		cols = {}
		for i in xrange(len(self.schema)):
			cols["X%d" % (i + 1)] = self.schema[i]
		df.rename(cols)
                df.save(self.output()['user'].fn)
                os.remove(self.external_user)

class User2LDA(luigi.Task):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
                self.user_field = parser.get("basic", "user_field")	
                self.user_plda = '%s/data/user/user.plda' % root

        def output(self):
                return luigi.LocalTarget(self.user_plda)

        def requires(self):
                user_task = GetUser(self.conf)
                make_dict_task = MakeTrainingDict(self.conf)
                self.user_target = user_task.output()
                self.dict_target = make_dict_task.output()
                return [GetUser(self.conf), MakeTrainingDict(self.conf)]

        def run(self):
		df = sf.load_sframe(self.input()[0]['user'].fn)
		delete_cols = [col for col in df.column_names() if col != self.user_field and col != "id"]
		df.remove_columns(delete_cols)
		wordset = WordSet(self.input()[1].fn)
		df[self.user_field] = df[self.user_field].apply(wordset.filter_bows)
		df = df[df[self.user_field] != ""]
		df.export_csv(self.output().fn, quote_level=csv.QUOTE_NONE, delimiter="\t", header=False)

if __name__ == "__main__":
    luigi.run()	
