#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect, json, re, random
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

from tools.formatter import *
from get_train_data import MakeTrainingDict
from contrib.target import MRHdfsTarget

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

class TargetUser(luigi.ExternalTask):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.ExternalTask.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                self.target_user = parser.get("user", "target_user_path")

        def output(self):
                return MRHdfsTarget(self.target_user)

class UserSegment(luigi.Task):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
                self.target_segment = '%s/data/temp/user.join.seg' % root

        def output(self):
                return luigi.LocalTarget(self.target_segment)


        def requires(self):
                return [TargetUser(self.conf)]

        def run(self):
                with self.output().open('w') as out_fd:
                        for t in self.input():
                                with t.open('r') as in_fd:
                                        format_join(in_fd, out_fd, True)

class User2LDA(luigi.Task):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
                self.trim_target_plda = '%s/data/user/user.join.plda.trim' % root

        def output(self):
                return luigi.LocalTarget(self.trim_target_plda)

        def requires(self):
                segment_task = UserSegment(self.conf)
                make_dict_task = MakeTrainingDict(self.conf)
                self.segment_target = segment_task.output()
                self.dict_target = make_dict_task.output()
                return [segment_task, make_dict_task]

        def run(self):
                with self.output().open('w') as out_fd:
                        with self.segment_target.open('r') as segment_fd:
                                format_plda(segment_fd, out_fd, True, self.dict_target.fn)

class UserHistory(luigi.Task):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("basic", "root")
                self.history = '%s/data/user/user.history' % root

        def output(self):
                return luigi.LocalTarget(self.history)

        def requires(self):
                return [TargetUser(self.conf)]

        def run(self):
                with self.output().open('w') as out_fd:
                        for t in self.input():
                                with t.open('r') as in_fd:
					format_history(in_fd, out_fd, True)

class GetUser(luigi.WrapperTask):
        conf = luigi.Parameter()

        def requires(self):
                yield User2LDA(self.conf)
                yield UserHistory(self.conf)

if __name__ == "__main__":
    luigi.run()	
