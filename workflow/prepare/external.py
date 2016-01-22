#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect
from shutil import copyfile
from ConfigParser import SafeConfigParser
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from luigi import six
import luigi.contrib.hdfs
from contrib.mr import check_mr_success, mr_cmd
from contrib.target import MRHdfsTarget

class ExternalPaperMeta(luigi.ExternalTask):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.ExternalTask.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                self.paper_meta = parser.get("external", "paper_meta")

        def output(self):
                return MRHdfsTarget(self.paper_meta)

class ExternalUserArchive(luigi.ExternalTask):
        conf = luigi.Parameter()

        def __init__(self, *args, **kwargs):
                luigi.ExternalTask.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
		root = parser.get("external", "root")
                self.user_archive = '%s/user/archive' % root

        def output(self):
                return luigi.contrib.hdfs.HdfsTarget(self.user_archive)

class GetExternalPaper(luigi.Task):
        conf = luigi.Parameter()

	def __init__(self, *args, **kwargs):	
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("external", "root")
		self.paper_root = '%s/doc' % root
		self.paper = '%s/final' % self.paper_root
                root = parser.get("basic", "root")
                self.bin = '%s/mr/%s' % (root, parser.get("external", "bin"))

        def requires(self):
		return [ExternalPaperMeta(self.conf)]

	def output(self):
		return luigi.contrib.hdfs.HdfsTarget(self.paper)

	def run(self):
		hdfs = luigi.contrib.hdfs.hadoopcli_clients.create_hadoopcli_client()
		hdfs.remove(self.paper_root)	
		hdfs.mkdir(self.paper_root)
		mr_path = os.path.dirname(self.bin)
		copyfile('%s/mapreduce.properties.doc' % mr_path,
			'%s/mapreduce.properties' % mr_path)
		exit_code = mr_cmd(self.bin, 'pr.paper')
		if exit_code != 0 or not check_mr_success(self.output().path):
			raise Exception('GetExternalPaper failed')	

class GetExternalUser(luigi.Task):
        conf = luigi.Parameter()

	def __init__(self, *args, **kwargs):	
                luigi.Task.__init__(self, *args, **kwargs)
                parser = SafeConfigParser()
                parser.read(self.conf)
                root = parser.get("external", "root")
		self.user_root = '%s/user/workspace' % root
		self.user = '%s/final' % self.user_root
		self.merge = '%s/merge' % self.user_root
		self.version = '%s/version' % self.user_root
		self.increamental_archive = '%s/archive.inc' % self.user_root
                root = parser.get("basic", "root")
                self.bin = '%s/mr/%s' % (root, parser.get("external", "bin"))
		self.local_version = '%s/data/user/version' % root
        
	def requires(self):
		return [ExternalUserArchive(self.conf), ExternalPaperMeta(self.conf)]
	
	def output(self):
		return {"user": luigi.contrib.hdfs.HdfsTarget(self.user),
			"version": luigi.contrib.hdfs.HdfsTarget(self.version)}

	def run(self):
		hdfs = luigi.contrib.hdfs.hadoopcli_clients.create_hadoopcli_client()
		dates = self.version_date()
		if len(dates) == 0:
			raise Exception('These\'s no user data in[%s]' % self.input()[0].fn)
		
		exit_code = -1
		mr_path = os.path.dirname(self.bin)
		copyfile('%s/mapreduce.properties.user' % mr_path,
			'%s/mapreduce.properties' % mr_path)
		if not os.path.exists(self.local_version):
			hdfs.remove(self.user_root)
			hdfs.mkdir(self.user_root)
			exit_code = mr_cmd(self.bin, 'pr.user')
			if exit_code != 0 or not check_mr_success(self.merge):
				raise Exception('GetExternalUser failed')
			hdfs.rename(self.merge, self.output()['user'].path)
		else:
			local_dates = [line.strip() for line in open(self.local_version)]
			latest_dates = set(dates) - set(local_dates)
			if len(latest_dates) == 0:
				raise Exception('These\'s no new arrival user data')	
			hdfs.remove(self.user_root)
			hdfs.mkdir(self.user_root)
			hdfs.mkdir(self.increamental_archive)
			for d in latest_dates:
				hdfs.copy('%s/%s' % (self.input()[0].fn, d),
					'%s/%s' % (self.increamental_archive, d))
			exit_code = mr_cmd(self.bin, 'pr.user.latest')
			if exit_code != 0 or not check_mr_success(self.output()['user'].path):
				raise Exception('GetExternalUser failed')
		#make version tag
		with self.output()['version'].open('w') as version_fd:
			dates.sort()
			for d in dates:
				print >> version_fd, d

	def version_date(self):
		hdfs = luigi.contrib.hdfs.hadoopcli_clients.create_hadoopcli_client()
		archive_dir = hdfs.listdir(self.input()[0].path)	
		return [os.path.basename(archive_fn).strip() for archive_fn in archive_dir]

if __name__ == "__main__":
    luigi.run()
