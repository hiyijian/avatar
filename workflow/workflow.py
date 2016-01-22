#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from luigi import six
from luigi.tools.deps import find_deps

from user.rec import Rec, MergeRec
from doc.index import IndexDoc
from prepare.get_paper_data import GetPaper
from prepare.get_target_data import Target2LDA


class ReRun(luigi.WrapperTask):
        conf = luigi.Parameter()
	changed = luigi.Parameter()

        def requires(self):
		tasks = set([])
		if "user" == self.changed:
			tasks = find_deps(Rec(self.conf), "GetExternalUser")
			self.remove_merge_version()
			self.remove_tasks(tasks)
			yield MergeRec(self.conf)	
		elif "target" == self.changed:
			tasks = tasks.union(find_deps(GetPaper(self.conf), "GetExternalPaper"))
			tasks = tasks.union(find_deps(IndexDoc(self.conf), "Target2LDA"))
			self.remove_tasks(tasks)
			yield IndexDoc(self.conf)
		elif "model" == self.changed:
			tasks = find_deps(IndexDoc(self.conf), "SampleTraining")
			self.remove_tasks(tasks)
			yield IndexDoc(self.conf)
		elif "all" == self.changed:
			tasks = tasks.union(find_deps(MergeRec(self.conf), "GetExternalPaper"))
			tasks = tasks.union(find_deps(MergeRec(self.conf), "GetExternalUser"))
			self.remove_tasks(tasks)
			yield MergeRec(self.conf)
		else:
			raise Exception('unrecognized option --changed %s' % self.changed)		
	
	def remove_merge_version(self):
		version_target = MergeRec(self.conf).output()['version']
		if version_target.exists():
			version_target.remove()

	def remove_tasks(self, tasks):
		for task in tasks:
			targets = task.output()
			if isinstance(targets, dict):
				targets = targets.values()
			else:
				targets = [targets]
			for target in targets:
				if target.exists():
					target.remove()	

if __name__ == "__main__":
    luigi.run()
