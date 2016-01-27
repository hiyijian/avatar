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

	def __init__(self, *args, **kwargs):
		luigi.WrapperTask.__init__(self, *args, **kwargs)
		tasks = set([])
		if "user" == self.changed:
			tasks = find_deps(Rec(self.conf), "GetExternalUser")
			self.remove_merge_tag()
			self.remove_tasks(tasks)
		elif "target" == self.changed:
			tasks = tasks.union(find_deps(GetPaper(self.conf), "GetExternalPaper"))
			tasks = tasks.union(find_deps(IndexDoc(self.conf), "Target2LDA"))
			self.remove_tasks(tasks)
		elif "model" == self.changed:
			tasks = find_deps(IndexDoc(self.conf), "SampleTraining")
			self.remove_tasks(tasks)
		elif "all" == self.changed:
			tasks = tasks.union(find_deps(MergeRec(self.conf), "GetExternalPaper"))
			tasks = tasks.union(find_deps(MergeRec(self.conf), "GetExternalUser"))
			self.remove_tasks(tasks)
		else:
			raise Exception('unrecognized option --changed %s' % self.changed)			

        def requires(self):
		if "user" == self.changed:
			yield MergeRec(self.conf)	
		elif "target" == self.changed:
			yield IndexDoc(self.conf)
		elif "model" == self.changed:
			yield IndexDoc(self.conf)
		elif "all" == self.changed:
			yield MergeRec(self.conf)
		else:
			raise Exception('unrecognized option --changed %s' % self.changed)		
	
	def remove_merge_tag(self):
		done_tag = MergeRec(self.conf).output()['done']
		if done_tag.exists():
			done_tag.remove()

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
