#!/usr/bin.python
# -*- coding: utf-8 -*-

import os, sys, inspect
reload(sys)
sys.setdefaultencoding('utf8')

import luigi
from luigi import six
from luigi.tools.deps import find_deps

from user.rec import Rec
from doc.index import IndexDoc


class ReRun(luigi.WrapperTask):
        conf = luigi.Parameter()
	changed = luigi.Parameter()

        def requires(self):
		tasks = set([])
		temp = set([])
		tasks_0=set([])
		tasks_1=set([])
		tasks_0 = find_deps(Rec(self.conf), "PaperSegment")
		tasks_1 = find_deps(Rec(self.conf), "SampleTraining")
		task_PaperSegment=tasks_0-tasks_1
		del tasks_0
		del tasks_1
		if "user" == self.changed:
			tasks = find_deps(Rec(self.conf), "UserSegment")
		elif "target" == self.changed:
			tasks = find_deps(Rec(self.conf), "Target2LDA")
			tasks = tasks.union(task_PaperSegment)
			del temp
		elif "model" == self.changed:
			tasks = find_deps(Rec(self.conf), "SampleTraining")
		elif "all" == self.changed:
			tasks = tasks.union(find_deps(Rec(self.conf), "UserSegment"))
			tasks = tasks.union(find_deps(Rec(self.conf), "PaperSegment"))
		
		self.remove_tasks(tasks)
		yield Rec(self.conf)

	def remove_tasks(self, tasks):
		for task in tasks:
			target = task.output()
			if isinstance(target, luigi.LocalTarget):
				if target.exists():
					target.remove()
			else:
				print "ignore to remove none-LocalTarget[%s]" % (target.fn)

if __name__ == "__main__":
    luigi.run()
