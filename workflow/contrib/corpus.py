import os
class FeaCorpus(object):
	def __init__(self, fname):
		self.fname = fname
			
	def __iter__(self):
		with open(self.fname, 'r') as in_fd:
			for line in in_fd:
				items = line.split('\t')
				id = items[0]
				feas_str = items[1]
				feas = []
				for fea in feas_str.split(" "):
					elems = fea.split(":")
					feas.append((int(elems[0]), float(elems[1])))
				yield feas
	
