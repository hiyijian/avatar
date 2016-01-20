import os
class FeaCorpus(object):
	def __init__(self, fname, onlyID=False):
		self.fname = fname
		self.onlyID = onlyID
			
	def __iter__(self):
		with open(self.fname, 'r') as in_fd:
			for line in in_fd:
				items = line.split('\t')
				id = items[0]
				feas_str = items[1].strip()
				if 0 == len(feas_str):
					continue
				if self.onlyID:
					yield id
				else:
					feas = []
					for fea in feas_str.split(" "):
						elems = fea.split(":")
						feas.append((int(elems[0]), float(elems[1])))
					yield feas
