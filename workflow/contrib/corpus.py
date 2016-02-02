import os, sys

class FeaCorpus(object):
	def __init__(self, fname, onlyID=False):
		self.fname = fname
		self.onlyID = onlyID
			
	def __iter__(self):
		id = ""
		feas_str = ""
		with open(self.fname, 'r') as in_fd:
			for line in in_fd:
				items = line.split('\t')
				if len(items) != 2:
					id = ""
					feas_str = line.strip()
				else:
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

class BatchFeaCorpus(object):
	def __init__(self, fname, batch_size):
		self.fea_corpus = FeaCorpus(fname)
		self.batch_size = batch_size

	def __iter__(self):
		batch = []
		idx = 0
		for fea in self.fea_corpus:
			batch.append(fea)
			if len(batch) >= self.batch_size:
				yield (idx, batch)
				idx += len(batch)
				batch = []
		if len(batch) > 0:
			yield (idx, batch)
