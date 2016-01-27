import numpy as np
import os, sys
from sklearn.preprocessing import normalize
from scipy.sparse import csr_matrix, csc_matrix

def load_dense_matrix(fn, ncol):
	lineno = 0
	with open(fn, 'r') as in_fd:
		for line in in_fd:
			lineno += 1
	ids = []
	m = np.zeros((lineno, ncol))
	nrow = 0
	with open(fn, 'r') as in_fd:
		for line in in_fd:
			items = line.split('\t')
			id = items[0]
			vstr = items[1]
			elements = vstr.split(' ')
			for ele in elements:
				items = ele.split(':')
				idx = int(items[0])
				wei = float(items[1])
				m[nrow][idx] = wei
			ids.append(id)
			nrow += 1
			if 0 == nrow % 5000:
				print "\rload matrix[%s]: %d" % (os.path.basename(fn), nrow),
				sys.stdout.flush()
		print "\rload matrix[%s]: %d" % (os.path.basename(fn), nrow),
		print "\n"
		return (ids, normalize(m, axis=1, norm='l1', copy=False))

def load_csr_matrix(fn, ncol):
	ids = []
	data = []
	indices = []
	indptr = [0]
	nrow = 0
	with open(fn, 'r') as in_fd:
		for line in in_fd:
			items = line.split('\t')
			id = items[0]
			topics = items[1]
			topics = topics.split(" ")
			sum = 0.0
			for topic in topics:
				items = topic.split(":")
				wei = float(items[1])
				sum += wei
			for topic in topics:
				items = topic.split(":")
				idx = int(items[0])
				wei = float(items[1])
				data.append(wei / sum)
				indices.append(idx)
			indptr.append(indptr[-1] + len(topics))
			ids.append(id)
			nrow += 1
			if 0 == nrow % 5000:
				print "\rload matrix[%s]: %d" % (os.path.basename(fn), nrow),
				sys.stdout.flush()
		print "\rload matrix[%s]: %d" % (os.path.basename(fn), nrow),
		print "\n"
		m = csr_matrix((np.array(data), np.array(indices), np.array(indptr)), shape=(nrow, ncol), dtype=np.float32) 
		return (ids, m)


def load_csc_matrix(fn, ncol):
	ids = []
	data = []
	row_ind = []
	col_ind = []
	nrow = 0
	with open(fn, 'r') as in_fd:
		for line in in_fd:
			items = line.split('\t')
			id = items[0]
			topics = items[1]
			topics = topics.split(" ")
			sum = 0.0
			for topic in topics:
				items = topic.split(":")
				wei = float(items[1])
				sum += wei
			for topic in topics:
				items = topic.split(":")
				idx = int(items[0])
				wei = float(items[1])
				data.append(wei / sum)
				row_ind.append(nrow)
				col_ind.append(idx)
			ids.append(id)
			nrow += 1
			if 0 == nrow % 5000:
				print "\rload matrix[%s]: %d" % (os.path.basename(fn), nrow),
				sys.stdout.flush()
		print "\rload matrix[%s]: %d" % (os.path.basename(fn), nrow),
		print "\n"
		m = csc_matrix((data, (row_ind, col_ind)), shape=(nrow, ncol), dtype=np.float32) 
		return (ids, m)

