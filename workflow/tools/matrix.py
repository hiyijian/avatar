import os, sys, inspect
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
        sys.path.insert(0, pfolder)
import numpy as np
from sklearn.preprocessing import normalize
from scipy.sparse import csr_matrix, csc_matrix

def load_dense_matrix(fea_corpus, nrow, ncol, norm=None):
	m = np.zeros((nrow, ncol))
	row = 0
	for fea in fea_corpus:
		for item in fea:
			idx = item[0]
			wei = item[1]
			m[row][idx] = wei
		row += 1
	if norm is None:
		return m
	return normalize(m, axis=1, norm=norm, copy=False)

def load_csr_matrix(fea_corpus, ncol, norm=None):
	data = []
	indices = []
	indptr = [0]
	nrow = 0
	for fea in fea_corpus:
		nfea = 0
		for item in fea:
			idx = item[0]
			wei = item[1]
			data.append(wei)
			indices.append(idx)
			nfea += 1
		indptr.append(indptr[-1] + nfea)
		nrow += 1
	m = csr_matrix((np.array(data), np.array(indices), np.array(indptr)), shape=(nrow, ncol), dtype=np.float32) 
	if norm is None: 
		return m
	m = normalize(m.astype(np.double), axis=1, norm='l2', copy=False)
	return m.astype(np.float)
