# -*- coding: utf-8 -*- 
import os, re, json, sys
from gensim import corpora, models, similarities
reload(sys)
sys.setdefaultencoding('utf8')

def format_meta(in_fd, out_fd, fields, addID):
	for line in in_fd:
		line = line.strip()
		items = line.split("\t")        
		if len(items) == 2:
			id = items[0]   
			jsonstr = items[1]
			try:
				jmap = json.loads(jsonstr)
				ts = ""
				for field in fields:
					if jmap.has_key(field):
						fstr = jmap[field].strip()
						fstr = re.sub("\[\d+\]", "", fstr)
						if field == "keyword_c":
							fstr = re.sub(";", " ", fstr)
							ts += fstr + " "
						else:
							ts += fstr + " "
				ts = ts.strip()
				if len(ts) != 0:
					if addID:
						out_fd.write("_*%s\t%s\n" % (id, ts))
					else:
						out_fd.write("%s\n" % ts)
			except Exception, e:
				pass

def format_plda(in_fd, out_fd, hasID, dict_fname):	
	dictionary = None
	if dict_fname is not None:
		dictionary = corpora.dictionary.Dictionary.load(dict_fname)
	if dictionary is not None:
		for line in in_fd:
			id = None
			if hasID:
				items = line.strip().lower().split("\t")
				id = items[0]
				line = items[1]
			words = line.strip().lower().split()
			bows = dictionary.doc2bow(words)
			if len(bows) > 0:
				if id is not None:
					out_fd.write("%s\t" % id)
				for bow in bows:
					word = dictionary.get(bow[0])
					out_fd.write("%s %d " % (word, bow[1]))
				out_fd.write("\n")
	else:
		for line in in_fd:
			id = None
			if hasID:
				items = line.strip().lower().split("\t")
				id = items[0]
				line = items[1]
                        words = line.strip().lower().split()
			if len(words) > 0:
				if id is not None:
					out_fd.write("%s\t" % id)
				dict = {}
				for word in words:
					if dict.has_key(word):
						dict[word] += 1
					else:
						dict[word] = 1
				for (word, tf) in dict.items():
					out_fd.write("%s %d " % (word, tf))
				out_fd.write("\n")

def format_history(in_fd, out_fd, addID):
	for line in in_fd:
		items = line.strip().split("\t")	
		if len(items) == 2:
			jhistory = []
			id = items[0]
			jsonstr = items[1]
			try:
				jarr = json.loads(jsonstr)
				ts = ""
				for jmap in jarr: 
					if jmap.has_key("id") and jmap.has_key('t'):
						docid = jmap["id"].strip()
						t = long(jmap["t"].strip())
						jhistory.append({'id': docid, 't': t})
				if len(jhistory) != 0:
					if addID:
						out_fd.write("%s\t%s\n" % (id, json.dumps(jhistory)))
					else:
						out_fd.write("%s\n" % ts)
			except Exception, e:
				pass
	


def format_join(in_fd, out_fd, addID):
	for line in in_fd:
		items = line.strip().split("\t")	
		if len(items) == 2:
			id = items[0]	
			jsonstr = items[1]
			try:
				jarr = json.loads(jsonstr)
				if isinstance(jarr, dict):
					jarr = [jarr]
				ts = ""
				for jmap in jarr: 
					if jmap.has_key("self"):
						title_c = jmap["self"].strip()
						ts += title_c + " "
					if jmap.has_key("ref"):
						for ref_title_c in jmap["ref"]:	
							ref_title_c = ref_title_c.strip()
							ts += ref_title_c + " "
					if jmap.has_key("by"):
						for by_title_c in jmap["by"]:	
							by_title_c = by_title_c.strip()
							ts += by_title_c + " "
				ts = ts.strip()
				if len(ts) != 0:
					if addID:
						out_fd.write("_*%s\t%s\n" % (id, ts))
					else:
						out_fd.write("%s\n" % ts)
			except Exception, e:
				pass
