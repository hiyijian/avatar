#!/usr/bin/python2.4
# Print a readable text of the model
# ./view_model.py model_file viewable_file

def plda_model_view(model_fn, view_fn):
	num_topics = 0
	map = []
	sum = []
	word_sum = {}
	for line in open(model_fn):
	  sep = line.split("\t")
	  word = sep[0]
	  sep = sep[1].split()
	  if num_topics == 0:
	    num_topics = len(sep)
	    for i in range(num_topics):
	      map.append({})
	      sum.append(0.0)
	  for i in range(len(sep)):
	    if float(sep[i]) > 1:
	      map[i][word] = float(sep[i])
	      if word_sum.has_key(word):
 	       word_sum[word] += float(sep[i])
	      else:
	        word_sum[word] = float(sep[i])
	      sum[i] += float(sep[i])

	for i in range(len(map)):
	  for key in map[i].keys():
	    map[i][key] = map[i][key]
	with open(view_fn, 'w') as view_fd:
		for i in range(len(map)):
		  x = sorted(map[i].items(), key=lambda(k, v):(v, k), reverse = True)
		  print >> view_fd
		  print >> view_fd, "TOPIC: ", i, sum[i]
		  print >> view_fd
		  for key in x:
		    print >>view_fd, key[0], key[1]
