import os, uuid, sys, inspect
pfolder = os.path.realpath(os.path.abspath (os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"..")))
if pfolder not in sys.path:
	sys.path.insert(0, pfolder)
from ConfigParser import SafeConfigParser
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from contrib.mr import check_mr_success, get_mr_dir

def infer_topic(in_fn, model_fn, out_fn, conf):
	parser = SafeConfigParser()
	parser.read(conf)
	root = parser.get("basic", "root")
	hadoop_stream = parser.get('basic', 'hadoop_stream')
	topic_num = parser.getint('plda+', 'topic_num')
	alpha = 50.0 / topic_num
	task_id = uuid.uuid4()
	infer_in_path = "%s/%s" % (parser.get('plda+', 'infer_in_path'), task_id)
	infer_out_path = "%s/%s" % (parser.get('plda+', 'infer_out_path'), task_id)
	infer_burn_in_iter = parser.getint('plda+', 'infer_burn_in_iter')
	infer_total_iter = parser.getint('plda+', 'infer_total_iter')
	infer_reduce_tasks = parser.getint('plda+', 'infer_reduce_tasks')
	infer_reducer_mb = parser.getint('plda+', 'infer_reducer_mb')
	mapper = '%s/plda/infer_mapper' % root
	reducer = '%s/plda/infer_reducer' % root
	reducer_wrapper = '%s/data/temp/reducer_wrapper.sh' % root

	hdfs = luigi.contrib.hdfs.hadoopcli_clients.create_hadoopcli_client()
	hdfs.mkdir(infer_in_path)
	hdfs.put(in_fn, infer_in_path)

	with open(reducer_wrapper, 'w') as wrapper_fd:
		print >> wrapper_fd, "#!/bin/bash"
		print >> wrapper_fd, "./infer_reducer --alpha %f --beta 0.01 --model_file ./%s --burn_in_iterations %d --total_iterations %d -sparse true" % \
			(alpha, os.path.basename(model_fn), infer_burn_in_iter, infer_total_iter)
	cmd = '''hadoop jar %s \
	      -D mapred.job.name="mr plda+ infer" \
	      -D mapred.job.map.memory.mb=32 \
	      -D mapred.job.reduce.memory.mb=%d \
	      -D io.compression.codecs=org.apache.hadoop.io.compress.DefaultCodec \
	      -input %s \
	      -output %s \
	      -file %s \
	      -file %s \
	      -file %s \
	      -file %s \
	      -mapper ./infer_mapper \
	      -reducer ./reducer_wrapper.sh \
	      -numReduceTasks %d
	      '''
	cmd = cmd % (hadoop_stream, infer_reducer_mb,
		infer_in_path, infer_out_path,
		model_fn, mapper, reducer, reducer_wrapper,
		infer_reduce_tasks)
	os.system(cmd)
	os.remove(reducer_wrapper)
	if check_mr_success(infer_out_path):
		with open(out_fn, 'w') as out_fd:
			get_mr_dir(infer_out_path, out_fd)
		hdfs.remove(infer_in_path)
		hdfs.remove(infer_out_path)
	else:
		hdfs.remove(infer_in_path)
		hdfs.remove(infer_out_path)
		raise Exception("failed to infer topic")
