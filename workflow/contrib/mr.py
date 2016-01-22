import os
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import commands

def cmd(cmdstr):
	exit_code = commands.getstatusoutput(cmdstr)[0]
	return exit_code

def check_mr_success(hdfs_dir):
        hdfs = luigi.contrib.hdfs.hadoopcli_clients.create_hadoopcli_client()
        return hdfs.exists('%s/_SUCCESS' % hdfs_dir)

def mr_cmd(javabin, jobname):
	javapath = os.path.dirname(javabin)
	jarbin = os.path.basename(javabin)
        cmd_str = 'cd %s && rm -rf status && java -jar %s -jobStreamName=%s -dataAnalysisName=%s && cd -'
        cmd_str = cmd_str % (javapath, jarbin, jobname, jobname)
        exit_code = cmd(cmd_str)
        return exit_code

def get_mr_dir(mr_dir, local_fd):
	if not check_mr_success(mr_dir):
		raise Exception('mr dir[%s] has no SUCESS flag' % mr_dir)
	src = luigi.contrib.hdfs.HdfsTarget('%s/part-*' % mr_dir)		
	with src.open() as in_fd:
		for line in in_fd:
			local_fd.write(line)
