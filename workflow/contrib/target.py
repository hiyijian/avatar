import os
from luigi import six
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import luigi.contrib.ssh

class MRHdfsTarget(luigi.contrib.hdfs.HdfsTarget):
        def exists(self):
                path = self.path
                if '*' in path or '?' in path or '[' in path or '{' in path:
                        return self.fs.exists('%s/_SUCCESS' % os.path.dirname(path))
                else:
                        return self.fs.exists(path)
