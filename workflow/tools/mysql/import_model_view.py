# -*- coding: utf-8 -*-
import re, json
import MySQLdb
import sys, argparse
reload(sys)
sys.setdefaultencoding('utf-8')

def trunc_db(conn):
        cur = conn.cursor()
        cur.execute('SET NAMES utf8')
        cur.execute("TRUNCATE TABLE ModelViews")
        conn.commit()
        cur.close()

def batch_insert(conn, values):
        cur = conn.cursor()
        sql = """
        INSERT INTO ModelViews(Topic, Rate, Context)
       	VALUES (%s, %s, %s)
        """
        cur.executemany(sql, values)
        conn.commit()
        cur.close()

class TopicCorpus(object):
	def __init__(self, fn):
		self.fn = fn
		self.topic_re = re.compile('TOPIC:  (\d+) (\d+\.\d+)')

	def __iter__(self):
		with open(self.fn) as in_fd:
			arr = []
			t = None
			rate = None
			for line in in_fd:
				line = line.strip()
				if len(line) == 0:
					continue
				match = self.topic_re.match(line)
				if match:
					if t is not None and rate is not None and len(arr) > 0:
						yield (t, rate, arr[:20])
					t = int(match.group(1))
					rate = float(match.group(2))
					arr = []
				else:
					feas = line.split(' ')
					word = feas[0]
					rate = float(feas[1])
					arr.append({"word": word, "rate": rate})
					
					 
def import_model_view(fn, host, db, user, passwd):
        try:
                conn = MySQLdb.connect(db=db, host=host ,user=user, passwd=passwd, charset="utf8")
                trunc_db(conn)
		tc = TopicCorpus(fn)	
                values = []
                batch_size = 100
		for t in tc:
			values.append((t[0], t[1], json.dumps(t[2])))
			if len(values) == batch_size:
				batch_insert(conn, values)
				values = []
                if len(values) > 0:
                        batch_insert(conn, values)
                conn.close()
        except Exception,e:
                print e
                return False
        return True
