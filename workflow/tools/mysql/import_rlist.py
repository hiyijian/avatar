# -*- coding: utf-8 -*-
import MySQLdb
import sys, argparse
reload(sys)
sys.setdefaultencoding('utf-8')

def trunc_db(conn):
	cur = conn.cursor()
	cur.execute('SET NAMES utf8')
	cur.execute("TRUNCATE TABLE Recommends")
	conn.commit()
	cur.close()

def batch_insert(conn, values):
	cur = conn.cursor()
	sql = """
	INSERT INTO Recommends(uid, recommend)
	VALUES (%s, %s)
	"""
	cur.executemany(sql, values)
	conn.commit()
	cur.close()
			
def import_rlist(infile, host, db, user, passwd):
			
	try:
		conn = MySQLdb.connect(db=db, host=host ,user=user, passwd=passwd, charset="utf8")
		trunc_db(conn)
		values = []
		batch_size = 100
		with open(infile, 'r') as in_fd:
			for line in in_fd:
				items = line.split('\t')
				uid = items[0]
				rlist = items[1]
				values.append((uid, rlist))
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
