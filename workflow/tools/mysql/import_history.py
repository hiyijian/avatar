# -*- coding: utf-8 -*-
import MySQLdb
import sys, argparse
reload(sys)
sys.setdefaultencoding('utf-8')

def trunc_db(conn):
	cur = conn.cursor()
	cur.execute('SET NAMES utf8')
	cur.execute("TRUNCATE TABLE Histories")
	conn.commit()
	cur.close()

def batch_insert(conn, values):
	cur = conn.cursor()
	sql = """
	INSERT INTO Histories(uid, history)
	VALUES (%s, %s)
	"""
	cur.executemany(sql, values)
	conn.commit()
	cur.close()
			
def import_history(infile, host, db, user, passwd):
			
	try:
		conn = MySQLdb.connect(db=db, host=host ,user=user, passwd=passwd, charset="utf8")
		trunc_db(conn)
		values = []
		batch_size = 1000
		with open(infile, 'r') as in_fd:
			for line in in_fd:
				items = line.split('\t')
				uid = items[0]
				history = items[1]
				values.append((uid, history))
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
