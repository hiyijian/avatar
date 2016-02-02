# -*- coding: utf-8 -*-
#/home/jianyi/project/avatar/data/user/user.join.topic
import MySQLdb
import sys, argparse
import json
reload(sys)
sys.setdefaultencoding('utf-8')

def trunc_db(conn):
	cur = conn.cursor()
	cur.execute('SET NAMES utf8')
	cur.execute("TRUNCATE TABLE DocTopics")
	conn.commit()
	cur.close()

def batch_insert(conn, values):
	cur = conn.cursor()
	sql = """
	INSERT INTO DocTopics(doc_id, Topics)
	VALUES (%s, %s)
	"""
	cur.executemany(sql, values)
	conn.commit()
	cur.close()

def resolve_siphon(topic_list=[]):
        max_top = 25
        main_feas = []
        other = 0
        otherp2 = 0
        feas = []
        for i in range(len(topic_list)):
                topics_k=topic_list[i].split(':')
                topic_serial=topics_k[0]
                topic_rate=topics_k[1]
                feas.append({'serial':topic_serial,'rate':float(topic_rate)})
        sorted_feas = sorted(feas,key = lambda x:x['rate'],reverse = True)
        if len(sorted_feas) > max_top:
                main_feas = sorted_feas[:max_top]
                for fea in sorted_feas[max_top:]:
                        other += fea['rate']
                        otherp2 += fea['rate'] * fea['rate']
        else:
                main_feas = sorted_feas
        return json.dumps({"c": len(sorted_feas), "main": main_feas, "other": other, "otherp2": otherp2})	

def import_doc_topic(infile, host, db, user, passwd):
			
	try:
		conn = MySQLdb.connect(db=db, host=host ,user=user, passwd=passwd, charset="utf8")
		trunc_db(conn)
		values = []
		batch_size = 300
		with open(infile, 'r') as in_fd:
			for line in in_fd:
				line=line.strip('\n')
				items = line.split('\t')
				docid = items[0]
				topic_text=items[1]
				topic_list=topic_text.split(" ")
				values.append((docid, resolve_siphon(topic_list)))
				if len(values) == batch_size:
					batch_insert(conn, values)
					values = []
		if len(values) > 0:
			batch_insert(conn, values)
		conn.close()
		values=[]
	except Exception,e:
		print e
		return False
	return True
	
