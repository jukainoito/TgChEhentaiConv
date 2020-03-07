# -*- coding: utf-8 -*-
import logging
import functools
import time

import sqlite3
import json

from .db import HentaiDatabase


logger = logging.getLogger(__name__)

def dict_factory(cursor, row):
	d = {}
	for idx, col in enumerate(cursor.description):
		d[col[0]] = row[idx]
	return d

def convert(data):
	if 'update_id' in data.keys(): 
		data['update_id'] = int(data['update_id'])
	if 'message_id' in data.keys(): 
		data['message_id'] = int(data['message_id'])
	if 'chat_id' in data.keys(): 
		data['chat_id'] = int(data['chat_id'])
	if 'downloaded' in data.keys(): 
		data['downloaded'] = data['downloaded'] == 1
	if 'uploaded' in data.keys(): 
		data['uploaded'] = data['uploaded'] == 1
	if 'edited' in data.keys(): 
		data['edited'] = data['edited'] == 1
	if 'text_urls' in data.keys() and data['text_urls'] is not None:
		data['text_urls'] = json.loads(data['text_urls'])
	return data

class HentaiSqliteDb(HentaiDatabase):

	def __init__(self, path):
		self.path = path
		self.init()

	def connect(self):
		logger.info('Start connect to database: {}'.format(self.path))
		self.conn = sqlite3.connect(self.path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
		self.conn.row_factory = dict_factory
		return self.conn

	def check_conn(func):
		@functools.wraps(func)
		def wrapper(*args, **kwargs):
			db  = args[0]
			return func(*args, **kwargs)
		return wrapper

	def init(self):
		self.connect()
		self.check_table('tg_hentai_msg', '''
			create table tg_hentai_msg
			(
				update_id TEXT
					constraint tg_hentai_msg_pk
						primary key,
				message_id TEXT not null,
				chat_id TEXT not null,
				orig_chat_text TEXT not null,
				text_urls TEXT not null,
				neo_chat_text TEXT,
				downloaded INTEGER default 0 not null,
				uploaded INTEGER default 0,
				edited INTEGER default 0 not null
			);
			''')
		self.check_table('tg_hentai_url', '''
			create table tg_hentai_url
			(
			  url           TEXT                  not null
			    constraint tg_hentai_url_pk
			      primary key,
			  title         TEXT,
			  domain         TEXT not null,
			  hentai_id_1	TEXT not null,
			  hentai_id_2	TEXT not null,
			  filepath      TEXT,
			  telegraph_url TEXT,
			  downloaded    INTEGER default 0 not null,
			  uploaded      INTEGER default 0 not null
			);
			''')


	def check_table(self, table_name, create_sql):
		logger.info('Start check table {} is exist'.format(table_name))
		if self.conn is None:
			return None
		cur = self.conn.cursor()
		ret = None
		try:
			cur.execute('SELECT 1 from ' + table_name)
			line = cur.fetchone()
			if line is not None:
				ret = True
		except Exception as e:
			try:
				logger.info('Database Error: {}'.format(repr(e)))
				cur = self.conn.rollback()
				cur = self.conn.cursor()
				logger.info('Create table {} ...'.format(table_name))
				cur.execute(create_sql)
				self.conn.commit()
			except Exception:
				logger.info("Create Table Error: " + create_sql)
		finally:
			if cur:
				cur.close()
		return ret

	def get_conn(self):
		return self.conn

	def close(self):
		logger.info('Close databse connect')
		self.conn.close()
		self.conn = None

	def get_max_update_id(self):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('SELECT max(update_id) as max FROM tg_hentai_msg')
		dictRows = cur.fetchall()
		cur.close()
		if dictRows[0]['max'] is None:
			return 0
		return int(dictRows[0]['max'])


	@check_conn
	def has_urls(self, urls):
		if not isinstance(urls, list):
			urls = [urls]
		questionmarks = '?' * len(urls)
		con = self.get_conn()
		cur = con.cursor()
		query = 'SELECT * FROM tg_hentai_url WHERE url in ({})'.format(','.join(questionmarks))
		cur.execute(query, urls)
		dictRows = cur.fetchall()
		cur.close()
		return list(map(lambda data:convert(data) , dictRows))


	@check_conn
	def add_msg(self, update_id, message_id, chat_id, orig_chat_text, text_urls):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('INSERT INTO tg_hentai_msg (update_id, message_id, chat_id, orig_chat_text, text_urls) VALUES (?, ?, ?, ?, ?)',
			(str(update_id), str(message_id), str(chat_id), orig_chat_text, json.dumps(text_urls)))
		con.commit()

	@check_conn
	def add_url(self, url, domain, hentai_id_1, hentai_id_2):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('INSERT INTO tg_hentai_url (url, domain, hentai_id_1, hentai_id_2) VALUES (?, ?, ?, ?)',
			(url, domain, hentai_id_1, hentai_id_2))
		con.commit()

	@check_conn
	def get_union(self, downloaded=None, uploaded=None, edited=None):
		con = self.get_conn()
		cur = con.cursor()
		sel_sql = 'SELECT * from tg_hentai_msg'
		var_list = []
		if downloaded is not None or uploaded is not None or edited is not None:
			sel_sql = sel_sql + ' WHERE TRUE '
			if downloaded is not None:
				sel_sql = sel_sql + ' and downloaded=? '
				var_list.append(1 if downloaded else 0)
			if uploaded is not None:
				sel_sql = sel_sql + ' and uploaded=?'
				var_list.append(1 if uploaded else 0)
			if edited is not None:
				sel_sql = sel_sql + ' and edited=?'
				var_list.append(1 if edited else 0)

		cur.execute(sel_sql, tuple(var_list))
		dictRows = cur.fetchall()
		cur.close()

		dictRows = list(map(lambda data:convert(data) , dictRows))
		for data in dictRows:
			data['url_array'] = self.has_urls(data['text_urls'])

		return dictRows

	def get_undownloaded(self):
		return self.get_union(downloaded=False)

	def get_unuploaded(self):
		return self.get_union(downloaded=True, uploaded=False)


	def get_unedited(self):
		return self.get_union(downloaded=True, uploaded=True, edited=False)

	@check_conn
	def get_msg(self, update_id=None, downloaded=None, uploaded=None, edited=None):
		con = self.get_conn()
		cur = con.cursor()
		sel_sql = 'SELECT * FROM tg_hentai_msg as msgs '
		condition_vars = list()
		if update_id is not None or downloaded is not None or uploaded is not None or edited is not None:
			sel_sql = sel_sql + ' WHERE TRUE '
			if update_id is not None:
				sel_sql = sel_sql + ' and msgs.update_id=? '
				condition_vars.append(str(update_id))
			if downloaded is not None:
				sel_sql = sel_sql + ' and msgs.downloaded=? '
				condition_vars.append(1 if downloaded else 0)
			if uploaded is not None:
				sel_sql = sel_sql + ' and msgs.uploaded=? '
				condition_vars.append(1 if uploaded else 0)
			if edited is not None:
				sel_sql = sel_sql + ' and msgs.edited=? '
				condition_vars.append(1 if edited else 0)

		cur.execute(sel_sql, tuple(condition_vars))
		dictRows = cur.fetchall()
		cur.close()
		return list(map(lambda data:convert(data) , dictRows))

	def get_undownloaded_msg(self):
		return self.get_msg(downloaded=False)

	def get_unuploaded_msg(self):
		return self.get_msg(downloaded=True, uploaded=False)
	
	def get_unedited_msg(self):
		return self.get_msg(downloaded=True, uploaded=True, edited=False)

	@check_conn
	def get_url(self, url=None, downloaded=None, uploaded=None):
		con = self.get_conn()
		cur = con.cursor()
		sel_sql = 'SELECT * FROM tg_hentai_url as urls'
		condition_vars = list()
		if url is not None or downloaded is not None or uploaded is not None:
			sel_sql = sel_sql + ' WHERE TRUE '
			if url is not None:
				sel_sql = sel_sql + ' and urls.url=? '
				condition_vars.append(url)
			if downloaded is not None:
				sel_sql = sel_sql + ' and urls.downloaded=? '
				condition_vars.append(downloaded)
			if uploaded is not None:
				sel_sql = sel_sql + ' and urls.uploaded=? '
				condition_vars.append(uploaded)

		cur.execute(sel_sql, tuple(condition_vars))
		dictRows = cur.fetchall()
		cur.close()
		return list(map(lambda data:convert(data) , dictRows))

	def get_undownloaded_url(self):
		self.get_url(downloaded=False)

	def get_unuploaded_url(self):
		return self.get_url(downloaded=True, uploaded=False)


	@check_conn
	def set_downloaded_url(self, url, title, save_path):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_url SET downloaded=TRUE, title=?, filepath=? WHERE url=?',
				(title, save_path, url))
		con.commit()


	@check_conn
	def set_downloaded_msg(self, update_id):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_msg SET downloaded=TRUE WHERE update_id=?',
				(str(update_id),))
		con.commit()


	@check_conn
	def set_uploaded_url(self, hentai_url, telegraph_url):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_url SET uploaded=TRUE, telegraph_url=? WHERE url=?',
				(telegraph_url, hentai_url, ))
		con.commit()


	@check_conn
	def set_uploaded_msg(self, update_id):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_msg SET uploaded=TRUE WHERE update_id = ?',
				(str(update_id), ))
		con.commit()


	@check_conn
	def set_edited(self, update_id, data):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_msg SET edited=TRUE, neo_chat_text=? WHERE update_id=?',
				(data['neoText'], str(update_id),))
		con.commit()

