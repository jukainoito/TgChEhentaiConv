# -*- coding: utf-8 -*-
import logging
import functools
import time

import psycopg2, psycopg2.extras

from .db import HentaiDatabase


logger = logging.getLogger(__name__)



class HentaiPgDb(HentaiDatabase):

	def __init__(self, host, database, user, password,port="5432"):
		self.database = database
		self.host = host
		self.user = user
		self.port = port
		self.password = password if password is not None and len(password)>0 else None
		self.conn = None
		self.init()

	def check_conn(func):
		@functools.wraps(func)
		def wrapper(*args, **kwargs):
			db  = args[0]
			while db.get_conn()is None or db.get_conn().closed:
				try:
					db.connect()
					break
				except Exception as e:
					time.sleep(30)
					# logger.debug('Database Error: {}'.format(repr(e)))
			return func(*args, **kwargs)
		return wrapper

	def connect(self):
		logger.info('Start connect to database: {}:{} {} {}'.format(self.host, self.port, self.user, self.database))
		self.conn = psycopg2.connect(database=self.database, user=self.user, host=self.host, port=self.port, password=self.password)
		return self.conn

	@check_conn
	def init(self):
		self.check_table('tg_hentai_msg', '''
			create table tg_hentai_msg
			(
				update_id BIGINT
					constraint tg_hentai_msg_pk
						primary key,
				message_id BIGINT not null,
				chat_id BIGINT not null,
				orig_chat_text TEXT not null,
				text_urls TEXT[] not null,
				neo_chat_text TEXT,
				downloaded BOOLEAN default false not null,
				uploaded BOOLEAN default false,
				edited BOOLEAN default false not null
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
			  downloaded    boolean default false not null,
			  uploaded      boolean default false not null
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

	@check_conn
	def get_max_update_id(self):
		con = self.get_conn()
		cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		cur.execute('SELECT max(update_id) FROM tg_hentai_msg')
		dictRows = cur.fetchall()
		cur.close()
		if dictRows[0]['max'] is None:
			return 0
		return dictRows[0]['max']


	@check_conn
	def has_urls(self, urls):
		if not isinstance(urls, list):
			urls = [urls]
		con = self.get_conn()
		cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		cur.execute('SELECT * FROM tg_hentai_url WHERE url=ANY(%s)', (urls,))
		dictRows = cur.fetchall()
		cur.close()
		return dictRows


	@check_conn
	def add_msg(self, update_id, message_id, chat_id, orig_chat_text, text_urls):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('INSERT INTO tg_hentai_msg (update_id, message_id, chat_id, orig_chat_text, text_urls) VALUES (%s, %s, %s, %s, %s)',
			(update_id, message_id, chat_id, orig_chat_text, text_urls))
		con.commit()

	@check_conn
	def add_url(self, url, domain, hentai_id_1, hentai_id_2):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('INSERT INTO tg_hentai_url (url, domain, hentai_id_1, hentai_id_2) VALUES (%s, %s, %s, %s)',
			(url, domain, hentai_id_1, hentai_id_2))
		con.commit()

	@check_conn
	def get_union(self, downloaded=None, uploaded=None, edited=None):
		con = self.get_conn()
		cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		sel_sql = '''
			SELECT msgs.*, json_agg(urls.*) AS url_array  FROM tg_hentai_msg AS msgs
			  LEFT JOIN tg_hentai_url AS urls
			    ON urls.url = ANY(msgs.text_urls)
			    '''
		var_list = []
		if downloaded is not None or uploaded is not None or edited is not None:
			sel_sql = sel_sql + ' WHERE TRUE '
			if downloaded is not None:
				sel_sql = sel_sql + ' and msgs.downloaded=%s '
				var_list.append(downloaded)
			if uploaded is not None:
				sel_sql = sel_sql + ' and msgs.uploaded=%s'
				var_list.append(uploaded)
			if edited is not None:
				sel_sql = sel_sql + ' and msgs.edited=%s'
				var_list.append(edited)
		sel_sql = sel_sql + ' GROUP BY msgs.update_id'

		cur.execute(sel_sql, tuple(var_list))
		dictRows = cur.fetchall()
		cur.close()
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
		cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		sel_sql = 'SELECT * FROM tg_hentai_msg as msgs '
		condition_vars = list()
		if update_id is not None or downloaded is not None or uploaded is not None or edited is not None:
			sel_sql = sel_sql + ' WHERE TRUE '
			if update_id is not None:
				sel_sql = sel_sql + ' and msgs.update_id=%s '
				condition_vars.append(update_id)
			if downloaded is not None:
				sel_sql = sel_sql + ' and msgs.downloaded=%s '
				condition_vars.append(downloaded)
			if uploaded is not None:
				sel_sql = sel_sql + ' and msgs.uploaded=%s '
				condition_vars.append(uploaded)
			if edited is not None:
				sel_sql = sel_sql + ' and msgs.edited=%s '
				condition_vars.append(edited)

		cur.execute(sel_sql, tuple(condition_vars))
		dictRows = cur.fetchall()
		cur.close()
		return dictRows

	def get_undownloaded_msg(self):
		return self.get_msg(downloaded=False)

	def get_unuploaded_msg(self):
		return self.get_msg(downloaded=True, uploaded=False)
	
	def get_unedited_msg(self):
		return self.get_msg(downloaded=True, uploaded=True, edited=False)

	@check_conn
	def get_url(self, url=None, downloaded=None, uploaded=None):
		con = self.get_conn()
		cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		sel_sql = 'SELECT * FROM tg_hentai_url as urls'
		condition_vars = list()
		if url is not None or downloaded is not None or uploaded is not None:
			sel_sql = sel_sql + ' WHERE TRUE '
			if url is not None:
				sel_sql = sel_sql + ' and urls.url=%s '
				condition_vars.append(url)
			if downloaded is not None:
				sel_sql = sel_sql + ' and urls.downloaded=%s '
				condition_vars.append(downloaded)
			if uploaded is not None:
				sel_sql = sel_sql + ' and urls.uploaded=%s '
				condition_vars.append(uploaded)

		cur.execute(sel_sql, tuple(condition_vars))
		dictRows = cur.fetchall()
		cur.close()
		return dictRows

	def get_undownloaded_url(self):
		self.get_url(downloaded=False)

	def get_unuploaded_url(self):
		return self.get_url(downloaded=True, uploaded=False)


	@check_conn
	def set_downloaded_url(self, url, title, save_path):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_url SET downloaded=TRUE, title=%s, filepath=%s WHERE url=%s',
				(title, save_path, url))
		con.commit()


	@check_conn
	def set_downloaded_msg(self, update_id):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_msg SET downloaded=TRUE WHERE update_id=%s',
				(update_id,))
		con.commit()


	@check_conn
	def set_uploaded_url(self, hentai_url, telegraph_url):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_url SET uploaded=TRUE, telegraph_url=%s WHERE url=%s',
				(telegraph_url, hentai_url, ))
		con.commit()


	@check_conn
	def set_uploaded_msg(self, update_id):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_msg SET uploaded=TRUE WHERE update_id = %s',
				(update_id, ))
		con.commit()


	@check_conn
	def set_edited(self, update_id, data):
		con = self.get_conn()
		cur = con.cursor()
		cur.execute('UPDATE tg_hentai_msg SET edited=TRUE, neo_chat_text=%s WHERE update_id=%s',
				(data['neoText'], update_id,))
		con.commit()

