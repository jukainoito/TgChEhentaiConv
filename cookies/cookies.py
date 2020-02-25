# -*- coding: utf-8 -*-
import json
import os
import logging

logger = logging.getLogger(__name__)

class HentaiCookies(object):
	def __init__(self, cookiesFilePath):
		self.cookiesFilePath = cookiesFilePath
		self.load_cookies()

	def get(self, domain):
		if domain not in ['e-hentai.org', 'exhentai.org']:
			logger.info('unsupport domain: {}'.format(domain))
			return None
		return self.cookies[domain]

	def load_cookies(self):
		eCookies = {}
		exCookies = {}
		try:
			temp, ext = os.path.splitext(self.cookiesFilePath)
			if ext.lower() == '.json':
				with open(self.cookiesFilePath, mode='r', encoding='utf-8') as f:
					data = json.load(f)
					if isinstance(data, list):
						for obj in data:
							if obj['domain'] == '.e-hentai.org':
								eCookies[obj['name']] = obj['value']
							elif obj['domain'] == '.exhentai.org':
								exCookies[obj['name']] = obj['value']
		except Exception:
			logger.error("Faild to load cookies file {}".format(self.cookiesFilePath),exc_info = True)
		finally:
			if f:
				f.close()
		self.cookies = {
			'e-hentai.org': eCookies,
			'exhentai.org': exCookies
		}