# -*- coding: utf-8 -*-
import requests
import logging
import os
import re
from lxml import etree
import sys
sys.path.append('../')

from cookies import HentaiCookies
from utils import get_hentai_url_from_text

logger = logging.getLogger(__name__)

class EHentaiDownloader(object):

	def __init__(self, cookies_path, save_dir=None, proxy=None):
		self.HEADERS = {
			'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
		}
		self.HENTAI_ARCHIVER_URL_XPATH = '//*[@id="gd5"]/p[2]/a/@onclick'
		self.HENTAI_TITLE_XPATH = '//*[@id="gj"]/text()'
		self.PROXIES = None
		if proxy is not None:
			self.PROXIES = {
				'http': proxy,
				'https': proxy
			}
		if save_dir is None:
			save_dir = '.'
		self.SAVE_DIR = os.path.normpath(os.path.abspath(save_dir))
		if  not os.path.exists(self.SAVE_DIR):
			os.makedirs(self.SAVE_DIR)

		self.hentai_cookies = HentaiCookies(cookies_path)

	def get_page_info(self, cookies, page_url):
		logger.debug(page_url)
		logger.debug(cookies)
		r = requests.get(page_url, headers=self.HEADERS, cookies=cookies, proxies=self.PROXIES)
		logger.debug(r.text)
		htmlEtree = etree.HTML(r.text)
		archiver = ''.join(htmlEtree.xpath(self.HENTAI_ARCHIVER_URL_XPATH))
		if archiver is None:
			logger.warning('Get archiver url error: ' + page_url)
			return None
		archiver = re.search('\'(.*)\'', archiver)[1]
		title = ''.join(htmlEtree.xpath(self.HENTAI_TITLE_XPATH))
		return {
			'archiver': archiver,
			'title': title
		}

	def download_archiver(self, cookies, archer_url, path):
		payload = {
			'dltype': 'res',
			'dlcheck': 'Download Resample Archive'
		}
		logger.info('Get archer url: {}'.format(archer_url))
		r = requests.post(archer_url, data=payload, headers=self.HEADERS, cookies=cookies, proxies=self.PROXIES, verify=False, timeout=60)
		htmlEtree = etree.HTML(r.text)
		continueUrl = ''.join(htmlEtree.xpath('//*[@id="continue"]/a/@href'))
		dlUrl = continueUrl + '?start=1'
		logger.info('Get download url: {}'.format(dlUrl))
		if os.path.exists(path):
			return None
		r = requests.get(dlUrl, headers=self.HEADERS, proxies=self.PROXIES, verify=False, stream = True)
		f = open(path, "wb")
		for chunk in r.iter_content(chunk_size=1024):
			if chunk:
				f.write(chunk)
				f.flush()

	def download(self, page_url):
		file_name = get_hentai_url_from_text(page_url)
		logger.debug(file_name)
		file_name = file_name[0]
		domain = file_name[1]
		file_name = file_name[2] + '-' + file_name[3]
		useCookies = self.hentai_cookies.get(domain)
		logger.debug(useCookies)
		path = os.path.abspath(os.path.normpath(os.path.join(self.SAVE_DIR, file_name))) + '.zip'
		
		reTry = 0
		info = None
		while reTry <= 10:
			try:
				info = self.get_page_info(useCookies, page_url)
				info['path'] = path
				self.download_archiver(useCookies, info['archiver'], path)
				break
			except Exception as e:
				logger.info('Download Error: {}'.format(repr(e)))
				reTry=reTry+1
		return info
