# -*- coding: utf-8 -*-
import sys, os, json, yaml, zipfile, shutil, threading, time, signal, telegram

from natsort import natsorted
from urllib.parse import urlparse
from PIL import Image
from io import BytesIO


import logging, argparse

import urllib3
urllib3.disable_warnings()

from daemon import Daemon

from database import HentaiDatabase
from bot import TelegramBot
from database import HentaiDatabase
from utils import get_hentai_url_from_text
from ehentai import EHentaiDownloader
from web import HentaiTelegraph, upload_file


PROGRAM_DIR_PATH = os.path.dirname(os.path.abspath(sys.argv[0]))
PID_LOCK_PATH = os.path.join(PROGRAM_DIR_PATH, 'lock.pid')
DEFAULT_LOG_PATH = os.path.join(PROGRAM_DIR_PATH, 'run.log')

parser = argparse.ArgumentParser()
parser.add_argument("-s", "-stop", "--stop", help="stop daemon", action="store_true")
parser.add_argument("-c", "-config", "--config", help="YAML config file", action="store")
parser.add_argument("-delete", "--delete", help="delete download archiver file", action="store_true")
parser.add_argument("-debug", "--debug", help="debug", action="store_true")
args = parser.parse_args()

IS_DELETE_ARCHIVER = args.delete
IS_STOP = args.stop
IS_DEBUG = args.debug
YAML_CONFIG_PATH = None
if args.config is not None:
	YAML_CONFIG_PATH = os.path.normpath(os.path.abspath(args.config))

class TgHentaiBot(object):

	def __init__(self, config):
		self.CONFIG = config
		self.initDb()
		self.initBot()
		self.initEHentaiDownloader()
		self.initTelegraph()

	def initTelegraph(self):
		self.telegraph = HentaiTelegraph(self.CONFIG['telegraph']['token'], proxy=self.CONFIG['telegraph']['proxy'])

	def initEHentaiDownloader(self):
		self.hentaiDownloader = EHentaiDownloader(self.CONFIG['ehentai']['cookies_file'],
			self.CONFIG['ehentai']['archer_dir'],
			self.CONFIG['ehentai']['proxy'])


	def initDb(self):
		self.db = HentaiDatabase(self.CONFIG['database']['host'], 
			self.CONFIG['database']['database'], 
			self.CONFIG['database']['user'],
			self.CONFIG['database']['password'],)

	def initBot(self):
		botRequest = None
		if self.CONFIG['bot']['proxy'] is not None:
			botRequest = telegram.utils.request.Request(
				con_pool_size = 10,
				proxy_url=self.CONFIG['bot']['proxy'],
				connect_timeout=120, read_timeout=1200)
		self.bot = TelegramBot(self.CONFIG['bot']['token'], botRequest=botRequest)


	def addUrls(self, matchUrls, urls=None):
		if urls is None:
			urls = list(map(lambda tmp: tmp[0], matchUrls))
		inUrls = list(map(lambda tmp: tmp['url'], self.db.has_urls(urls)))
		noInUrls = list(filter(lambda url: (url[0] not in inUrls), matchUrls))
		for url in noInUrls:
			res = self.db.get_url(url=url[0])
			if len(res) == 0:
				self.db.add_url(url[0], url[1], url[2], url[3])


	def doUpdates(self):
		maxUpdateId = self.db.get_max_update_id()
		if IS_DEBUG:
			logger.debug('maxUpdateId: {}'.format(maxUpdateId))
		updates = self.bot.get_updates(maxUpdateId + 1)
		for update in updates:
			logger.info('Handle update id: {}'.format(update.update_id))
			if update.channel_post is not None and update.channel_post.chat.type == 'channel' and update.channel_post.chat.id == self.CONFIG['bot']['chat_id'] and update.channel_post.text is not None:
				matchUrls = get_hentai_url_from_text(update.channel_post.text)
				if len(matchUrls) != 0:
					urls = list(map(lambda tmp: tmp[0], matchUrls))
					res = self.db.get_msg(update_id=update.update_id)
					if len(res) == 0:
						self.db.add_msg(update.update_id, update.channel_post.message_id, update.channel_post.chat.id, update.channel_post.text, urls)
					self.addUrls(matchUrls, urls)
					# inUrls = list(map(lambda tmp: temp['url'], self.db.has_urls(urls)))
					# noInUrls = list(filter(lambda url: (url[0] not in inUrls), matchUrls))
					# for url in noInUrls:
					# 	self.db.add_url(url[0], url[1], url[2], url[3])

	def doSingleDownloadUrl(self, urlData):
		pageUrl = urlData['url']
		logger.info('Download url:{}'.format(pageUrl))
		info = self.hentaiDownloader.download(pageUrl)
		if info is None:
			return None
		self.db.set_downloaded_url(pageUrl, info['title'], info['path'])
		return info

	def doDownload(self):
		datas = self.db.get_undownloaded()
		for data in datas:
			hasDlErr = 0
			updateId = data['update_id']
			for urlData in data['url_array']:
				if urlData is None:
					matchUrls = get_hentai_url_from_text(data['orig_chat_text'])
					self.addUrls(matchUrls)
					continue
				elif urlData['downloaded']:
					continue
				info = self.doSingleDownloadUrl(urlData)
				if info is None:
					hasDlErr = hasDlErr + 1
			if hasDlErr == 0:
				self.db.set_downloaded_msg(updateId)


	def compressImage(self, inputFile, targetSize=5*1024*1024, step=5, quality=100):
		im = Image.open(inputFile)
		if os.path.getsize(inputFile) >= (targetSize-1024):
			while quality>0:
				imgIO = BytesIO()
				im.save(imgIO, 'jpeg', quality=quality)
				if imgIO.tell() < (targetSize-1024):
					im.save(inputFile, 'jpeg', quality=quality)
					break
				quality = quality - step


	def unzip(self, zipFilePath, unZipDir):
		logger.info('Start unzip file: {}'.format(zipFilePath))

		with zipfile.ZipFile(zipFilePath) as existing_zip:
			existing_zip.extractall(unZipDir)


	def upImageDirToTelegraph(self, imageDir):
		logger.info('Upload image dir: {}'.format(imageDir))

		uploadFiles = os.listdir(imageDir)
		uploadFiles = natsorted(uploadFiles)
		uploadFiles = list(map(lambda file: imageDir+'/'+file, uploadFiles))

		# removeFiles = []
		for uploadFile in uploadFiles:
			fileSize = os.path.getsize(uploadFile)
			if int(fileSize/1024) > (5*1024-1):
				self.compressImage(uploadFile)
				# removeFiles.append(uploadFile)

		urls = []
		if len(uploadFiles) > 20:
			up_size = len(uploadFiles)
			part = 20
			temp_pos = 0
			while temp_pos != None :
				new_pos = temp_pos + part
				if new_pos >= up_size:
					new_pos = None
				newUploadFiles = uploadFiles[temp_pos: new_pos]
				temp_urls = upload_file(newUploadFiles, self.CONFIG['telegraph']['proxy'])
				urls.extend(temp_urls)
				temp_pos = new_pos

		else:
			urls = upload_file(uploadFiles, self.CONFIG['telegraph']['proxy'])
		return urls

	def upTelegraphPage(self, title, pathTitle, imageUrls):
		logger.info('Upload telegraph page path: {} title: {}'.format(pathTitle, title))

		telegraphContent = ''
		for imageUrl in imageUrls:
			telegraphContent = telegraphContent + '<img src="'+imageUrl+'">'
		pageRes = self.telegraph.create_page(pathTitle, html_content='None')
		pageRes = self.telegraph.edit_page(pageRes['path'], title, html_content=telegraphContent)
		return pageRes

	def doSingleUpload(self, urlData):
		pageUrl = urlData['url']
		title = urlData['title']
		filePath = urlData['filepath']
		logger.info('Start upload file: {}  url: {}'.format(filePath, pageUrl))
		telegraphTitle = str(self.CONFIG['bot']['chat_id'])  + '-' + urlData['domain'] + '-' + urlData['hentai_id_1'] + '_' + urlData['hentai_id_2']
		unZipDir, ext = os.path.splitext(filePath)

		try:
			self.unzip(filePath, unZipDir)
			urls = self.upImageDirToTelegraph(unZipDir)
			shutil.rmtree(unZipDir)
			if IS_DELETE_ARCHIVER:
				try:
					os.remove(filePath)
				except Exception as delErr:
					logger.info('Delete file error: {} file: '.format(repr(e), filePath))

			createRes = self.upTelegraphPage(title, telegraphTitle, urls)
			logger.info(createRes)
			self.db.set_uploaded_url(pageUrl, 'https://telegra.ph/{}'.format(createRes['path']))
			return createRes
		except Exception as e:
			logger.info('Upload Error: {}'.format(repr(e)))
			return None

	def doUpload(self):
		datas = self.db.get_unuploaded()
		hasErr = 0
		for data in datas:
			updateId = data['update_id']
			for urlData in data['url_array']:
				if urlData is not None and urlData['uploaded']:
					continue
				res = self.doSingleUpload(urlData)
				if res == None:
					hasErr = hasErr + 1
			if hasErr == 0:
				self.db.set_uploaded_msg(updateId)

	def doEdit(self):
		datas = self.db.get_unedited();
		logger.debug('Satrt Edit: {}'.format(datas))
		for data in datas:
			updateId = data['update_id']
			chatId = data['chat_id']
			msgId = data['message_id']

			originText = data['orig_chat_text']

			urlReps = []
			for urlData in data['url_array']:
				sourceUrl = urlData['url']
				telegraphUrl = urlData['telegraph_url']
				title = urlData['title']
				urlReps.append({
					'title': title,
					'source': sourceUrl,
					'neo': telegraphUrl,
					'domain': urlData['domain']
				})

			logger.info('Edit msg updateId: {} msgId: {} '.format(updateId, msgId))

			data = self.bot.edit_hentai_to_telegraph(chat_id=chatId, message_id=msgId, origin_text=originText, url_datas=urlReps)
			self.db.set_edited(updateId, data)

def doLoopFun(func):
	while True:
		if IS_DEBUG:
			func()
		else:
			try:
				func()
			except Exception as e: 
				logger.warning('Error: {}'.format(repr(e)))
		time.sleep(10)

def break_exit(signalnum, handler):
	if signalnum == 2:
		os._exit(0)

class App(Daemon):
	def run(self):
		if YAML_CONFIG_PATH is None:
			config = readEnvConfig()
		else:
			config = readConfigFromYAML(YAML_CONFIG_PATH)

		global logger

		logging.basicConfig(level = logging.DEBUG if IS_DEBUG else logging.INFO, 
			filename=None if IS_DEBUG else config['log'], filemode="a",
			format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		logger = logging.getLogger(__name__)

		runner = TgHentaiBot(config)
		signal.signal(signal.SIGINT, break_exit)
		signal.signal(signal.SIGTERM, break_exit)
		threads=[]
		threads.append(threading.Thread(target=doLoopFun, args=(runner.doUpdates,)))
		threads.append(threading.Thread(target=doLoopFun, args=(runner.doDownload,)))
		threads.append(threading.Thread(target=doLoopFun, args=(runner.doUpload,)))
		threads.append(threading.Thread(target=doLoopFun, args=(runner.doEdit,)))
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()


def isEmpteString(var):
	if var is None or len(var)==0:
		return True
	return False

def getValueOfDict(key, dictData):
	if key in dictData:
		return dictData[key]
	else:
		return None

def hasValueOfDict(key, dictData):
	if key in dictData:
		return True
	else:
		return False


def readEnvConfig():
	config = {
		'TG_HENTAI_ALL_PROXY': getValueOfDict('TG_HENTAI_ALL_PROXY', os.environ),
		'TG_HENTAI_DATABASE': getValueOfDict('TG_HENTAI_DATABASE', os.environ),
		'TG_HENTAI_DATABASE_HOST': getValueOfDict('TG_HENTAI_DATABASE_HOST', os.environ),
		'TG_HENTAI_DATABASE_USER': getValueOfDict('TG_HENTAI_DATABASE_USER', os.environ),
		'TG_HENTAI_DATABASE_PASSWORD': getValueOfDict('TG_HENTAI_DATABASE_PASSWORD', os.environ),
		'TG_HENTAI_BOT_TOKEN': getValueOfDict('TG_HENTAI_BOT_TOKEN', os.environ),
		'TG_HENTAI_BOT_PROXY': getValueOfDict('TG_HENTAI_BOT_PROXY', os.environ),
		'TG_HENTAI_BOT_CHAT_ID': getValueOfDict('TG_HENTAI_BOT_CHAT_ID', os.environ),
		'TG_HENTAI_EHENTAI_COOKIS': getValueOfDict('TG_HENTAI_EHENTAI_COOKIS', os.environ),
		'TG_HENTAI_EHENTAI_PROXY': getValueOfDict('TG_HENTAI_EHENTAI_PROXY', os.environ),
		'TG_HENTAI_EHENTAI_ARCHER_DIR': getValueOfDict('TG_HENTAI_EHENTAI_ARCHER_DIR', os.environ),
		'TG_HENTAI_TELEGRAPH_PROXY': getValueOfDict('TG_HENTAI_TELEGRAPH_PROXY', os.environ),
		'TG_HENTAI_TELEGRAPH_TOKEN': getValueOfDict('TG_HENTAI_TELEGRAPH_TOKEN', os.environ),
		'TG_HENTAI_LOG': getValueOfDict('TG_HENTAI_LOG', os.environ)
	}
	config['TG_HENTAI_BOT_PROXY'] = config['TG_HENTAI_BOT_PROXY'] if not isEmpteString(config['TG_HENTAI_BOT_PROXY']) else config['TG_HENTAI_ALL_PROXY']
	config['TG_HENTAI_EHENTAI_PROXY'] = config['TG_HENTAI_EHENTAI_PROXY'] if not isEmpteString(config['TG_HENTAI_EHENTAI_PROXY']) else config['TG_HENTAI_ALL_PROXY']
	config['TG_HENTAI_TELEGRAPH_PROXY'] = config['TG_HENTAI_TELEGRAPH_PROXY'] if not isEmpteString(config['TG_HENTAI_TELEGRAPH_PROXY']) else config['TG_HENTAI_ALL_PROXY']
	config['TG_HENTAI_LOG'] = config['TG_HENTAI_LOG'] if not isEmpteString(config['TG_HENTAI_LOG']) else DEFAULT_LOG_PATH
	
	return {
		"log": config['TG_HENTAI_LOG'],
		"databse": {
			"host": config['TG_HENTAI_DATABASE_HOST'],
			"database": config['TG_HENTAI_DATABASE'],
			"user": config['TG_HENTAI_DATABASE_USER'],
			"password": config['TG_HENTAI_DATABASE_PASSWORD']
		},
		"bot": {
			"token": config['TG_HENTAI_BOT_TOKEN'],
			"proxy": config['TG_HENTAI_BOT_PROXY'],
			"chat_id": None if config['TG_HENTAI_BOT_CHAT_ID'] is None else int(config['TG_HENTAI_BOT_CHAT_ID'])
		},
		"ehentai": {
			"cookies_file": config['TG_HENTAI_EHENTAI_COOKIS'],
			"proxy": config['TG_HENTAI_EHENTAI_PROXY'],
			"archer_dir": config['TG_HENTAI_EHENTAI_ARCHER_DIR']
		},
		"telegraph": {
			"token": config['TG_HENTAI_TELEGRAPH_TOKEN'],
			"proxy": config['TG_HENTAI_TELEGRAPH_PROXY']
		}
	}

def readConfigFromYAML(file):
	with open(file) as f:
		try:
			yml = yaml.safe_load(f)
			allProxy = getValueOfDict('proxy', yml)
			if not hasValueOfDict('proxy', yml['bot']):
				yml['bot']['proxy'] = allProxy
			if not hasValueOfDict('proxy', yml['ehentai']):
				yml['ehentai']['proxy'] = allProxy
			if not hasValueOfDict('proxy', yml['telegraph']):
				yml['telegraph']['proxy'] = allProxy


			if getValueOfDict('log', yml) is None:
				yml['log'] = DEFAULT_LOG_PATH
			return yml
		except yaml.scanner.ScannerError as e:
			print('YAML file read error')
			sys.exit(-1)

if __name__ == '__main__':
	app = App(PID_LOCK_PATH)
	if not IS_STOP:
		if IS_DEBUG:
			app.run()
		else:
			# app.run()
			app.start()
	else:
		app.stop()
