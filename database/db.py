# coding:utf-8

from abc import ABCMeta, abstractmethod

class HentaiDatabase:
	__metaclass__ = ABCMeta

	@abstractmethod
	def get_max_update_id(self):
		pass

	@abstractmethod
	def has_urls(self, urls):
		pass

	@abstractmethod
	def add_msg(self, update_id, message_id, chat_id, orig_chat_text, text_urls):
		pass

	@abstractmethod
	def add_url(self, url, domain, hentai_id_1, hentai_id_2):
		pass

	@abstractmethod
	def get_undownloaded(self):
		pass

	@abstractmethod
	def get_unuploaded(self):
		pass

	@abstractmethod
	def get_unedited(self):
		pass

	@abstractmethod
	def get_msg(self, update_id=None, downloaded=None, uploaded=None, edited=None):
		pass

	@abstractmethod
	def get_undownloaded_msg(self):
		pass

	@abstractmethod
	def get_unuploaded_msg(self):
		pass
	
	@abstractmethod
	def get_unedited_msg(self):
		pass

	@abstractmethod
	def get_url(self, url=None, downloaded=None, uploaded=None):
		pass

	@abstractmethod
	def get_undownloaded_url(self):
		pass

	@abstractmethod
	def get_unuploaded_url(self):
		pass

	@abstractmethod
	def set_downloaded_url(self, url, title, save_path):
		pass

	@abstractmethod
	def set_downloaded_msg(self, update_id):
		pass

	@abstractmethod
	def set_uploaded_url(self, hentai_url, telegraph_url):
		pass

	@abstractmethod
	def set_uploaded_msg(self, update_id):
		pass

	@abstractmethod
	def set_edited(self, update_id, data):
		pass
