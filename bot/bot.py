# -*- coding: utf-8 -*-
import telegram
import logging
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)

class TelegramBot(object):

	def __init__(self, token, botRequest=None):
		logger.info('Init telegram bot token: {}'.format(token))
		self.token = token
		self.botRequest = botRequest
		self.bot = telegram.Bot(token=token, request=botRequest)

	def get_updates(self, offset=None):
		# logger.info('Start get updates offset: {}'.format(offset))
		updates = None
		while True:
			try:
				updates = self.bot.getUpdates(offset = offset)
				break
			except:
				pass
		return updates

	def edit_hentai_to_telegraph(self, chat_id, message_id, origin_text, url_datas):
		buttons = list()
		count = 0
		neo_text = origin_text
		url_len = len(url_datas)
		for url in url_datas:
			domain = url['domain']
			title = url['title']
			source = url['source']
			neo = url['neo']

			count = count + 1
			if url_len is not 1:
				btn_text = 'Source_{}'.format(str(count).zfill(2))
			else:
				btn_text = 'Source'
			buttons.append(InlineKeyboardButton(btn_text, url=source))
			neo_text = neo_text.replace(source, '<a href="{}">{}</a>'.format(neo, title))

		reply_markup = InlineKeyboardMarkup([buttons])
		self.bot.edit_message_text(neo_text, reply_markup=reply_markup, chat_id=chat_id, message_id=message_id, parse_mode='HTML', disable_web_page_preview=False)
		return {
			'neoText': neo_text
		}
