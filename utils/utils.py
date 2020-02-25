# -*- coding: utf-8 -*-
import re
import logging

logger = logging.getLogger(__name__)

def get_hentai_url_from_text(text):
	pattern = re.compile(r'(http[s]?://(e-hentai.org|exhentai.org)/g/(\w+)/(\w+)/)')
	return pattern.findall(text)

# from urllib.parse import urlparse
# def get_filename_from_hentai_url(url):
# 	tmpUrl = urlparse(url)
# 	if tmpUrl.netloc in ['e-hentai.org', 'exhentai.org']:
# 		tmpUrl = tmpUrl.scheme + '://' + tmpUrl.netloc + tmpUrl.path
# 		if tmpUrl[-1]  != '/':
# 			tmpUrl = tmpUrl + '/'
# 		match = re.search('/g\/(.*)\/(.*)\/$', tmpUrl)
# 		if len(match[1]) == 0 or len(match[2]) == 0:
# 			logger.info('Url not match: {}'.format(url))
# 			return None
# 		else:
# 			return match[1] + '-' + match[2] + '.zip'
