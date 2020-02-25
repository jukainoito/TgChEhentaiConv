# -*- coding: utf-8 -*-

from telegraph import Telegraph
import requests

from telegraph import TelegraphException

class HentaiTelegraphApi(object):
	
    __slots__ = ('access_token', 'session', 'proxies')

    def __init__(self, access_token=None, session=None, proxy=None):
        self.access_token = access_token
        if session is None:
            self.session = requests.Session()
        else:
            self.session = session
        self.proxies = None
        if proxy is not None:
            self.proxies = {
                'http': proxy,
                'https': proxy
            }

    def method(self, method, values=None, path=''):
        values = values.copy() if values is not None else {}

        if 'access_token' not in values and self.access_token:
            values['access_token'] = self.access_token

        response = self.session.post(
            'https://api.telegra.ph/{}/{}'.format(method, path),
            values, proxies=self.proxies
        ).json()

        if response.get('ok'):
            return response['result']

        raise TelegraphException(response.get('error'))


class HentaiTelegraph(Telegraph):
    """ Telegraph API client helper

    :param access_token: Telegraph access token
    """

    __slots__ = ('_telegraph',)

    def __init__(self, access_token=None, session=None, proxy=None):
        self._telegraph = HentaiTelegraphApi(access_token, session, proxy)
