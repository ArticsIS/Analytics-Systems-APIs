# -*- coding: utf-8 -*-

import requests
import re
import json
import logging
from datetime import datetime, timedelta

class YandexClient:
	def __init__(self, app_config_path, credentials=None):
		config_file = open(app_config_path)
		self.app_config = json.load(config_file)
		config_file.close()
		if credentials is not None:
			self.credentials = credentials
			safest_time = datetime.now() + timedelta(hours=10)
			if safest_time >= datetime.strptime(self.credentials['expired_at'], '%Y-%m-%d %H:%M:%S'):
				refreshed_payload = {
					'grant_type': 'refresh_token',
					'refresh_token': self.credentials['refresh_token'],
					'client_id': self.app_config['client_id'],
					'client_secret': self.app_config['client_secret']
				}
				refreshed_auth_data = self.requestProto('POST', 'oauth', 'token', refreshed_payload)
				if refreshed_auth_data is not None:
					refreshed_auth_data['expired_at'] = (datetime.datetime.now() + datetime.timedelta(
						seconds=refreshed_auth_data['expires_in'])).strftime('%Y-%m-%d %H:%M:%S')
					self.credentials = refreshed_auth_data
				else:
					logging.error('YA: refresh: ERROR')
					raise Exception('Token refresh caused exception')
		else:
			logging.error('YA: empty credentials')
			print('Please obtain new credentials via link: {0!s}'.format(self.returnOAuthLink()))
	def returnOAuthLink(self):
		return 'https://oauth.yandex.ru/authorize?response_type=code&client_id={0!s}'.format(self.app_config['client_id'])
	def processOAuthCode(self, request):
		error_code = request.args.get('error', default=None)
		error_description = request.args.get('error_description', default=None)
		code = request.args.get('code', default=None)
		if error_code is not None:
			credentials_json = {
				'error': True,
				'code': error_code,
				'description': error_description
			}
		else:
			exchange_payload = {
				'grant_type': 'authorization_code',
				'code': code,
				'client_id': self.app_config['client_id'],
				'client_secret': self.app_config['client_secret'],
			}
			exchanger = self.requestProto('POST', 'oauth', 'token', exchange_payload)
			token_response = json.loads(exchanger.text)
			if exchanger.status_code != 200:
				return False
			token_response['expired_at'] = (
						datetime.datetime.now() + datetime.timedelta(seconds=token_response['expires_in'])).strftime(
				'%Y-%m-%d %H:%M:%S')
			self.credentials = token_response
			return True
	def returnCredentials(self):
		return self.credentials
	def getRegions(self):
		regions = self.directRequestProto('api.direct', 'live/v4/json/', {'method': 'GetRegions', 'locale': 'ru'})
		if regions is not None:
			return regions['data']
		else:
			return None
	def getWordstatReportList(self):
		reports = self.directRequestProto('api.direct', 'live/v4/json/', {'method': 'GetWordstatReportList'})
		if reports is not None:
			return reports['data']
		else:
			return None
	def getWordstatReport(self, report_id):
		report = self.directRequestProto('api.direct', 'live/v4/json/', {'method': 'GetWordstatReport', 'param': report_id})
		if report is not None:
			return report.get('data', None)
		else:
			return None
	def returnKeywordSuggestions(self, keywords):
		suggestions = self.directRequestProto('api.direct', 'live/v4/json/', {'method': 'GetKeywordsSuggestion', 'param': {'Keyword': keywords}})
		if suggestions is not None:
			return suggestions.get('data', None)
		else:
			return None
	def getCountersList(self, params=None):
		raw_response = self.requestProto('GET', 'api-metrika', 'management/v1/counters', params, restricted=True)
		if raw_response is not None:
			counters = raw_response['counters']
			if params is not None and int(params.get('per_page', 0) + params.get('offset', 1) - 1) < int(
					raw_response['rows']):
				params['offset'] = params['offset'] + len(counters)
				counters += self.getCountersList(params)
			else:
				return counters
		else:
			return None
	def getApplicationsList(self):
		raw_response = self.requestProto('GET', 'api.appmetrica', 'management/v1/applications', restricted=True)
		if raw_response is not None:
			counters = raw_response['applications']
			return counters
		else:
			return None
	def getCounterGoals(self, counter_id, include_deleted=True):
		goals = self.requestProto('GET', 'api-metrika', 'management/v1/counter/{0!s}/goals'.format(counter_id), {'useDeleted': include_deleted}, restricted=True)
		return goals
	def getReport(self, counter_id, metrics, start_date, end_date, dimensions=None, filters=None, offset=1):
		default_dim = 'ym:s:date,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign,ym:s:UTMContent,ym:s:UTMTerm'
		request_params = {
			'ids': counter_id,
			'metrics': metrics,
			'accuracy': 'full',
			'date1': start_date,
			'date2': end_date,
			'dimensions': dimensions if dimensions is not None else default_dim,
			'filters': filters,
			'offset': offset,
			'limit': 50000
		}
		metrics_list = [re.sub('ym:s:*|ym:pv:*|ym:ad:*|ym:ud:*', '', d) for d in request_params['metrics'].split(',')]
		dimensions_list = [re.sub('ym:s:*|ym:pv:*|ym:ad:*|ym:ud:*', '', d) for d in request_params['dimensions'].split(',')]
		raw_response = self.requestProto('GET', 'api-metrika', 'stat/v1/data', request_params, True)
		if raw_response is not None:
			result = []
			for idx, row in enumerate(raw_response['data']):
				normal_string_dimensions = {v: row['dimensions'][k]['name'] for k, v in enumerate(dimensions_list)}
				normal_string_metrics = {v: row['metrics'][k] for k, v in enumerate(metrics_list)}
				result.append({**normal_string_dimensions, **normal_string_metrics, **{'counter_id': counter_id}})
			if int(raw_response['total_rows']) >= int(request_params['limit'] + request_params['offset'] - 1):
				result += self.getReport(counter_id, metrics, start_date, end_date, dimensions, filters, offset=(len(result) + offset))
			return result
		return None
	def createWordstatReport(self, keywords, regions=None):
		request = {
			'method': 'CreateNewWordstatReport',
			'param': {
				'Phrases': keywords
			}
		}
		if regions is not None:
			request['GeoID'] = regions
		new_report_id = self.directRequestProto('api.direct', 'live/v4/json/', request)
		return new_report_id['data']
	def directRequestProto(self, service, point, params):
		request_url = 'https://{0!s}.yandex.ru/{1!s}'.format(service, point)
		headers = {
			'Content-Type': 'application/json; charset=utf-8'
		}
		params['token'] = self.credentials['access_token']
		request = requests.post(request_url, data=json.dumps(params, ensure_ascii=False).encode('utf8'),
								headers=headers)
		response = json.loads(request.text)
		if request.status_code == 200:
			if 'data' in response.keys():
				return response
			else:
				logging.error('YA: {0!s} : {1!s}'.format(point, request.text))
				return None
		else:
			logging.error('YA: {0!s} : {1!s}'.format(point, request.text))
			return None
	def requestProto(self, http, service, point, params={}, restricted=False):
		if restricted:
			auth_header = {
				'Authorization': 'OAuth {0!s}'.format(self.credentials['access_token'])
			}
		request_url = 'https://{0!s}.yandex.ru/{1!s}'.format(service, point)
		if http == 'POST':
			headers = {
				'Content-Type': 'application/x-www-form-urlencoded'
			}
			if restricted:
				headers = dict(headers, **auth_header)
			request = requests.post(request_url, data=params, headers=headers)
		if http == 'GET':
			if restricted:
				request = requests.get(request_url, params=params, headers=auth_header)
			else:
				request = requests.get(request_url, params=params)

		if request.status_code == 202:
			return request.text
		if request.status_code == 200:
			if point.find('.csv') != -1:
				request.encoding = 'utf-8'
				return request.text
			elif request.text == 'Your query is added to the queue.':
				return request.text
			elif request.text.find('Wait for result.') != -1:
				return request.text
			else:
				return json.loads(request.text)

		else:
			logging.error('YA: {0!s} : {1!s}'.format(point, request.text))
			return None
