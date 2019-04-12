# -*- coding: utf-8 -*-

import re
import logging
import httplib2
from datetime import datetime, timedelta
from googleapiclient import discovery
from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError

class AnalyticsClient:
	def __init__(self, local = False, credentials = 'client_secrets.json', scopes = ['https://www.googleapis.com/auth/analytics.readonly']):
		logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
		if local:
			from oauth2client.service_account import ServiceAccountCredentials
			self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
				credentials, 
				scopes = scopes
			)
		else:
			assert type(credentials) == dict
			import google.oauth2.credentials
			self.credentials = google.oauth2.credentials.Credentials(**credentials)
		if not self.credentials.valid:
			self.credentials.refresh(httplib2.Http())
			logging.warning('GA: CREDENTIALS REFRESH: {0!s}'.format(self.credentials.client_id))
		self.service = discovery.build('analytics', 'v3', credentials = self.credentials, cache_discovery=False)
	def returnCredentials(self):
		return {
			'token': self.credentials.token,
			'token_uri': self.credentials.token_uri,
			'refresh_token': self.credentials.refresh_token,
			'id_token': self.credentials.id_token,
			'client_id': self.credentials.client_id,
			'client_secret': self.credentials.client_secret,
			'scopes': self.credentials.scopes
		}
	def returnAccountsTree(self):
		return self.service.management().accountSummaries().list().execute().get('items', [])
	def returnParentByProfile(self, profile):
		tree = self.returnAccountsTree()
		account_summary = [a for a in tree if profile in str(tree)]
		if len(account_summary) < 1:
			return []
		account_summary = account_summary[0]
		for property_tree in account_summary.get('webProperties'):
			if profile in str(property_tree):
				for profile_tree in property_tree['profiles']:
					if profile in str(profile):
						property_tree['profiles'] = profile_tree
						break
				account_summary['webProperties'] = [property_tree]
				break
		return account_summary
	def returnGoalsList(self, account, property, profile):
		return self.service.management().goals().list(
			accountId=account,
			webPropertyId=property,
			profileId=profile
		).execute().get('items', [])
	def executeQuery(self, profile, metrics, dimensions, date_start, date_end, filters=None, normalize=False):
		anormal_results = []
		normal_results = []
		dim_list = re.sub('ga:*', '', dimensions).split(',')
		metrics_list = re.sub('ga:*', '', metrics).split(',')
		try:
			limit = self.queryV3(self.service, profile, 0, date_start, date_end, metrics, dimensions, filters)
			if int(limit.get('totalResults', 0)) < 10000:
				anormal_results.append(limit)
			else:
				for pag_index in range(0, int(limit.get('totalResults', 0)), 10000):
					results = self.queryV3(
						self.service, 
						profile, 
						pag_index, 
						date_start, date_end, 
						metrics, dimensions, filters
					)
					anormal_results.append(results)
		except TypeError as error:
			logging.error('GA : proto : Query error : {0!s}'.format(error))
			return [{'error': 'Query error', 'error_msg': str(error)}]
		except HttpError as error:
			logging.error('GA : proto : API error : {0!s} : {1!s}'.format(error.resp.status, error._get_reason()))
			return [{'error': error.resp.status, 'error_msg': error._get_reason()}]
		except AccessTokenRefreshError:
			logging.error('GA : proto : Credentials revoked or expired')
			return [{'error': 'Credentials', 'error_msg': 'Credentials revoked or expired'}]
		if normalize:
			for i in anormal_results:
				for j in i.get('rows', []):
					normal_string_dimensions = {v: j[k] if len(j[k]) > 0 else '(none)' for k, v in enumerate(dim_list)}
					normal_string_metrics = {v: j[k + len(dim_list)] if len(j[k + len(dim_list)]) > 0 else 0 for k, v in
											 enumerate(metrics_list)}
					normal_results.append({**normal_string_dimensions, **normal_string_metrics, **{'profile': profile}})
			return normal_results
		else:
			return anormal_results
	def fetchReportV3(self, profile, metrics, dimensions, date_start, date_end, filters=None, normalize=False):
		date_range = [
			(datetime.strptime(date_start, '%Y-%m-%d').date() + timedelta(days=i)).strftime('%Y-%m-%d') for i in
					  range((datetime.strptime(date_end, '%Y-%m-%d').date() - datetime.strptime(date_start, '%Y-%m-%d').date()).days + 1)]
		result = []
		for d in date_range:
			result += self.executeQuery(profile, metrics, dimensions, d, d, filters, normalize)
		return result

	def fetchReportV4(self, profile, dateRange, metrics, dimensions, metricsFilterings=[], dimensionsFilterings=[], pageSize=10000, pageToken=0):
		self.serviceV4 = discovery.build('analyticsreporting', 'v4', credentials=self.credentials)
		query_body = {
			'reportRequests': []
		}
		for index, report in enumerate(metrics):
			drDefinition = [dr if type(dr) == dict else {'startDate': dr[0], 'endDate': dr[1] if len(dr) > 0 else dr[0]}
							for idx, dr in dateRange]
			query_body['reportRequests'].append({
				'viewId': profile,
				'dateRanges': drDefinition,
				'metrics': metrics[index] if type(metrics[index][0]) == dict else [{'expression': s} for s in
																				   metrics[index]],
				'dimensions': dimensions[index] if type(dimensions[index][0]) == dict else [{'name': s} for s in
																							dimensions[index]],
				'samplingLevel': 'LARGE',
				'pageSize': pageSize,
				'pageToken': str(pageToken)
			})
			if len(metricsFilterings) and len(metricsFilterings[index]) > 0:
				query_body['reportRequests'][index]['metricFilterClauses'] = metricsFilterings[index]
			if len(dimensionsFilterings) and len(dimensionsFilterings[index]) > 0:
				query_body['reportRequests'][index]['metricFilterClauses'] = dimensionsFilterings[index]
		touch_query = self.serviceV4.reports().batchGet(body=query_body).execute()
		normalized_reports = []
		for index, report in enumerate(touch_query['reports']):
			report_keys = report['data'].keys()
			is_sampled = 'samplesReadCounts' in report_keys and 'samplingSpaceSizes' in report_keys
			if is_sampled:
				normalized_report = []
				logging.warning('GA: {0!s} : Sampling detected. Reducing...'.format(profile))
				reducedDateRanges = [
					{
						'startDate': s,
						'endDate': s
					} for s in
					[(datetime.strptime(drDefinition[0]['startDate'], '%Y-%m-%d').date() + timedelta(days=i)).strftime('%Y-%m-%d')
					 for i in range((datetime.strptime(drDefinition[0]['endDate'], '%Y-%m-%d').date() - datetime.strptime(drDefinition[0]['startDate'], '%Y-%m-%d').date()).days + 1)]
				]
				datesIterator = iter(reducedDateRanges)
				for d in datesIterator:
					try:
						allowed_range = [d, next(dates_iterator)]
					except:
						allowed_range = [d]
					query_body['reportRequests'][index]['dateRanges'] = allowed_range
					chunk_query = {'reportRequests': [query_body['reportRequests'][index]]}
					chunk = self.serviceV4.reports().batchGet(body=chunk_query).execute()
					for r in chunk['reports']:
						if 'nextPageToken' in r.keys():
							normalized_report = self.normalizeReport(r, profile) + self.fetchReportV4(
								profile, [allowed_range],
								metrics, dimensions,
								metricsFilterings, dimensionsFilterings,
								pageToken=int(r.get('nextPageToken'))
							)[index]
						else:
							normalized_report += self.normalizeReport(r, profile)
				normalized_reports.append(normalized_report)
			else:
				if 'nextPageToken' in report.keys():
					normalized_reports.append(
						self.normalizeReport(report, profile) +
						self.fetchReportV4(
							profile, dateRange, 
							metrics, dimensions,
							metricsFilterings, dimensionsFilterings,
							pageToken=int(report.get('nextPageToken'))
						)[index]
					)
				else:
					normalized_reports.append(self.normalizeReport(report, profile))
		return normalized_reports

	@staticmethod
	def normalizeReport(report, profile):
		result = []
		metrics_list = [re.sub('ga:*', '', m['name']) for m in
						report['columnHeader']['metricHeader']['metricHeaderEntries']]
		dimensions_list = [re.sub('ga:*', '', d) for d in report['columnHeader']['dimensions'] if d != 'ga:segment']
		if not len(report['data']['rows']):
			normal_string_dimensions = {v: '' for k, v in enumerate(dimensions_list)}
			normal_string_metrics = {v: 0 for k, v in enumerate(metrics_list)}
			result.append({**normal_string_dimensions, **normal_string_metrics, **{'profile': profile}})
		for row in report['data']['rows']:
			normal_string_dimensions = {v: row['dimensions'][k] for k, v in enumerate(dimensions_list)}
			normal_string_metrics = {v: row['metrics'][0]['values'][k] for k, v in enumerate(metrics_list)}
			result.append({**normal_string_dimensions, **normal_string_metrics, **{'profile': profile}})
		return result

	@staticmethod
	def queryV3(service, profile_id, pag_index, start_date, end_date, metrics, dimensions, filters):
		return service.data().ga().get(
			ids = 'ga:' + str(profile_id),
			start_date = start_date,
			end_date = end_date,
			metrics = metrics,
			filters = filters,
			dimensions = dimensions,
			samplingLevel = 'HIGHER_PRECISION',
			start_index = str(pag_index + 1),
			max_results = str(pag_index + 10000)
		).execute()
