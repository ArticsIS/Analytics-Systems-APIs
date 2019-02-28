from systems import AnalyticsClient, YandexClient

if __name__ == '__main__':
	config_file_path = '/config/yandex.json'
	# config content example
	# {
	#	"app_name": "YandexAPIApp",
	#	"client_id": "aabbcc",
	#	"client_secret": "ddeeff",
	#	"callback_url": "https://my.callback.ru/oauth2/yandex"
	# }
	
	auth_data = {
		'access_token': 'aqwert123',
		'refresh_token': 'zxcvxvc123r43',
		'expired_at': '2019-02-02 13:00:38',
		'expires_in': '15552000'
	}

	client = YandexClient(config_file_path, auth_data)
	wordstat_report_id = client.createWordstatReport(['api', 'bad'])
	print(client.getWordstatReport(wordstat_report_id))