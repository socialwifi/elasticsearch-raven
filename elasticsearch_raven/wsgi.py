from elasticsearch_raven.http import HttpUtils

http_utils = HttpUtils()

http_utils.start_sender()
application = http_utils.get_application()
