from elasticsearch_raven.transport import ElasticsearchTransport


def application(environ, start_response):
    response_body = 'Hello World'
    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain'),
                        ('Content-Length', str(len(response_body)))]

    transport = ElasticsearchTransport(
        'elasticsearch://127.0.0.1:9200/django-log-{0:%Y.%m.%d}/1')
    transport.send(environ['wsgi.input'].read())

    start_response(status, response_headers)
    return [response_body.encode('utf-8')]
