import re


def get_index(environ):
    path = environ.get('PATH_INFO', '')
    matches = re.findall('^/api/(.*)/store/$', path)
    if len(matches) == 1:
        return matches[0]
    else:
        raise AttributeError

