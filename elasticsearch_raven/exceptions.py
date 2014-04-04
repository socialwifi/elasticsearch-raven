
class ElasticsearchRavenError(Exception):
    pass


class DamagedSentryMessageError(ElasticsearchRavenError):
    pass


class BadSentryMessageHeaderError(ElasticsearchRavenError):
    pass


class DamagedSentryMessageBodyError(DamagedSentryMessageError):
    pass
