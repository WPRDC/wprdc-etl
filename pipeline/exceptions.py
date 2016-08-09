class InvalidConfigException(Exception):
    pass

class IsHeaderException(Exception):
    '''Thrown when a line matches headers

    Headers shouldn't be treated and loaded as data, so when we
    find exact matches, raise this error which can then be
    handled further down in the pipeline
    '''
    pass

class CKANException(Exception):
    '''Thrown when a non-success status is received from CKAN
    '''

class HTTPConnectorError(Exception):
    pass

class DuplicateFileException(Exception):
    '''Thrown when two checksums match
    '''

class InvalidPipelineError(Exception):
    pass

class MissingStatusDatabaseError(Exception):
    pass