class InvalidConfigException(Exception):
    pass

class IsHeaderException(Exception):
    '''Exception to be thrown when a line matches headers

    Headers shouldn't be treated and loaded as data, so when we
    find exact matches, raise this error which can then be
    handled further down in the pipeline
    '''
    pass
