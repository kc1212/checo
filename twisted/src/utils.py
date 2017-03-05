def byteify(inp):
    """
    Recursively encode an object from unicode into UTF-8, any object that is not of instance unicode is ignored.
    :param inp: The input object.
    :return: The encoded object.
    """
    if isinstance(inp, dict):
        return {byteify(key): byteify(value) for key, value in inp.iteritems()}
    elif isinstance(inp, list):
        return [byteify(element) for element in inp]
    elif isinstance(inp, unicode):
        return inp.encode('utf-8')
    else:
        return inp


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

