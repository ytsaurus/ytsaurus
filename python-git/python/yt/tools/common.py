import copy

import yt.logger as logger

def update_args(args, object):
    result = copy.deepcopy(args)
    for key in object:
        if key in vars(result):
            vars(result)[key] = object[key]
        else:
            logger.warning("Key '%s' ignored", key)
    return result


