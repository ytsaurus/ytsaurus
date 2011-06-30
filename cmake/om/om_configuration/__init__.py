# -*- coding: utf-8 -*-

from socket import gethostname, gethostbyaddr

from om.exceptions import Error
from om.util import locate_package_data

def version():
    return '1.0.4'

def requires_om():
    return '>=1.0.1'

def configuration_path():
    try:
        return locate_package_data('om_configuration', version(),
                                   gethostbyaddr(gethostname())[0])
    except Error:
        return None
    except KeyError:
        return None
