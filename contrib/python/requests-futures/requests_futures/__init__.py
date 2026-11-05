# -*- coding: utf-8 -*-

# Requests Futures

"""
async requests HTTP library
~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

import logging
from logging import NullHandler

__title__ = 'requests-futures'
__version__ = '1.1.0'
__author__ = 'Ross McFarland'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2013 Ross McFarland'

# Set default logging handler to avoid "No handler found" warnings.
logging.getLogger(__name__).addHandler(NullHandler())
