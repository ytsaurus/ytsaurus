import sys
import logging.config

try:
    import yatest.common

    if yatest.common.context.test_stderr:
        logging.config.dictConfig({
            'version': 1,
            'formatters': {
                'base': {
                    'format': '%(message)s',
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'DEBUG',
                    'formatter': 'base',
                    'stream': sys.stderr,
                },
            },
            'root': {
                'formatter': 'base',
                'level': 'DEBUG',
                'handlers': ['console']
            },
            'devtools.swag.daemon': {
                'formatter': 'base',
                'level': 'INFO',
                'handlers': ['console']
            }
        })
except Exception:
    pass
