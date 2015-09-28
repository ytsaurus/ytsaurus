try:
    from simplejson import *
except ImportError:
    # These version of simplejson has no compliled speedup module.
    from yt.packages.simplejson import *

