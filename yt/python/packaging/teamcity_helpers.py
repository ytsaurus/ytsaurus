import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../scripts/teamcity-build/python"))

import helpers  # noqa
from teamcity import teamcity_message  # noqa
