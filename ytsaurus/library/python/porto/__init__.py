"""Porto python API

Example:

import porto

api = porto.Connection()
container = api.Run("test", command="sleep 5")
container.Wait()
print container['status']
container.Destroy()

"""

from .api import PortoApi as Connection  # noqa
