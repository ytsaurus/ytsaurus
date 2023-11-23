import logging
import os
from spyt.testing.common.cluster import SpytCluster
import spyt.testing.common.cypress as cypress_helpers
from spyt.testing.common.helpers import get_python_path, get_java_home
import yt.wrapper

logger = logging.getLogger(__name__)


class SpytPublicTestBase:
    PYTHON_PATH = get_python_path()
    JAVA_HOME = get_java_home()
    YT = None

    @classmethod
    def get_proxy_address(cls):
        return os.environ["YT_PROXY"]

    @classmethod
    def _create_yt_client(cls):
        yt.wrapper.config["proxy"]["enable_proxy_discovery"] = False
        cls.YT = yt.wrapper.YtClient(proxy=cls.get_proxy_address())
        logger.debug("YT client created")

    @classmethod
    def setup_class(cls):
        if not cls.YT:
            cls._create_yt_client()
        cypress_helpers.quick_setup(cls.YT, cls.PYTHON_PATH, cls.JAVA_HOME)

    def spyt_cluster(self):
        return SpytCluster(proxy=self.get_proxy_address(), java_home=self.JAVA_HOME)
