import logging
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class SpytEnablers(object):
    def __init__(self, enable_byop=True, enable_profiling=False, enable_arrow=None,
                 enable_mtn=False, enable_solomon_agent=True, enable_preference_ipv6=True):
        self.enable_byop = enable_byop
        self.enable_profiling = enable_profiling
        self.enable_arrow = enable_byop if enable_arrow is None else enable_arrow
        self.enable_mtn = enable_mtn
        self.enable_solomon_agent = enable_solomon_agent
        self.enable_preference_ipv6 = enable_preference_ipv6
        self.config_enablers = {}

    def _get_enabler(self, enabler, enabler_name):
        config_enabler = self.config_enablers.get(enabler_name) or False
        if enabler and not config_enabler:
            logger.warning("{} is enabled in start arguments, but disabled for current cluster version. "
                           "It will be disabled".format(enabler_name))
        return enabler and config_enabler

    def apply_config(self, config):
        self.config_enablers = config.get("enablers") or {}
        self.enable_byop = self._get_enabler(self.enable_byop, "enable_byop")
        self.enable_arrow = self._get_enabler(self.enable_arrow, "enable_arrow")
        self.enable_mtn = self._get_enabler(self.enable_mtn, "enable_mtn")
        self.enable_solomon_agent = self._get_enabler(self.enable_solomon_agent, "enable_solomon_agent")
        self.enable_preference_ipv6 = self._get_enabler(self.enable_preference_ipv6, "enable_preference_ipv6")

    def get_spark_conf(self):
        enable_byop_conf_name = "spark.hadoop.yt.byop.enabled"
        enable_arrow_conf_name = "spark.hadoop.yt.read.arrow.enabled"
        enable_profiling_conf_name = "spark.hadoop.yt.profiling.enabled"
        enable_mtn_conf_name = "spark.hadoop.yt.mtn.enabled"
        enable_solomon_agent_name = "spark.hadoop.yt.solomonAgent.enabled"
        enable_preference_ipv6_name = "spark.hadoop.yt.preferenceIpv6.enabled"
        enablers = [
            enable_byop_conf_name, enable_profiling_conf_name, enable_arrow_conf_name,
            enable_mtn_conf_name, enable_solomon_agent_name, enable_preference_ipv6_name
        ]
        return {
            enable_byop_conf_name: str(self.enable_byop),
            enable_profiling_conf_name: str(self.enable_profiling),
            enable_arrow_conf_name: str(self.enable_arrow),
            enable_mtn_conf_name: str(self.enable_mtn),
            enable_solomon_agent_name: str(self.enable_solomon_agent),
            enable_preference_ipv6_name: str(self.enable_preference_ipv6),
            "spark.yt.enablers": ",".join(enablers)
        }


SPARK_DEFAULTS = SpytEnablers().get_spark_conf()

def safe_get(d, key):
    if d is None:
        return None
    return d.get(key)

def set_enablers(spark_conf, spark_conf_args, spark_cluster_conf, enablers):
    for name in enablers:
        env_name = name.upper().replace(".", "_")
        enable_app = safe_get(spark_conf_args, name) or safe_get(spark_conf, name) or os.getenv(env_name) or SPARK_DEFAULTS.get(name) or "false"
        enable_cluster = safe_get(spark_cluster_conf, name) or "false"
        enable = ((enable_app.lower() == "true") and (enable_cluster.lower() == "true"))
        spark_conf.set(name, str(enable))

def set_except_enablers(spark_conf, conf_patch, enablers):
    if conf_patch is None:
        return
    for k, v in conf_patch.items():
        if k not in enablers:
            spark_conf.set(k, v)

def parse_enablers(conf):
    return [x.strip() for x in conf.get("spark.yt.enablers", "").split(",") if len(x.strip()) > 0]

def get_enablers_list(spark_cluster_conf):
    return parse_enablers(spark_cluster_conf) + parse_enablers(SPARK_DEFAULTS)
