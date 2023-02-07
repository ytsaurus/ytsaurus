from yatest.common.network import PortManager
import yql_utils

port_manager = PortManager()


def get_yql_port(service='unknown'):
    port = port_manager.get_port()
    yql_utils.log('get port for service %s: %d' % (service, port))
    return port
