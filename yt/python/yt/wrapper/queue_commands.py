from .driver import make_request, make_formatted_request
from .common import set_param
from .ypath import TablePath


def register_queue_consumer(queue_path, consumer_path, vital, partitions=None, client=None):
    """Register queue consumer.

    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param bool vital: vital.
    """

    params = {}
    set_param(params, "queue_path", queue_path, lambda path: TablePath(path, client=client))
    set_param(params, "consumer_path", consumer_path, lambda path: TablePath(path, client=client))
    set_param(params, "vital", vital)
    set_param(params, "partitions", partitions)

    return make_request("register_queue_consumer", params, client=client)


def unregister_queue_consumer(queue_path, consumer_path, client=None):
    """Unregister queue consumer.

    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    """

    params = {
        "queue_path": TablePath(queue_path, client=client),
        "consumer_path": TablePath(consumer_path, client=client),
    }

    return make_request("unregister_queue_consumer", params, client=client)


def list_queue_consumer_registrations(queue_path=None, consumer_path=None, format=None, client=None):
    """List queue consumer registrations.

    :param queue_path: path to queue table.
    :type queue_path: None or str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param consumer_path: path to consumer table.
    :type consumer_path: None or str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    """

    params = {}
    set_param(params, "queue_path", queue_path, lambda path: TablePath(path, client=client))
    set_param(params, "consumer_path", consumer_path, lambda path: TablePath(path, client=client))

    res = make_formatted_request("list_queue_consumer_registrations", params, format, client=client)
    return res
