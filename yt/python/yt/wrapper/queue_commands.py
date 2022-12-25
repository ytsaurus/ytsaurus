from .driver import make_request
from .common import set_param
from .ypath import TablePath


def register_queue_consumer(queue_path, consumer_path, vital, client=None):
    """Register queue consumer.

    :param queue_path: path to queue table.
    :type queue_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param consumer_path: path to consumer table.
    :type consumer_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param bool vital: vital.
    """

    params = {
        "queue_path": TablePath(queue_path, client=client),
        "consumer_path": TablePath(consumer_path, client=client),
    }

    set_param(params, "vital", vital)

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
