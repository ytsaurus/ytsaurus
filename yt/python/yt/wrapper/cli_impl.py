import yt.wrapper as yt
import yt.logger as logger


def _set_attribute(path, name, value, recursive, client=None):
    """ Sets attribute at given path
    """
    if recursive:
        batch_client = yt.create_batch_client(client=client)

        processed_nodes = []
        for obj in yt.search(path, attributes=[name], enable_batch_mode=True, client=client):
            if name not in obj.attributes or obj.attributes[name] != value:
                set_result = batch_client.set("{}&/@{}".format(str(obj), name), value)
                processed_nodes.append((obj, set_result))

        batch_client.commit_batch()

        for obj, set_result in processed_nodes:
            if set_result.get_error() is not None:
                error = yt.YtResponseError(set_result.get_error())
                if not error.is_resolve_error():
                    logger.warning("Cannot set attribute on path '%s' (error: %s)", str(obj), repr(error))
            else:
                logger.info("Attribute '%s' is set to '%s' on path '%s'", name, value, str(obj))
    else:
        yt.set_attribute(path, name, value)


def _remove_attribute(path, name, recursive, client=None):
    """ Removes attribute at given path
    """
    if recursive:
        batch_client = yt.create_batch_client(client=client)

        processed_nodes = []
        for obj in yt.search(path, attributes=[name], enable_batch_mode=True, client=client):
            if name in obj.attributes:
                remove_result = batch_client.remove("{}&/@{}".format(str(obj), name))
                processed_nodes.append((obj, remove_result))

        batch_client.commit_batch()

        for obj, remove_result in processed_nodes:
            if remove_result.get_error() is not None:
                error = yt.YtResponseError(remove_result.get_error())
                if not error.is_resolve_error():
                    logger.warning("Cannot remove attribute on path '%s' (error: %s)", str(obj), repr(error))
            else:
                logger.info("Attribute '%s' is removed on path '%s'", name, str(obj))
    else:
        yt.remove_attribute(path, name)
