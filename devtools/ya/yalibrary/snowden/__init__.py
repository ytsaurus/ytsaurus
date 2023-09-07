import uuid
import exts.yjson as json
import logging
import threading
import traceback
from six.moves.urllib.request import urlopen, Request
import six

from exts import retry

from yalibrary import chunked_queue

__all__ = ['push', 'init', 'snowden_sync']


REMOTE_STORE_URL = 'https://back-snowden.n.yandex-team.ru/{}/add'
REMOTE_SHOW_URL = 'https://back-snowden.n.yandex-team.ru/{}/query'
ID = '_id'


logger = logging.getLogger(__name__)
snowden_chunked_queue = None
snowden_shard = None


def init(store_dir, shard):
    global snowden_shard
    snowden_shard = shard
    global snowden_chunked_queue
    snowden_chunked_queue = chunked_queue.ChunkedQueue(store_dir)

    snowden_sync_thread = threading.Thread(target=snowden_sync, name='SnowdenSyncThread')
    snowden_sync_thread.daemon = True
    snowden_sync_thread.start()


def push(value):
    global snowden_chunked_queue
    if snowden_chunked_queue is None:
        return
    if ID not in value:
        value[ID] = uuid.uuid4().hex
    try:
        snowden_chunked_queue.add(value)
    except Exception:
        import traceback
        logger.debug('Error while saving report because of %s', traceback.format_exc())


def snowden_sync():
    try:
        logger.debug('Snowden start cleanup')
        snowden_chunked_queue.cleanup(1000)
        logger.debug('Snowden sync thread started')
        snowden_chunked_queue.consume(consumer)
        logger.debug('Snowden sync thread ended')
    except Exception:
        # noinspection PyBroadException
        try:
            logger.debug(traceback.format_exc())
        except Exception:
            pass


def consumer(chunks):
    assert snowden_shard is not None
    uids = set(x[ID] for x in chunks)
    logger.debug('Sync reports %s', uids)
    response = urlopen(REMOTE_STORE_URL.format(snowden_shard), six.ensure_binary(json.dumps(chunks)), timeout=1)
    saved_uids = set(json.loads(response.read()))
    logger.debug('Synced reports %s', saved_uids)
    if saved_uids != uids:
        raise Exception('Not all uids synced correctly')


@retry.retrying(max_times=10)
def request(tail, data):
    assert snowden_shard is not None
    return urlopen(Request(REMOTE_SHOW_URL.format(snowden_shard) + tail, data)).read()
