# -*- coding: utf-8 -*-
"""
requests_futures
~~~~~~~~~~~~~~~~

This module provides a small add-on for the requests http library that runs
requests in the background using Python's built-in ``concurrent.futures``.

    from requests_futures.sessions import FuturesSession

    session = FuturesSession()
    # request is run in the background
    future = session.get('http://httpbin.org/get')
    # ... do other stuff ...
    # wait for the request to complete, if it hasn't already
    response = future.result()
    print('response status: {0}'.format(response.status_code))
    print(response.content)

"""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
from functools import partial
from pickle import dumps
from threading import Lock
from warnings import warn

from requests import Session
from requests.adapters import DEFAULT_POOLSIZE, DEFAULT_RETRIES, Retry


def wrap(self, sup, background_callback, *args_, **kwargs_):
    """Runs `sup`, then feeds its response through `background_callback`.

    This has to be a module-level function, rather than a bound method or a
    closure, because it is what actually gets submitted to the executor: a
    bound method isn't picklable, so this would fail as soon as `executor`
    is a :class:`~concurrent.futures.ProcessPoolExecutor`.

    :param self: The `FuturesSession` (or subclass) instance the request was
        made through; passed on to `background_callback`.
    :param sup: The callable that performs the actual HTTP request, e.g.
        ``partial(Session.request, self)``.
    :param background_callback: Called as
        ``background_callback(session, response)``. Deprecated in favor of
        `hooks`.
    :param args_: Positional arguments forwarded to `sup`.
    :param kwargs_: Keyword arguments forwarded to `sup`.
    :returns: `background_callback`'s return value, unless it returns
        `None`, in which case the :class:`~requests.Response` from `sup` is
        returned instead -- so a callback that mutates the response in
        place (e.g. calling ``resp.json()`` to pre-parse it) doesn't need to
        return anything.
    """
    resp = sup(*args_, **kwargs_)
    result = background_callback(self, resp)
    return resp if result is None else result


def _configure_adapters(session, adapter_kwargs):
    """Apply `adapter_kwargs` to the adapters already mounted on `session`.

    Adapters are reconfigured in place, rather than replaced, so that a
    supplied session keeps its retry policy and any custom adapter behavior
    while still picking up the pool sizing.
    """
    for prefix in ('https://', 'http://'):
        adapter = session.get_adapter(prefix)
        if not hasattr(adapter, 'init_poolmanager'):
            # not an HTTPAdapter, pool sizing doesn't apply, leave it alone
            continue
        if 'max_retries' in adapter_kwargs:
            max_retries = adapter_kwargs['max_retries']
            # matches HTTPAdapter.__init__'s own handling
            if max_retries == DEFAULT_RETRIES:
                adapter.max_retries = Retry(0, read=False)
            else:
                adapter.max_retries = Retry.from_int(max_retries)

        pool_connections = adapter_kwargs.get(
            'pool_connections', adapter._pool_connections
        )
        pool_maxsize = adapter_kwargs.get('pool_maxsize', adapter._pool_maxsize)
        pool_block = adapter_kwargs.get('pool_block', adapter._pool_block)
        if (pool_connections, pool_maxsize, pool_block) == (
            adapter._pool_connections,
            adapter._pool_maxsize,
            adapter._pool_block,
        ):
            # nothing to resize, leave the existing pools and their warm
            # connections alone
            continue

        adapter.init_poolmanager(
            pool_connections, pool_maxsize, block=pool_block
        )
        # cached proxy managers were built with the old pool settings and
        # init_poolmanager() doesn't touch them, so drop them to be rebuilt
        # from the new settings on next use
        for proxy_manager in adapter.proxy_manager.values():
            proxy_manager.clear()
        adapter.proxy_manager.clear()


PICKLE_ERROR = (
    'Cannot pickle request. Refer to documentation: https://'
    'github.com/ross/requests-futures/#using-processpoolexecutor'
)


class FuturesSession(Session):
    """A :class:`requests.Session` that runs requests in the background.

    :meth:`request` (and therefore the ``get``/``post``/etc. verb methods) do
    not block: they submit the actual HTTP call to a
    :class:`~concurrent.futures.ThreadPoolExecutor` (the default) or a
    caller-supplied :class:`~concurrent.futures.Executor`, such as a
    :class:`~concurrent.futures.ProcessPoolExecutor`, and immediately return a
    :class:`~concurrent.futures.Future` in place of the usual
    :class:`requests.Response`. Call :meth:`~concurrent.futures.Future.result`
    on it to block for, and retrieve, the response -- or use it with
    :func:`concurrent.futures.as_completed` / :func:`concurrent.futures.wait`
    to work with several in flight requests at once.

    See :doc:`usage` for worked examples, including retries, streaming,
    sharing an executor across sessions, and the ``ProcessPoolExecutor`` case.
    """

    def __init__(
        self,
        executor=None,
        max_workers=8,
        session=None,
        adapter_kwargs=None,
        *args,
        **kwargs,
    ):
        """Creates a FuturesSession.

        :param executor: The executor to submit requests to. Defaults to a
            new :class:`~concurrent.futures.ThreadPoolExecutor` sized by
            `max_workers`, owned by this session -- see :meth:`close`. Pass a
            :class:`~concurrent.futures.ProcessPoolExecutor` to run requests
            in worker processes instead of threads; see :doc:`usage` for what
            that requires (module-global, picklable callables and arguments).
            An executor supplied here, or shared across several
            `FuturesSession` instances, is never shut down by :meth:`close`.
        :type executor: concurrent.futures.Executor, optional
        :param max_workers: Number of worker threads to create for the
            default `ThreadPoolExecutor`. Ignored, along with the connection
            pool resizing described below, whenever `executor` is passed
            explicitly -- size the pool yourself in that case.
        :type max_workers: int, optional
        :param session: An existing :class:`requests.Session` to actually
            issue requests through, instead of this one. Useful for reusing
            an already-configured session (auth, headers, mounted adapters,
            a custom :class:`~requests.adapters.HTTPAdapter` subclass) across
            one or more `FuturesSession` instances that share an `executor`.
            When supplied, :meth:`close` never closes it, and futures are
            submitted directly to `executor` rather than tracked for
            cancellation, since ownership of the session's lifecycle stays
            with the caller.
        :type session: requests.Session, optional
        :param adapter_kwargs: Keyword arguments forwarded to
            :meth:`~requests.adapters.HTTPAdapter.init_poolmanager` --
            typically `pool_connections`, `pool_maxsize`, `pool_block`, and
            `max_retries` -- applied to the adapters already mounted on
            whichever session will actually serve requests (`session`, if
            supplied, otherwise this one). This reconfigures the existing
            adapters in place rather than mounting new ones, so a supplied
            session's retry policy and any custom adapter subclass are
            preserved. When `executor` is not supplied and `max_workers`
            exceeds :data:`requests.adapters.DEFAULT_POOLSIZE`, the pool is
            sized to `max_workers` by default so the worker threads aren't
            throttled by urllib3's default pool size; `adapter_kwargs` is
            merged over that default and applies regardless of whether the
            executor is owned or supplied.
        :type adapter_kwargs: dict, optional
        """
        _adapter_kwargs = {}
        super(FuturesSession, self).__init__(*args, **kwargs)
        self._owned_executor = executor is None
        self._pending_futures = set()
        self._pending_futures_lock = Lock()
        self._close_lock = Lock()
        self._closed = False
        if executor is None:
            executor = ThreadPoolExecutor(max_workers=max_workers)
            # set connection pool size equal to max_workers if needed
            if max_workers > DEFAULT_POOLSIZE:
                _adapter_kwargs.update(
                    {
                        'pool_connections': max_workers,
                        'pool_maxsize': max_workers,
                    }
                )

        _adapter_kwargs.update(adapter_kwargs or {})

        if _adapter_kwargs:
            # configure whichever session will actually serve requests: the
            # supplied one, if any, otherwise self. `self.session` isn't
            # assigned yet, so the `session` argument is used directly here.
            _configure_adapters(session or self, _adapter_kwargs)

        self.executor = executor
        self.session = session

    def request(self, *args, **kwargs):
        """Maintains the existing api for Session.request.

        Used by all of the higher level methods, e.g. Session.get.

        The background_callback param allows you to do some processing on the
        response in the background, e.g. call resp.json() so that json parsing
        happens in the background thread. It is deprecated; use `hooks`
        instead.

        When the executor is a `ProcessPoolExecutor`, the function *and* its
        arguments must all be picklable, since that's what gets sent to the
        worker process; this is verified up front so a bad `hooks` callable
        or file-like `data=` raises `RuntimeError` here rather than a raw
        pickling error out of the returned `Future`.

        This method itself never blocks and never raises for problems with
        the request -- connection errors, timeouts, and bad status codes all
        surface later, from the returned `Future`'s
        :meth:`~concurrent.futures.Future.result`. `RuntimeError` is raised
        directly from here instead, in three cases:

        * the pickling guard above;
        * after :meth:`close` on a session with the default, self-owned
          executor (no `executor=` supplied) -- `close()` shuts that
          executor down, so this method's call to
          :meth:`~concurrent.futures.Executor.submit` raises directly, with
          `concurrent.futures`' own message ("cannot schedule new futures
          after shutdown"). This applies whether or not `session=` was also
          supplied, since `close()` shuts down any executor it owns either
          way;
        * after :meth:`close` on a session built with a supplied
          `executor=` and no `session=` -- the only combination where
          `close()` leaves the executor itself running but tracks pending
          futures and rejects new ones itself, with its own message
          ("cannot schedule new futures after close").

        A session built with both a supplied `executor=` and a supplied
        `session=` is never blocked by `close()` at all: `close()` only
        closes the `FuturesSession`'s own connections in that case, and
        leaves both the executor and the supplied session running.

        :rtype: concurrent.futures.Future
        """
        if self.session:
            func = self.session.request
        else:
            # avoid calling super to not break pickled method
            func = partial(Session.request, self)

        background_callback = kwargs.pop('background_callback', None)
        if background_callback:
            # stacklevel 4: warn -> request -> requests' Session.<verb> ->
            # our <verb> -> caller
            warn(
                '`background_callback` is DEPRECATED. Use `hooks` instead. '
                'Will be removed in 2.0.',
                DeprecationWarning,
                stacklevel=4,
            )
            func = partial(wrap, self, func, background_callback)

        if isinstance(self.executor, ProcessPoolExecutor):
            # verify the whole call can be pickled, not just the function;
            # the executor pickles these same objects when submitting, so
            # anything rejected here would have failed there too, but with
            # a raw error out of future.result() instead of a pointer to
            # the docs. Pickle's failure mode varies by object and python
            # version, so the catch is broad; the original is preserved as
            # __cause__.
            try:
                dumps((func, args, kwargs))
            except Exception as e:
                raise RuntimeError(PICKLE_ERROR) from e

        if self._owned_executor or self.session:
            return self.executor.submit(func, *args, **kwargs)

        with self._pending_futures_lock:
            if self._closed:
                raise RuntimeError('cannot schedule new futures after close')
            future = self.executor.submit(func, *args, **kwargs)
            self._pending_futures.add(future)
        future.add_done_callback(self._remove_pending_future)
        return future

    def _remove_pending_future(self, future):
        with self._pending_futures_lock:
            self._pending_futures.discard(future)

    def close(self):
        with self._close_lock:
            if self._owned_executor:
                self.executor.shutdown(cancel_futures=True)
            elif not self.session:
                with self._pending_futures_lock:
                    self._closed = True
                    pending_futures = tuple(self._pending_futures)
                for future in pending_futures:
                    future.cancel()
                wait(pending_futures)
                super(FuturesSession, self).close()
                return
            super(FuturesSession, self).close()

    def get(self, url, **kwargs):
        r"""
        Sends a GET request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: concurrent.futures.Future
        """
        return super(FuturesSession, self).get(url, **kwargs)

    def options(self, url, **kwargs):
        r"""Sends a OPTIONS request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: concurrent.futures.Future
        """
        return super(FuturesSession, self).options(url, **kwargs)

    def head(self, url, **kwargs):
        r"""Sends a HEAD request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: concurrent.futures.Future
        """
        return super(FuturesSession, self).head(url, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        r"""Sends a POST request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param json: (optional) json to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: concurrent.futures.Future
        """
        return super(FuturesSession, self).post(
            url, data=data, json=json, **kwargs
        )

    def put(self, url, data=None, **kwargs):
        r"""Sends a PUT request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: concurrent.futures.Future
        """
        return super(FuturesSession, self).put(url, data=data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        r"""Sends a PATCH request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param data: (optional) Dictionary, list of tuples, bytes, or file-like
            object to send in the body of the :class:`Request`.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: concurrent.futures.Future
        """
        return super(FuturesSession, self).patch(url, data=data, **kwargs)

    def delete(self, url, **kwargs):
        r"""Sends a DELETE request. Returns :class:`Future` object.

        :param url: URL for the new :class:`Request` object.
        :param \*\*kwargs: Optional arguments that ``request`` takes.
        :rtype: concurrent.futures.Future
        """
        return super(FuturesSession, self).delete(url, **kwargs)
