/* Copyright (c) 2011-2012 Denis Bilenko. See LICENSE for details. */
#include <stddef.h>
#include "Python.h"
#include "ev.h"
#include "corecext.h"
#include "callbacks.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#endif

#ifdef Py_PYTHON_H

/* define gevent_realloc with libev semantics */
#include "../_ffi/alloc.c"

#if PY_MAJOR_VERSION >= 3
  #define PyInt_FromLong               PyLong_FromLong
#endif


#ifndef CYTHON_INLINE
  #if defined(__clang__)
    #define CYTHON_INLINE __inline__ __attribute__ ((__unused__))
  #elif defined(__GNUC__)
    #define CYTHON_INLINE __inline__
  #elif defined(_MSC_VER)
    #define CYTHON_INLINE __inline
  #elif defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
    #define CYTHON_INLINE inline
  #else
    #define CYTHON_INLINE
  #endif
#endif

#define GGIL_DECLARE  PyGILState_STATE ___save
#define GGIL_ENSURE  ___save = PyGILState_Ensure();
#define GGIL_RELEASE  PyGILState_Release(___save);


static CYTHON_INLINE void gevent_check_signals(struct PyGeventLoopObject* loop)
{
    if (!ev_is_default_loop(loop->_ptr)) {
        /* only reporting signals on the default loop */
        return;
    }
    PyErr_CheckSignals();
    if (PyErr_Occurred()) {
        gevent_handle_error(loop, Py_None);
    }
}

#define GET_OBJECT(PY_TYPE, EV_PTR, MEMBER) \
    ((struct PY_TYPE *)(((char *)EV_PTR) - offsetof(struct PY_TYPE, MEMBER)))


void gevent_noop(struct ev_loop* loop, void* watcher, int revents) {}

static void gevent_stop(PyObject* watcher, struct PyGeventLoopObject* loop)
{
    PyObject *result, *method;
    int error;
    error = 1;
    method = PyObject_GetAttrString(watcher, "stop");
    if (method) {
        result = PyObject_Call(method, _empty_tuple, NULL);
        if (result) {
            Py_DECREF(result);
            error = 0;
        }
        Py_DECREF(method);
    }
    if (error) {
        assert(PyErr_Occurred());
        gevent_handle_error(loop, watcher);
    }
}


static void gevent_callback(struct PyGeventLoopObject* loop, PyObject* callback, PyObject* args, PyObject* watcher, void *c_watcher, int revents)
{
    GGIL_DECLARE;
    PyObject *result, *py_events;
    long length;
    py_events = 0;
    GGIL_ENSURE;
    Py_INCREF(loop);
    Py_INCREF(callback);
    Py_INCREF(args);
    Py_INCREF(watcher);
    gevent_check_signals(loop);
    if (args == Py_None) {
        args = _empty_tuple;
    }
    length = PyTuple_Size(args);
    if (length < 0) {
        /* returns -1 and sets an error if args isn't a tuple. */
        assert(PyErr_Occurred());
        gevent_handle_error(loop, watcher);
        goto end;
    }
    if (length > 0 && PyTuple_GET_ITEM(args, 0) == GEVENT_CORE_EVENTS) {
        py_events = PyInt_FromLong(revents);
        if (!py_events) {
            gevent_handle_error(loop, watcher);
            goto end;
        }
        PyTuple_SET_ITEM(args, 0, py_events);
    }
    else {
        py_events = NULL;
    }
    result = PyObject_Call(callback, args, NULL);
    if (result) {
        Py_DECREF(result);
    }
    else {
        assert(PyErr_Occurred());
        gevent_handle_error(loop, watcher);
        if (revents & (EV_READ|EV_WRITE)) {
            /* io watcher: not stopping it may cause the failing callback to be called repeatedly */
            gevent_stop(watcher, loop);
            goto end;
        }
    }
    if (!ev_is_active(c_watcher)) {
        /* Watcher was stopped, maybe by libev. Let's call stop() to clean up
         * 'callback' and 'args' properties, do Py_DECREF() and ev_ref() if necessary.
         * BTW, we don't need to check for EV_ERROR, because libev stops the watcher in that case. */
        gevent_stop(watcher, loop);
    }
end:
    if (py_events) {
        Py_DECREF(py_events);
        PyTuple_SET_ITEM(args, 0, GEVENT_CORE_EVENTS);
    }
    Py_DECREF(watcher);
    Py_DECREF(args);
    Py_DECREF(callback);
    Py_DECREF(loop);
    GGIL_RELEASE;
}


void gevent_call(struct PyGeventLoopObject* loop, struct PyGeventCallbackObject* cb)
{
    /* no need for GIL here because it is only called from run_callbacks which already has GIL */
    PyObject *result, *callback, *args;
    if (!loop || !cb)
        return;
    callback = cb->callback;
    args = cb->args;
    if (!callback || !args)
        return;
    if (callback == Py_None || args == Py_None)
        return;
    Py_INCREF(loop);
    Py_INCREF(callback);
    Py_INCREF(args);

    Py_INCREF(Py_None);
    Py_DECREF(cb->callback);
    cb->callback = Py_None;
    /*
      you are not allowed to use PyObject_Call() with an error pending;
      debug builds crash with an assertion.
      How do we get here with an error pending? good question. It's been
      seen in 3.12 before we stopped using CYTHON_FAST_THREAD_STATE.
      Hopefully changing that makes this dead code.
    */
    if (PyErr_Occurred()) {
        /* by now, cb->callback is None, so using it as the context doesn't
           produce a useful output. The callable object is more helpful.
           Writing unraisable clears the error, unless it gets an error of
           its own.
        */
        PyErr_WriteUnraisable(callback);
        PyErr_Clear();
    }
    assert(!PyErr_Occurred());
    result = PyObject_Call(callback, args, NULL);
    if (result) {
        Py_DECREF(result);
    }
    else {
        assert(PyErr_Occurred());
        gevent_handle_error(loop, (PyObject*)cb);
    }

    Py_INCREF(Py_None);
    Py_DECREF(cb->args);
    cb->args = Py_None;

    Py_DECREF(callback);
    Py_DECREF(args);
    Py_DECREF(loop);
}

/*
 * PyGeventWatcherObject is the first member of all the structs, so
 * it is the same in all of them and they can all safely be cast to
 * it. We could also use the *data member of the libev watcher objects.
 */

#undef DEFINE_CALLBACK
#define DEFINE_CALLBACK(WATCHER_LC, WATCHER_TYPE) \
    void gevent_callback_##WATCHER_LC(struct ev_loop *_loop, void *c_watcher, int revents) {                  \
        struct PyGeventWatcherObject* watcher = (struct PyGeventWatcherObject*)GET_OBJECT(PyGevent##WATCHER_TYPE##Object, c_watcher, _watcher);    \
        gevent_callback(watcher->loop, watcher->_callback, watcher->args, (PyObject*)watcher, c_watcher, revents); \
    }


DEFINE_CALLBACKS


void gevent_run_callbacks(struct ev_loop *_loop, void *watcher, int revents) {
    struct PyGeventLoopObject* loop;
    PyObject *result;
    GGIL_DECLARE;
    GGIL_ENSURE;
    loop = GET_OBJECT(PyGeventLoopObject, watcher, _prepare);
    Py_INCREF(loop);
    gevent_check_signals(loop);
    result = gevent_loop_run_callbacks(loop);
    if (result) {
        Py_DECREF(result);
    }
    else {
        /*
         For reasons that are unclear, PyErr_WriteUnraisable, which invokes
         sys.unraisablehook, isn't safe to call here, at least in some cases.
         test__pool.TestCoroutinePool.test_stderr_raising fails its timeout
         if we use that. Instead, we use PyErr_Print, which doesn't have any
         context, but doesn't hang either. It calls sys.excepthook.
         */

        PyErr_Print();
        PyErr_Clear();
    }
    Py_DECREF(loop);
    GGIL_RELEASE;
}

/* This is only used on Win32 */

void gevent_periodic_signal_check(struct ev_loop *_loop, void *watcher, int revents) {
    GGIL_DECLARE;
    GGIL_ENSURE;
    gevent_check_signals(GET_OBJECT(PyGeventLoopObject, watcher, _periodic_signal_checker));
    GGIL_RELEASE;
}

#undef GGIL_DECLARE
#undef GGIL_ENSURE
#undef GGIL_RELEASE

#endif  /* Py_PYTHON_H */

#ifdef __clang__
#pragma clang diagnostic pop
#endif
