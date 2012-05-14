#pragma once

#include <uv.h>
#include <v8.h>

#include <node.h>
#include <node_buffer.h>

#include <pthread.h>
#include <string.h>
#include <stdlib.h>

#define TRACE_CURRENT_THREAD(marker) \
    (fprintf(stderr, "=== " marker " Thread 0x%012lx: %s\n", (size_t)pthread_self(), __PRETTY_FUNCTION__))

#if 0
#define T_THREAD_AFFINITY_IS_V8() TRACE_CURRENT_THREAD("V8")
#define T_THREAD_AFFINITY_IS_UV() TRACE_CURRENT_THREAD("UV")
#else
#define T_THREAD_AFFINITY_IS_V8()
#define T_THREAD_AFFINITY_IS_UV()
#endif

#define THREAD_AFFINITY_IS_V8()
#define THREAD_AFFINITY_IS_UV()
#define THREAD_AFFINITY_IS_ANY()

#define PRISZT "lu"

#define CHECK_RETURN_VALUE(expr) \
    do { int rv = (expr); assert(rv == 0 && #expr); } while (0)

#define COMMON_V8_USES \
    using v8::Arguments; \
    using v8::Local; \
    using v8::Persistent; \
    using v8::Handle; \
    using v8::HandleScope; \
    using v8::Value; \
    using v8::Integer;
    using v8::String; \
    using v8::Object; \
    using v8::Function; \
    using v8::FunctionTemplate; \
    using v8::Undefined; \
    using v8::Null; \


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 * Common guidelines for developing a V8-shared object.
 *
 *   - Don't use mutexes while working in V8 thread.
 *   - V8-side functions _have to be_ fast.
 */
class TGuard {
public:
    TGuard(pthread_mutex_t* mutex, bool acquire = true)
        : Mutex(mutex)
    {
        if (acquire) {
            pthread_mutex_lock(Mutex);
        }
    }

    ~TGuard()
    {
        pthread_mutex_unlock(Mutex);
    }

private:
    pthread_mutex_t* Mutex;
};

////////////////////////////////////////////////////////////////////////////////

void DoNothing(uv_work_t*);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
