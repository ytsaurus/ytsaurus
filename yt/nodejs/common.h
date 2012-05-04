#pragma once

#include <uv.h>
#include <v8.h>

#include <pthread.h>
#include <string.h>
#include <stdlib.h>

#define TRACE_CURRENT_THREAD() \
    (fprintf(stderr, "=== Thread 0x%012lx: %s\n", (size_t)pthread_self(), __PRETTY_FUNCTION__))

#define CHECK_RETURN_VALUE(expr) \
    do { int rv = (expr); assert(rv == 0 && #expr); } while (0)

#define COMMON_V8_USES \
    using v8::Arguments; \
    using v8::Local; \
    using v8::Persistent; \
    using v8::Handle; \
    using v8::HandleScope; \
    using v8::Value; \
    using v8::String; \
    using v8::Object; \
    using v8::FunctionTemplate; \
    using v8::Undefined; \
    using v8::Null; \


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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
