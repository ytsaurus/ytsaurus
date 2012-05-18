#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/ytree/public.h>

#include <uv.h>
#include <v8.h>

#include <node.h>
#include <node_buffer.h>

// Do not conflict with util/.
#ifdef STATIC_ASSERT
#undef STATIC_ASSERT
#endif

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

#define CHECK_RETURN_VALUE(expr) \
    do { int rv = (expr); YASSERT(rv == 0 && #expr); } while (0)

#define COMMON_V8_USES \
    using v8::Arguments; \
    using v8::Array; \
    using v8::Boolean; \
    using v8::Local; \
    using v8::Persistent; \
    using v8::Handle; \
    using v8::HandleScope; \
    using v8::Value; \
    using v8::Number;
    using v8::Integer;
    using v8::String; \
    using v8::Object; \
    using v8::Function; \
    using v8::FunctionTemplate; \
    using v8::Undefined; \
    using v8::Null; \
    using v8::Exception; \
    using v8::ThrowException;

#define EXPECT_THAT_IS(value, type) \
    do { \
        if (!(value)->Is##type()) { \
            return ThrowException(Exception::TypeError( \
                String::New("Expected " #value " to be a " #type))); \
        } \
    } while(0)

#define EXPECT_THAT_HAS_INSTANCE(value, type) \
    do { \
        if (!type::HasInstance(value)) { \
            return ThrowException(Exception::TypeError( \
                String::New("Expected " #value " to be an instance of " #type))); \
        } \
    } while(0)

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void DoNothing(uv_work_t* request);

NYTree::INodePtr ConvertV8ValueToYson(v8::Handle<v8::Value> value);
NYTree::INodePtr ConvertV8StringToYson(v8::Handle<v8::String> string);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
