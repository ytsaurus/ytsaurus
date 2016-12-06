#pragma once

#include <yt/core/misc/error.h>
#include <yt/core/misc/ref_tracked.h>

#define BUILDING_NODE_EXTENSION

#include <uv.h>
#include <v8.h>

#include <node.h>
#include <node_buffer.h>

// Do not conflict with util/.
#ifdef STATIC_ASSERT
#undef STATIC_ASSERT
#endif

#ifndef offset_of
#define offset_of(type, member) \
    ((intptr_t) ((char *) &(((type *) 8)->member) - 8))
#endif

#ifndef container_of
#define container_of(ptr, type, member) \
    ((type *) ((char *) (ptr) - offset_of(type, member)))
#endif

#ifndef ROUND_UP
#define ROUND_UP(a, b) ((a) % (b) ? ((a) + (b)) - ((a) % (b)) : (a))
#endif

#include <string.h>
#include <stdlib.h>

#define TRACE_CURRENT_THREAD(marker) \
    (fprintf(stderr, "=== " marker " Thread 0x%012lx: %s\n", (size_t)pthread_self(), __PRETTY_FUNCTION__))

#define THREAD_AFFINITY_IS_V8()
#define THREAD_AFFINITY_IS_UV()
#define THREAD_AFFINITY_IS_ANY()

#define COMMON_V8_USES \
    using v8::Arguments; \
    using v8::Array; \
    using v8::Boolean; \
    using v8::Context; \
    using v8::Exception; \
    using v8::Function; \
    using v8::FunctionTemplate; \
    using v8::Handle; \
    using v8::HandleScope; \
    using v8::Integer; \
    using v8::Local; \
    using v8::Number; \
    using v8::Object; \
    using v8::Persistent; \
    using v8::String; \
    using v8::ThrowException; \
    using v8::TryCatch; \
    using v8::Undefined; \
    using v8::Value; \
    /**/

#define EXPECT_THAT_IS(value, type) \
    do { \
        if (!(value)->Is##type()) { \
            return ThrowException(Exception::TypeError(String::Concat( \
                String::New(__PRETTY_FUNCTION__), \
                String::New(": Expected " #value " to be a " #type)))); \
        } \
    } while (0)

#define EXPECT_THAT_HAS_INSTANCE(value, type) \
    do { \
        if (!type::HasInstance(value)) { \
            return ThrowException(Exception::TypeError(String::Concat( \
                String::New(__PRETTY_FUNCTION__), \
                String::New(": Expected " #value " to be an instance of " #type)))); \
        } \
    } while (0)

#if 0
#define EIO_PUSH(callback, data) \
    eio_grp((callback), (data), &uv_default_loop()->uv_eio_channel);
#else
#define EIO_PUSH(callback, data) \
    eio_grp((callback), (data));
#endif

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

static const size_t DefaultStreamBufferSize = 1 << 15;

DEFINE_ENUM(ECompression,
    (None)
    (Gzip)
    (Deflate)
    (LZOP)
    (LZO)
    (LZF)
    (Snappy)
    (Brotli)
);

////////////////////////////////////////////////////////////////////////////////

// Assuming presence of outer v8::HandleScope.
inline void Invoke(
    const v8::Handle<v8::Function>& callback)
{
    node::MakeCallback(v8::Object::New(), callback, 0, nullptr);
}

// Assuming presence of outer v8::HandleScope.
inline void Invoke(
    const v8::Handle<v8::Function>& callback,
    const v8::Handle<v8::Value>& a1)
{
    v8::Handle<v8::Value> args[] = {a1};
    node::MakeCallback(v8::Object::New(), callback, Y_ARRAY_SIZE(args), args);
}

// Assuming presence of outer v8::HandleScope.
inline void Invoke(
    const v8::Handle<v8::Function>& callback,
    const v8::Handle<v8::Value>& a1,
    const v8::Handle<v8::Value>& a2)
{
    v8::Handle<v8::Value> args[] = {a1, a2};
    node::MakeCallback(v8::Object::New(), callback, Y_ARRAY_SIZE(args), args);
}

// Assuming presence of outer v8::HandleScope.
inline void Invoke(
    const v8::Handle<v8::Function>& callback,
    const v8::Handle<v8::Value>& a1,
    const v8::Handle<v8::Value>& a2,
    const v8::Handle<v8::Value>& a3)
{
    v8::Handle<v8::Value> args[] = {a1, a2, a3};
    node::MakeCallback(v8::Object::New(), callback, Y_ARRAY_SIZE(args), args);
}

void InitializeCommon(v8::Handle<v8::Object> target);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
