#include "common.h"
#include "config.h"
#include "node.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/misc/shutdown.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/net/address.h>

#include <yt/core/tracing/trace_manager.h>

#include <util/string/escape.h>

#include <dlfcn.h>

extern "C" {
    // XXX(sandello): This is extern declaration of eio's internal functions.
    // -lrt will dynamically bind these symbols. We do this dirty-dirty stuff
    // because we would like to alter the thread pool size.

    extern void eio_set_min_parallel (unsigned int nthreads);
    extern void eio_set_max_parallel (unsigned int nthreads);

    extern unsigned int eio_nreqs    (void); /* number of requests in-flight */
    extern unsigned int eio_nready   (void); /* number of not-yet handled requests */
    extern unsigned int eio_npending (void); /* number of finished but unhandled requests */
    extern unsigned int eio_nthreads (void); /* number of worker threads in use currently */
}

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;
using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////
// Stuff related to EIO

Handle<Value> GetEioInformation(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 0);

    Local<Object> result = Object::New();
    result->Set(String::NewSymbol("nreqs"),    Integer::NewFromUnsigned(eio_nreqs()));
    result->Set(String::NewSymbol("nready"),   Integer::NewFromUnsigned(eio_nready()));
    result->Set(String::NewSymbol("npending"), Integer::NewFromUnsigned(eio_npending()));
    result->Set(String::NewSymbol("nthreads"), Integer::NewFromUnsigned(eio_nthreads()));

    return scope.Close(result);
}

Handle<Value> SetEioConcurrency(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 1);
    EXPECT_THAT_IS(args[0], Uint32);

    unsigned int numberOfThreads = args[0]->Uint32Value();
    YCHECK(numberOfThreads > 0);
    eio_set_min_parallel(numberOfThreads);
    eio_set_max_parallel(numberOfThreads);

    return scope.Close(Undefined());
}

////////////////////////////////////////////////////////////////////////////////
// Stuff related to global subsystems

Handle<Value> ConfigureSingletons(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 1);

    EXPECT_THAT_IS(args[0], Object);

    TryCatch catcher;
    INodePtr configNode = ConvertV8ValueToNode(args[0]);
    if (!configNode) {
        return catcher.HasCaught() ? catcher.ReThrow() : ThrowException(
            Exception::TypeError(String::New("Error converting from V8 to YSON")));
    }

    NNodeJS::THttpProxyConfigPtr config;
    try {
        // Qualify namespace to avoid collision with class method New().
        config = ::NYT::New<NYT::NNodeJS::THttpProxyConfig>();
        config->Load(configNode);
    } catch (const std::exception& ex) {
        return catcher.HasCaught() ? catcher.ReThrow() : ThrowException(
            Exception::TypeError(String::Concat(
                String::New("Error loading configuration: "),
                String::New(ex.what()))));
    }

    try {
        NLogging::TLogManager::Get()->Configure(config->Logging);
        NTracing::TTraceManager::Get()->Configure(config->Tracing);
        NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);
        TAddressResolver::Get()->Configure(config->AddressResolver);
    } catch (const std::exception& ex) {
        return catcher.HasCaught() ? catcher.ReThrow() : ThrowException(
            Exception::TypeError(String::Concat(
                String::New("Error initializing driver instance: "),
                String::New(ex.what()))));
    }

    return scope.Close(Undefined());
}

Handle<Value> ShutdownSingletons(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Shutdown();

    return scope.Close(Undefined());
}

////////////////////////////////////////////////////////////////////////////////
// Other stuff

Handle<Value> EscapeC(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YCHECK(args.Length() == 1);

    EXPECT_THAT_IS(args[0], String);

    // Unwrap arguments.
    String::Utf8Value value(args[0]);

    TString unescaped(*value, value.length());
    TString escaped = ::EscapeC(unescaped);

    return scope.Close(String::New(escaped.c_str()));
}

Handle<Value> WipeOutCurrentProcess(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 1);
    EXPECT_THAT_IS(args[0], Uint32);

    int exitCode = args[0]->Uint32Value();
    _exit(exitCode);

    return scope.Close(Undefined());
}

void JemallocWriteCb(void*, const char* string)
{
    ssize_t ignored __attribute__((unused));
    ignored = ::write(2, string, strlen(string));
}

Handle<Value> JemallocStats(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 0);

    void* ptr = nullptr;
    void (*malloc_stats_print)(void (*)(void *, const char *), void *, const char*) = nullptr;

    ptr = dlsym(nullptr, "malloc_stats_print");
    malloc_stats_print = (decltype(malloc_stats_print))(ptr);
    if (!malloc_stats_print) {
        return ThrowException(String::New("Jemalloc is not in use"));
    }

    malloc_stats_print(&JemallocWriteCb, nullptr, "a");

    return scope.Close(Undefined());
}

Handle<Value> JemallocCtlWrite(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 2);

    EXPECT_THAT_IS(args[0], String);

    String::Utf8Value key(args[0]);

    void* ptr = nullptr;
    int (*mallctl)(const char*, void*, size_t, void*, size_t) = nullptr;

    ptr = dlsym(nullptr, "mallctl");
    mallctl = (decltype(mallctl))(ptr);
    if (!mallctl) {
        return ThrowException(String::New("Jemalloc is not in use"));
    }

    if (args[1]->IsString()) {
        String::Utf8Value value(args[1]);
        mallctl(*key, nullptr, 0, *value, value.length());
    } else if (args[1]->IsNumber()) {
        unsigned int value = args[1]->Uint32Value();
        mallctl(*key, nullptr, 0, &value, sizeof(value));
    } else if (args[1]->IsNull()) {
        mallctl(*key, nullptr, 0, nullptr, 0);
    }

    return scope.Close(Undefined());
}

////////////////////////////////////////////////////////////////////////////////
// V8 stuff

Handle<Value> GetHeapStatistics(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YCHECK(args.Length() == 0);

    v8::HeapStatistics heapStatistics;
    v8::V8::GetHeapStatistics(&heapStatistics);

    Local<Object> result = Object::New();
    result->Set(String::New("total_heap_size"), Number::New(heapStatistics.total_heap_size()));
    result->Set(String::New("total_heap_size_executable"), Number::New(heapStatistics.total_heap_size_executable()));
    result->Set(String::New("used_heap_size"), Number::New(heapStatistics.used_heap_size()));
    result->Set(String::New("heap_size_limit"), Number::New(heapStatistics.heap_size_limit()));

    return scope.Close(result);
}

static ui32 TotalGCScavengeCount = 0;
static ui32 TotalGCScavengeTime = 0;
static ui32 TotalGCMarkSweepCompactCount = 0;
static ui32 TotalGCMarkSweepCompactTime = 0;

static TInstant GCStartInstant;

static void GCPrologue(v8::GCType gcType, v8::GCCallbackFlags)
{
    GCStartInstant = TInstant::Now();
}

static void GCEpilogue(v8::GCType gcType, v8::GCCallbackFlags)
{
    ui32* counter = nullptr;
    ui32* timer = nullptr;

    switch (gcType) {
        case v8::kGCTypeScavenge:
            counter = &TotalGCScavengeCount;
            timer = &TotalGCScavengeTime;
            break;
        case v8::kGCTypeMarkSweepCompact:
            counter = &TotalGCMarkSweepCompactCount;
            timer = &TotalGCMarkSweepCompactTime;
            break;
        default:
            Y_UNREACHABLE();
    }

    auto gcDuration = TInstant::Now() - GCStartInstant;

    *counter += 1;
    *timer += gcDuration.MilliSeconds();

    GCStartInstant = TInstant::Zero();
}

Handle<Value> GetGCStatistics(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Validate arguments.
    YCHECK(args.Length() == 0);

    Local<Object> result = Object::New();
    result->Set(String::New("total_scavenge_count"), Integer::NewFromUnsigned(TotalGCScavengeCount));
    result->Set(String::New("total_scavenge_time"), Integer::NewFromUnsigned(TotalGCScavengeTime));
    result->Set(String::New("total_mark_sweep_compact_count"), Integer::NewFromUnsigned(TotalGCMarkSweepCompactCount));
    result->Set(String::New("total_mark_sweep_compact_time"), Integer::NewFromUnsigned(TotalGCMarkSweepCompactTime));

    return scope.Close(result);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void InitializeCommon(Handle<Object> target)
{
    v8::V8::AddGCPrologueCallback(&GCPrologue);
    v8::V8::AddGCEpilogueCallback(&GCEpilogue);

    target->Set(
        String::NewSymbol("GetEioInformation"),
        FunctionTemplate::New(GetEioInformation)->GetFunction());
    target->Set(
        String::NewSymbol("SetEioConcurrency"),
        FunctionTemplate::New(SetEioConcurrency)->GetFunction());
    target->Set(
        String::NewSymbol("ConfigureSingletons"),
        FunctionTemplate::New(ConfigureSingletons)->GetFunction());
    target->Set(
        String::NewSymbol("ShutdownSingletons"),
        FunctionTemplate::New(ShutdownSingletons)->GetFunction());
    target->Set(
        String::NewSymbol("EscapeC"),
        FunctionTemplate::New(EscapeC)->GetFunction());
    target->Set(
        String::NewSymbol("WipeOutCurrentProcess"),
        FunctionTemplate::New(WipeOutCurrentProcess)->GetFunction());
    target->Set(
        String::NewSymbol("JemallocStats"),
        FunctionTemplate::New(JemallocStats)->GetFunction());
    target->Set(
        String::NewSymbol("JemallocCtlWrite"),
        FunctionTemplate::New(JemallocCtlWrite)->GetFunction());
    target->Set(
        String::NewSymbol("GetHeapStatistics"),
        FunctionTemplate::New(GetHeapStatistics)->GetFunction());
    target->Set(
        String::NewSymbol("GetGCStatistics"),
        FunctionTemplate::New(GetGCStatistics)->GetFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
