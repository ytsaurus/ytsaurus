#include "common.h"
#include "config.h"
#include "node.h"

#include <core/logging/log_manager.h>

#include <core/tracing/trace_manager.h>

#include <core/misc/address.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/shutdown.h>

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
    return Undefined();
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
        NLog::TLogManager::Get()->Configure(config->Logging);
        NTracing::TTraceManager::Get()->Configure(config->Tracing);
        NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);
        TAddressResolver::Get()->Configure(config->AddressResolver);
    } catch (const std::exception& ex) {
        return catcher.HasCaught() ? catcher.ReThrow() : ThrowException(
            Exception::TypeError(String::Concat(
                String::New("Error initializing driver instance: "),
                String::New(ex.what()))));
    }

    return Undefined();
}

Handle<Value> ShutdownSingletons(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length());

    Shutdown();

    return Undefined();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void InitializeCommon(Handle<Object> target)
{
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
