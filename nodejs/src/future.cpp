#include "future.h"
#include "error.h"
#include "uv_invoker.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TFutureWrap::ConstructorTemplate;

TFutureWrap::TFutureWrap()
    : node::ObjectWrap()
{
    THREAD_AFFINITY_IS_V8();
}

TFutureWrap::~TFutureWrap() throw()
{
    THREAD_AFFINITY_IS_V8();
}

////////////////////////////////////////////////////////////////////////////////

void TFutureWrap::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TFutureWrap::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TFutureWrap"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Subscribe", TFutureWrap::Subscribe);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Cancel", TFutureWrap::Cancel);

    target->Set(
        String::NewSymbol("TFutureWrap"),
        ConstructorTemplate->GetFunction());
}

bool TFutureWrap::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

TFutureWrap* TFutureWrap::Unwrap(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return ObjectWrap::Unwrap<TFutureWrap>(value->ToObject());
}

TFuture<void> TFutureWrap::UnwrapFuture(Handle<Value> value)
{
    return Unwrap(value)->GetFuture();
}

Handle<Value> TFutureWrap::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 0);

    try {
        auto wrap = new TFutureWrap();
        wrap->Wrap(args.This());

        return scope.Close(args.This());
    } catch (const std::exception& ex) {
        return ThrowException(ConvertErrorToV8(ex));
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TFutureWrap::Subscribe(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 1);

    EXPECT_THAT_IS(args[0], Function);

    auto future = TFutureWrap::UnwrapFuture(args.This());
    future.Subscribe(BIND([
        // We trust UV invoker to never drop any actions.
        cb = Persistent<Function>::New(args[0].As<Function>())
    ] (const TError& error) mutable {
        THREAD_AFFINITY_IS_V8();
        HandleScope scope;

        Invoke(cb, ConvertErrorToV8(error));

        cb.Dispose();
        cb.Clear();
    }).Via(GetUVInvoker()));

    return scope.Close(Undefined());
}

Handle<Value> TFutureWrap::Cancel(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 0);

    auto future = TFutureWrap::UnwrapFuture(args.This());
    future.Cancel();

    return scope.Close(Undefined());
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TFutureWrap::GetFuture()
{
    return Future_;
}

void TFutureWrap::SetFuture(TFuture<void> future)
{
    Future_ = std::move(future);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
