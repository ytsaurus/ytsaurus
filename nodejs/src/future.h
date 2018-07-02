#pragma once

#include "common.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

//! This class wraps TFuture and allows interoperation between V8 and YT.
class TFutureWrap
    : public node::ObjectWrap
    , public TRefTracked<TFutureWrap>
{
protected:
    TFutureWrap();

public:
    ~TFutureWrap();

    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    static TFutureWrap* Unwrap(v8::Handle<v8::Value> value);
    static TFuture<void> UnwrapFuture(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> Subscribe(const v8::Arguments& args);
    static v8::Handle<v8::Value> Cancel(const v8::Arguments& args);

    // Synchronous C++ API.
    TFuture<void> GetFuture();
    void SetFuture(TFuture<void> future);

protected:
    TFuture<void> Future_;

private:
    TFutureWrap(const TFutureWrap&) = delete;
    TFutureWrap& operator=(const TFutureWrap&) = delete;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
