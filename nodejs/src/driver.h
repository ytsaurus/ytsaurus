#pragma once

#include "common.h"

#include <yt/ytlib/driver/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class TDriverWrap
    : public node::ObjectWrap
    , public TRefTracked<TDriverWrap>
{
protected:
    TDriverWrap(bool echo, NDriver::IDriverPtr driver);
    ~TDriverWrap();

public:
    using node::ObjectWrap::Ref;
    using node::ObjectWrap::Unref;

    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> FindCommandDescriptor(const v8::Arguments& args);
    v8::Handle<v8::Value> DoFindCommandDescriptor(const TString& commandName);

    static v8::Handle<v8::Value> GetCommandDescriptors(const v8::Arguments& args);
    v8::Handle<v8::Value> DoGetCommandDescriptors();

    // Asynchronous JS API.
    static v8::Handle<v8::Value> Execute(const v8::Arguments& args);

private:
    // This is for testing purposes only.
    const bool Echo;
    // This is for real.
    NDriver::IDriverPtr Driver;

private:
    TDriverWrap(const TDriverWrap&) = delete;
    TDriverWrap& operator=(const TDriverWrap&) = delete;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT

