#pragma once

#include "common.h"

#include <core/ytree/public.h>

namespace NYT {
namespace NNodeJS {

NYTree::INodePtr ConvertV8ValueToNode(v8::Handle<v8::Value> value);
NYTree::INodePtr ConvertV8BytesToNode(const char* buffer, size_t length, ECompression compression, NYTree::INodePtr format);
v8::Handle<v8::Value> ConvertNodeToV8Value(const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

//! This class wraps INodePtr and allows interoperation between V8 and YT.
class TNodeWrap
    : public node::ObjectWrap
{
protected:
    TNodeWrap(NYTree::INodePtr node);

public:
    ~TNodeWrap() throw();

    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    static NYTree::INodePtr UnwrapNode(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);
    static v8::Handle<v8::Value> CreateMerged(const v8::Arguments& args);
    static v8::Handle<v8::Value> CreateV8(const v8::Arguments& args);

    static v8::Handle<v8::Value> Print(const v8::Arguments& args);
    static v8::Handle<v8::Value> Traverse(const v8::Arguments& args);
    static v8::Handle<v8::Value> Get(const v8::Arguments& args);

    // Synchronous C++ API.
    NYTree::INodePtr GetNode();
    const NYTree::INodePtr GetNode() const;

    void SetNode(NYTree::INodePtr node);

protected:
    NYTree::INodePtr Node_;

private:
    TNodeWrap(const TNodeWrap&);
    TNodeWrap& operator=(const TNodeWrap&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
