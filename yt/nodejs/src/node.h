#pragma once

#include "common.h"

#include <ytlib/ytree/public.h>

namespace NYT {

NYTree::INodePtr ConvertV8ValueToNode(v8::Handle<v8::Value> value);
NYTree::INodePtr ConvertV8BytesToNode(const char* buffer, size_t length, ECompression compression, NYTree::INodePtr format);

////////////////////////////////////////////////////////////////////////////////

//! This class wraps INodePtr and allows interoperation between V8 and YT.
class TNodeJSNode
    : public node::ObjectWrap
{
protected:
    TNodeJSNode(NYTree::INodePtr node);

public:
    ~TNodeJSNode() throw();

    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);
    static NYTree::INodePtr Node(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);
    static v8::Handle<v8::Value> CreateMerged(const v8::Arguments& args);
    static v8::Handle<v8::Value> CreateV8(const v8::Arguments& args);

    static v8::Handle<v8::Value> Print(const v8::Arguments& args);
    static v8::Handle<v8::Value> Get(const v8::Arguments& args);

    // Synchronous C++ API.
    NYTree::INodePtr GetNode();
    const NYTree::INodePtr GetNode() const;

    void SetNode(NYTree::INodePtr node);

protected:
    NYTree::INodePtr Node_;

private:
    TNodeJSNode(const TNodeJSNode&);
    TNodeJSNode& operator=(const TNodeJSNode&);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
