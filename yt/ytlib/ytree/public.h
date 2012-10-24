#pragma once

#include <ytlib/misc/common.h>

#include <ytlib/actions/callback_forward.h>

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! A static node type.
DECLARE_ENUM(ENodeType,
    // Node contains a string (Stroka).
    (String)
    // Node contains an integer number (i64).
    (Integer)
    // Node contains an FP number (double).
    (Double)
    // Node contains a map from strings to other nodes.
    (Map)
    // Node contains a list (vector) of other nodes.
    (List)
    // Node is atomic, i.e. has no visible properties (aside from attributes).
    (Entity)
    // Either List or Map.
    (Composite)
);

//! The data format.
DECLARE_ENUM(EYsonFormat,
    // Binary.
    // Most compact but not human-readable.
    (Binary)

    // Text.
    // Not so compact but human-readable.
    // Does not use indentation.
    // Uses escaping for non-text characters.
    (Text)

    // Text with indentation.
    // Extremely verbose but human-readable.
    // Uses escaping for non-text characters.
    (Pretty)
);

class EYsonType;

class TYsonString;

class ETokenType;

class TTokenizer;

struct IParser;

struct INode;
typedef TIntrusivePtr<INode> INodePtr;
typedef TIntrusivePtr<const INode> IConstNodePtr;

struct ICompositeNode;
typedef TIntrusivePtr<ICompositeNode> ICompositeNodePtr;

struct IStringNode;
typedef TIntrusivePtr<IStringNode> IStringNodePtr;

struct IIntegerNode;
typedef TIntrusivePtr<IIntegerNode> IIntegerNodePtr;

struct IDoubleNode;
typedef TIntrusivePtr<IDoubleNode> IDoubleNodePtr;

struct IListNode;
typedef TIntrusivePtr<IListNode> IListNodePtr;

struct IMapNode;
typedef TIntrusivePtr<IMapNode> IMapNodePtr;

struct IEntityNode;
typedef TIntrusivePtr<IEntityNode> IEntityNodePtr;

struct INodeFactory;
typedef TIntrusivePtr<INodeFactory> INodeFactoryPtr;

struct IYPathResolver;
typedef TIntrusivePtr<IYPathResolver> IYPathResolverPtr;

struct IYsonConsumer;
class TYsonProducer;

class TYsonInput;
class TYsonOutput;

struct IAttributeDictionary;

struct IAttributeOwner;

struct ISystemAttributeProvider;

struct IYPathService;
typedef TIntrusivePtr<IYPathService> IYPathServicePtr;

typedef TCallback<IYPathServicePtr()> TYPathServiceProducer;

class TYPathRequest;
typedef TIntrusivePtr<TYPathRequest> TYPathRequestPtr;

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathRequest;

class TYPathResponse;
typedef TIntrusivePtr<TYPathResponse> TYPathResponsePtr;

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse;

using NYPath::TYPath;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree 
} // namespace NYT
