#pragma once

#include <core/misc/public.h>

#include <core/ypath/public.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonSerializableLite;
class TYsonSerializable;

class TYsonString;

DECLARE_REFCOUNTED_STRUCT(INode);
typedef TIntrusivePtr<const INode> IConstNodePtr;
DECLARE_REFCOUNTED_STRUCT(ICompositeNode);
typedef TIntrusivePtr<ICompositeNode> ICompositeNodePtr;
DECLARE_REFCOUNTED_STRUCT(IStringNode);
DECLARE_REFCOUNTED_STRUCT(IInt64Node);
DECLARE_REFCOUNTED_STRUCT(IUint64Node);
DECLARE_REFCOUNTED_STRUCT(IDoubleNode);
DECLARE_REFCOUNTED_STRUCT(IBooleanNode);
DECLARE_REFCOUNTED_STRUCT(IListNode);
DECLARE_REFCOUNTED_STRUCT(IMapNode);
DECLARE_REFCOUNTED_STRUCT(IEntityNode);

DECLARE_REFCOUNTED_STRUCT(INodeFactory);
DECLARE_REFCOUNTED_STRUCT(INodeResolver);

class TYsonProducer;

class TYsonInput;
class TYsonOutput;

struct IAttributeDictionary;

struct IAttributeOwner;

struct ISystemAttributeProvider;

DECLARE_REFCOUNTED_STRUCT(IYPathService);

DECLARE_REFCOUNTED_CLASS(TYPathRequest);
DECLARE_REFCOUNTED_CLASS(TYPathResponse);

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathRequest;

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse;

using NYPath::TYPath;

////////////////////////////////////////////////////////////////////////////////

//! A static node type.
DEFINE_ENUM(ENodeType,
    // Node contains a string (Stroka).
    (String)
    // Node contains an int64 number (i64).
    (Int64)
    // Node contains an uint64 number (ui64).
    (Uint64)
    // Node contains an FP number (double).
    (Double)
    // Node contains an boolean (bool).
    (Boolean)
    // Node contains a map from strings to other nodes.
    (Map)
    // Node contains a list (vector) of other nodes.
    (List)
    // Node is atomic, i.e. has no visible properties (aside from attributes).
    (Entity)
    // Either List or Map.
    (Composite)
);

DEFINE_ENUM(EErrorCode,
    ((ResolveError)    (500))
    ((AlreadyExists)   (501))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
