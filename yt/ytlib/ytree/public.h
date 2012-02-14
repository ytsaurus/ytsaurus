#pragma once

#include <ytlib/actions/action.h>
#include <ytlib/misc/common.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

typedef Stroka TYPath;
typedef Stroka TYson;

//! A static node type.
DECLARE_ENUM(ENodeType,
    // Node contains a string (Stroka).
    (String)
    // Node contains an integer number (i64).
    (Int64)
    // Node contains an FP number (double).
    (Double)
    // Node contains a map from strings to other nodes.
    (Map)
    // Node contains a list (vector) of other nodes.
    (List)
    // Node is atomic, i.e. has no visible properties (aside from attributes).
    (Entity)
);
    
struct INode;
typedef TIntrusivePtr<INode> TNodePtr;

struct ICompositeNode;
typedef TIntrusivePtr<ICompositeNode> TCompositeNodePtr;

struct IStringNode;
typedef TIntrusivePtr<IStringNode> TStringNodePtr;

struct IInt64Node;
typedef TIntrusivePtr<IInt64Node> TInt64NodePtr;

struct IDoubleNode;
typedef TIntrusivePtr<IDoubleNode> TDoubleNodePtr;

struct IListNode;
typedef TIntrusivePtr<IListNode> TListNodePtr;

struct IMapNode;
typedef TIntrusivePtr<IMapNode> TMapNodePtr;

struct IEntityNode;
typedef TIntrusivePtr<IEntityNode> TEntityNodePtr;

struct INodeFactory;
typedef TIntrusivePtr<INodeFactory> TNodeFactoryPtr;

struct IYsonConsumer;

//! A callback capable of generating YSON by calling appropriate
//! methods for its IYsonConsumer argument.
typedef IParamAction<IYsonConsumer*>::TPtr TYsonProducer;

struct IAttributeDictionary;

struct IAttributeProvider;

struct IYPathService;

// TODO(roizner): Rename it and move somewhere.
template <class T, class = void>
struct TDeserializeTraits;

class TYPathRequest;

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathRequest;

class TYPathResponse;

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree 
} // namespace NYT