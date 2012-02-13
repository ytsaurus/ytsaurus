#pragma once

#include <ytlib/misc/common.h>

#include <util/generic/strbuf.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

typedef Stroka TYPath;
typedef Stroka TYson;

struct INode;
typedef TIntrusivePtr<INode> TNodePtr;

struct ICompositeNode;
struct IStringNode;
struct IInt64Node;
struct IDoubleNode;
struct IListNode;

struct IMapNode;
typedef TIntrusivePtr<IMapNode> TMapNodePtr;

struct IEntityNode;

struct INodeFactory;

struct IYsonConsumer;

struct IAttributeDictionary;
typedef TIntrusivePtr<IAttributeDictionary> TAttributeDictionaryPtr;

struct IAttributeProvider;
typedef TIntrusivePtr<IAttributeProvider> TAttributeProviderPtr;

// TODO(roizner): Rename it and move somewhere.
template <class T, class = void>
struct TDeserializeTraits;

// TODO(roizner): add other stuff from this folder

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree 
} // namespace NYT