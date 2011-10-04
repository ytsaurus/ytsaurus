#pragma once

#include <util/generic/strbuf.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

typedef TStringBuf TYPath;

struct INode;
struct ICompositeNode;
struct IStringNode;
struct IInt64Node;
struct IDoubleNode;
struct IListNode;
struct IMapNode;
struct IEntityNode;

struct INodeFactory;

struct IYsonConsumer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

