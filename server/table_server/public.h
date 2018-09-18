#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

const int MaxTabletCount = 10000;

////////////////////////////////////////////////////////////////////////////////

class TTableNode;
class TReplicatedTableNode;

template <class TImpl>
class TTableNodeTypeHandlerBase;
class TTableNodeTypeHandler;
class TReplicatedTableNodeTypeHandler;

DECLARE_REFCOUNTED_CLASS(TSharedTableSchema);
DECLARE_REFCOUNTED_CLASS(TSharedTableSchemaRegistry);
DECLARE_REFCOUNTED_CLASS(TVirtualStaticTable);
DECLARE_REFCOUNTED_CLASS(TReplicatedTableOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

