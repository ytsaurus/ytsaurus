#pragma once

#include <yt/core/misc/public.h>

#include <yt/client/table_client/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxTabletCount = 10000;

////////////////////////////////////////////////////////////////////////////////

using TInternedTableSchema = TInternedObject<NTableClient::TTableSchema>;
using TTableSchemaRegistry = TInternRegistry<NTableClient::TTableSchema>;
using TTableSchemaRegistryPtr = TInternRegistryPtr<NTableClient::TTableSchema>;

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
DECLARE_REFCOUNTED_CLASS(TTabletBalancerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

