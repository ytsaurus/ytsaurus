#pragma once

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

using TTableId = NTableClient::TTableId;
using TMasterTableSchemaId = NObjectClient::TObjectId;

DECLARE_ENTITY_TYPE(TMasterTableSchema, TMasterTableSchemaId, NObjectClient::TDirectObjectIdHash)

class TTableNode;
class TReplicatedTableNode;

template <class TImpl>
class TTableNodeTypeHandlerBase;
class TTableNodeTypeHandler;
class TReplicatedTableNodeTypeHandler;

DECLARE_REFCOUNTED_CLASS(TTableManager)
DECLARE_REFCOUNTED_CLASS(TVirtualStaticTable);
DECLARE_REFCOUNTED_CLASS(TReplicatedTableOptions);
DECLARE_REFCOUNTED_CLASS(TTabletBalancerConfig);
DECLARE_REFCOUNTED_CLASS(TPartitionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

