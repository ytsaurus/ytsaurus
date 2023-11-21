#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

using TTableId = NTableClient::TTableId;
using TMasterTableSchemaId = NObjectClient::TObjectId;
using TTableCollocationId = NTableClient::TTableCollocationId;
using TSecondaryIndexId = NObjectClient::TObjectId;

DECLARE_ENTITY_TYPE(TMasterTableSchema, TMasterTableSchemaId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTableCollocation, TTableCollocationId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TSecondaryIndex, TSecondaryIndexId, NObjectClient::TDirectObjectIdHash)

class TTableNode;
class TReplicatedTableNode;
class TSchemafulNode;

template <class TImpl>
class TTableNodeTypeHandlerBase;
class TTableNodeTypeHandler;
class TReplicatedTableNodeTypeHandler;

DECLARE_REFCOUNTED_STRUCT(ITableManager)

DECLARE_REFCOUNTED_CLASS(TVirtualStaticTable)
DECLARE_REFCOUNTED_CLASS(TTableManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TPartitionConfig)
DECLARE_REFCOUNTED_CLASS(TMountConfigAttributeDictionary)

DECLARE_MASTER_OBJECT_TYPE(TMasterTableSchema);

using NTableClient::ETableCollocationType;
using NTabletClient::ESecondaryIndexKind;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

