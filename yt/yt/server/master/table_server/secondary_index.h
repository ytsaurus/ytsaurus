#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/library/query/secondary_index/schema.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TSecondaryIndex
    : public NObjectServer::TObject
    , public TRefTracked<TSecondaryIndex>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TTableId, TableId);
    DEFINE_BYVAL_RW_PROPERTY(TTableId, IndexTableId);
    DEFINE_BYVAL_RW_PROPERTY(ESecondaryIndexKind, Kind);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag, NObjectClient::NotReplicatedCellTagSentinel);
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::string>, Predicate);
    DEFINE_BYREF_RW_PROPERTY(std::optional<NTabletClient::TUnfoldedColumns>, UnfoldedColumns);
    DEFINE_BYVAL_RW_PROPERTY(ETableToIndexCorrespondence, TableToIndexCorrespondence, ETableToIndexCorrespondence::Invalid);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TTableSchemaPtr, EvaluatedColumnsSchema);

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    void OnAfterSnapshotLoaded();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TSecondaryIndex)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
