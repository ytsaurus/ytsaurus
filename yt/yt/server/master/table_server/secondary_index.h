#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

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
    DEFINE_BYREF_RW_PROPERTY(std::optional<TString>, Predicate);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TString>, UnfoldedColumn);
    DEFINE_BYVAL_RW_PROPERTY(ETableToIndexCorrespondence, TableToIndexCorrespondence, ETableToIndexCorrespondence::Invalid);

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    void OnAfterSnapshotLoaded();

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // COMPAT(sabdenovch)
    void SetIdsFromCompat();

private:
    TTableNode* CompatTable_ = nullptr;
    TTableNode* CompatIndexTable_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
