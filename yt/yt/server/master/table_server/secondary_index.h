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
    DEFINE_BYVAL_RW_PROPERTY(TTableNode*, Table);
    DEFINE_BYVAL_RW_PROPERTY(TTableNode*, IndexTable);
    DEFINE_BYVAL_RW_PROPERTY(ESecondaryIndexKind, Kind);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag, NObjectClient::NotReplicatedCellTagSentinel);

public:
    using TObject::TObject;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
