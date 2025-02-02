#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/client/object_client/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableCollocation
    : public NObjectServer::TObject
    , public TRefTracked<TTableCollocation>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, ExternalCellTag, NObjectClient::InvalidCellTag);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTableNodeRawPtr>, Tables);
    DEFINE_BYVAL_RW_PROPERTY(ETableCollocationType, Type);

    DEFINE_BYREF_RW_PROPERTY(
        NTabletClient::TReplicationCollocationOptionsPtr,
        ReplicationCollocationOptions,
        New<NTabletClient::TReplicationCollocationOptions>());

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TTableCollocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
