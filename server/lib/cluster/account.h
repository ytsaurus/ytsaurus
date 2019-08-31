#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TAccount
    : public TObject
    , public NYT::TRefTracked<TAccount>
{
public:
    static constexpr EObjectType Type = EObjectType::Account;

    TAccount(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        TObjectId parentId);

    DEFINE_BYREF_RO_PROPERTY(TObjectId, ParentId);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TPod*>, Pods);
    DEFINE_BYVAL_RW_PROPERTY(TAccount*, Parent);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TAccount*>, Children);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
