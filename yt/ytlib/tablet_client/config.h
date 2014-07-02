#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <ytlib/election/config.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

class TTabletCellConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<Stroka> Addresses;

    TTabletCellConfig()
    {
        RegisterParameter("addresses", Addresses);
    }

    NElection::TCellConfigPtr ToElection(const NElection::TCellGuid& cellGuid) const
    {
        auto result = New<NElection::TCellConfig>();
        result->CellGuid = cellGuid;
        result->Addresses = Addresses;
        return result;
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletCellConfig)

///////////////////////////////////////////////////////////////////////////////

class TTableMountCacheConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration SuccessExpirationTime;
    TDuration FailureExpirationTime;

    TTableMountCacheConfig()
    {
        RegisterParameter("success_expiration_time", SuccessExpirationTime)
            .Default(TDuration::Seconds(5));
        RegisterParameter("failure_expiration_time", FailureExpirationTime)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
