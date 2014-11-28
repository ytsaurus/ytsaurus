#pragma once

#include "public.h"
#include "subject.h"

#include <core/misc/property.h>

#include <server/object_server/object.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TUser
    : public TSubject
{
public:
    // Limits and bans.
    DEFINE_BYVAL_RW_PROPERTY(bool, Banned);
    DEFINE_BYVAL_RW_PROPERTY(double, RequestRateLimit);

    // Request counters.
    DEFINE_BYVAL_RW_PROPERTY(i64, RequestCounter);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, AccessTime);
    DEFINE_BYVAL_RW_PROPERTY(NProto::TRequestStatisticsUpdate*, RequestStatisticsUpdate);
    
    // Request rate management.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, CheckpointTime);
    DEFINE_BYVAL_RW_PROPERTY(i64, CheckpointRequestCounter);
    DEFINE_BYVAL_RW_PROPERTY(double, RequestRate);

public:
    explicit TUser(const TUserId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void ResetRequestRate();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
