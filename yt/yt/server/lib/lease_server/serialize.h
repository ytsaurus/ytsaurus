#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/serialize.h>

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELeaseManagerReign,
    ((StartingReign)                               (0)) // h0pless
    ((PersistentLeaseOwnerCellId)                  (1)) // h0pless
);

static_assert(TEnumTraits<ELeaseManagerReign>::IsMonotonic, "Lease manager reign enum is not monotonic");

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    using NHydra::TLoadContext::TLoadContext;

    DEFINE_BYVAL_RW_PROPERTY(ILeaseManagerPtr, LeaseManager);
};

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    using NHydra::TSaveContext::TSaveContext;
};

////////////////////////////////////////////////////////////////////////////////

struct TLeaseGuardSerializer
{
    static void Save(TSaveContext& context, const ILeaseGuardPtr& guard);
    static void Load(TLoadContext& context, ILeaseGuardPtr& guard);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<NLeaseServer::ILeaseGuardPtr, C, void>
{
    using TSerializer = NLeaseServer::TLeaseGuardSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
