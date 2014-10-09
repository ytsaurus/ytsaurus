#pragma once

#include "public.h"

#include <server/hydra/snapshot_service.pb.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TLocalSnapshotService
    : public NRpc::TServiceBase
{
public:
    TLocalSnapshotService(
        const NElection::TCellId& cellId,
        TFileSnapshotStorePtr fileStore);

private:
    TFileSnapshotStorePtr FileStore_;

    DECLARE_RPC_SERVICE_METHOD(NProto, LookupSnapshot);
    DECLARE_RPC_SERVICE_METHOD(NProto, ReadSnapshot);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
