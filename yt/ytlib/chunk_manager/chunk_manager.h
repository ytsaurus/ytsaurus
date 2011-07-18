#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"
#include "chunk_manager.pb.h"
#include "holder_tracker.h"

#include "../rpc/service.h"
#include "../rpc/server.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

//! Manages user transactions.
class TChunkManager
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TChunkManager> TPtr;
    typedef TChunkManagerConfig TConfig;

    //! Creates an instance.
    TChunkManager(
        const TConfig& config,
        NRpc::TServer* server);
 
private:
    typedef TChunkManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    class TState;
    
    //! Configuration.
    TConfig Config;

    //! All state modifications are carried out via this invoker.
    IInvoker::TPtr ServiceInvoker;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Tracks holder liveness.
    THolderTracker::TPtr HolderTracker;

    THolder::TPtr GetHolder(int id);

    RPC_SERVICE_METHOD_DECL(NProto, RegisterHolder);
    RPC_SERVICE_METHOD_DECL(NProto, HolderHeartbeat);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
