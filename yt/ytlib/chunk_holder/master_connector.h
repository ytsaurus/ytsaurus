#pragma once

#include "common.h"
#include "chunk_store.h"

#include "../chunk_manager/chunk_manager_rpc.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMasterConnector> TPtr;
    typedef TChunkHolderConfig TConfig;

    //! Creates an instance.
    TMasterConnector(
        const TConfig& config,
        TChunkStore::TPtr chunkStore,
        IInvoker::TPtr serviceInvoker)
        : Config(config)
        , ChunkStore(chunkStore)
        , ServiceInvoker(serviceInvoker)
        , State(EState::NotConnected)
        , HolderId(InvalidHolderId)
    {  }

    //! Initializes an instance.
    /*!
     *  The instance cannot be fully initialized within ctor hence
     *  it needs to produce smartpointers to this.
     */
    void Initialize();
    
private:
    typedef NChunkManager::TChunkManagerProxy TProxy;

    static const int InvalidHolderId = -1;

    // TODO: more states, e.g. to postpone initial chunk report
    DECLARE_ENUM(EState,
        (NotConnected)
        (Connected)
    );

    TConfig Config;
    TChunkStore::TPtr ChunkStore;
    IInvoker::TPtr ServiceInvoker;
    EState State;
    int HolderId;
    THolder<TProxy> Proxy;
    Stroka Address;

    void InitializeProxy();
    void InitializeAddress();

    void ScheduleHeartbeat();
    void OnHeartbeatTime();
    void SendRegister();
    void OnRegisterResponse(TProxy::TRspRegisterHolder::TPtr response);
    void SendHeartbeat();
    void OnHeartbeatResponse(TProxy::TRspHolderHeartbeat::TPtr response);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
