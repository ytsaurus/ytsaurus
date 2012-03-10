#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/enum.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/transaction_server/public.h>
#include <ytlib/cypress/public.h>
// TODO(babenko): replace with public.h
#include <ytlib/meta_state/meta_state_manager.h>
// TODO(babenko): replace with public.h
#include <ytlib/meta_state/composite_meta_state.h>
// TODO(babenko): replace with public.h
#include <ytlib/object_server/object_manager.h>
// TODO(babenko): replace with public.h
#include <ytlib/chunk_server/chunk_manager.h>
// TODO(babenko): replace with public.h
#include <ytlib/chunk_server/holder_authority.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

const int StateThreadQueueCount = 2;

DECLARE_ENUM(EStateThreadQueue,
    (Default)
    (ChunkRefresh)
);

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        const Stroka& configFileName,
        TCellMasterConfig* config);

    ~TBootstrap();

    TCellMasterConfigPtr GetConfig() const;

    NTransactionServer::TTransactionManagerPtr GetTransactionManager() const;
    NCypress::TCypressManagerPtr GetCypressManager() const;
    TWorldInitializerPtr GetWorldInitializer() const;
    NMetaState::IMetaStateManager::TPtr GetMetaStateManager() const;
    NMetaState::TCompositeMetaState::TPtr GetMetaState() const;
    NObjectServer::TObjectManager::TPtr GetObjectManager() const;
    NChunkServer::TChunkManager::TPtr GetChunkManager() const;
    NChunkServer::IHolderAuthority::TPtr GetHolderAuthority() const;

    IInvoker::TPtr GetControlInvoker();
    IInvoker::TPtr GetStateInvoker(EStateThreadQueue queueIndex = EStateThreadQueue::Default);

    void Run();

private:
    Stroka ConfigFileName;
    TCellMasterConfigPtr Config;

    NTransactionServer::TTransactionManagerPtr TransactionManager;
    NCypress::TCypressManagerPtr CypressManager;
    TWorldInitializerPtr WorldInitializer;
    NMetaState::IMetaStateManager::TPtr MetaStateManager;
    NMetaState::TCompositeMetaState::TPtr MetaState;
    NObjectServer::TObjectManager::TPtr ObjectManager;
    NChunkServer::TChunkManager::TPtr ChunkManager;
    NChunkServer::IHolderAuthority::TPtr HolderAuthority;

    TActionQueue::TPtr ControlQueue;
    TMultiActionQueuePtr StateQueue;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
