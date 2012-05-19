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
#include <ytlib/chunk_server/public.h>

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
        TCellMasterConfigPtr config);

    ~TBootstrap();

    TCellMasterConfigPtr GetConfig() const;

    NTransactionServer::TTransactionManagerPtr GetTransactionManager() const;
    NCypress::TCypressManagerPtr GetCypressManager() const;
    TWorldInitializerPtr GetWorldInitializer() const;
    NMetaState::IMetaStateManagerPtr GetMetaStateManager() const;
    NMetaState::TCompositeMetaStatePtr GetMetaState() const;
    NObjectServer::TObjectManager::TPtr GetObjectManager() const;
    NChunkServer::TChunkManagerPtr GetChunkManager() const;
    NChunkServer::IHolderAuthorityPtr GetHolderAuthority() const;

    IInvoker::TPtr GetControlInvoker();
    IInvoker::TPtr GetStateInvoker(EStateThreadQueue queueIndex = EStateThreadQueue::Default);

    void Run();

private:
    Stroka ConfigFileName;
    TCellMasterConfigPtr Config;

    NTransactionServer::TTransactionManagerPtr TransactionManager;
    NCypress::TCypressManagerPtr CypressManager;
    TWorldInitializerPtr WorldInitializer;
    NMetaState::IMetaStateManagerPtr MetaStateManager;
    NMetaState::TCompositeMetaStatePtr MetaState;
    NObjectServer::TObjectManager::TPtr ObjectManager;
    NChunkServer::TChunkManagerPtr ChunkManager;
    NChunkServer::IHolderAuthorityPtr HolderAuthority;

    TActionQueue::TPtr ControlQueue;
    TMultiActionQueuePtr StateQueue;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
