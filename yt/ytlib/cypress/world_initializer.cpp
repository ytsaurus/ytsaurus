#include "stdafx.h"
#include "world_initializer.h"

#include "../misc/periodic_invoker.h"

namespace NYT {
namespace NCypress {

using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TWorldInitializer::TWorldInitializer(
    NMetaState::TMetaStateManager* metaStateManager,
    TCypressManager* cypressManager)
    : MetaStateManager(metaStateManager)
    , CypressManager(cypressManager)
{
    YASSERT(metaStateManager != NULL);
    YASSERT(cypressManager != NULL);
}

void TWorldInitializer::Start()
{
    auto invoker = New<TPeriodicInvoker>(
        FromMethod(&TWorldInitializer::CheckWorldInitialized, TPtr(this))
        ->Via(MetaStateManager->GetStateInvoker()),
        TDuration::Seconds(3));
    invoker->Start();
}

void TWorldInitializer::CheckWorldInitialized()
{
    if (MetaStateManager->GetStateStatus() != EPeerStatus::Leading)
        return;

    // Create the other stuff around it.
    //auto root = GetNodeProxy(RootNodeId, SysTransactionId);
    //NYTree::SetYPath(
    //    IYPathService::FromNode(~root),
    //    "/",
    //    FromFunctor([] (IYsonConsumer* consumer)
    //    {
    //        BuildYsonFluently(consumer)
    //            .BeginMap()
    //                .Item("sys").BeginMap()
    //                    // TODO: use named constants instead of literals
    //                    .Item("chunks").WithAttributes().Entity().BeginAttributes()
    //                        .Item("type").Scalar("chunk_map")
    //                    .EndAttributes()
    //                    .Item("chunk_lists").WithAttributes().Entity().BeginAttributes()
    //                        .Item("type").Scalar("chunk_list_map")
    //                    .EndAttributes()
    //                    .Item("transactions").WithAttributes().Entity().BeginAttributes()
    //                        .Item("type").Scalar("transaction_map")
    //                    .EndAttributes()
    //                    .Item("nodes").WithAttributes().Entity().BeginAttributes()
    //                        .Item("type").Scalar("node_map")
    //                    .EndAttributes()
    //                    .Item("locks").WithAttributes().Entity().BeginAttributes()
    //                        .Item("type").Scalar("lock_map")
    //                    .EndAttributes()
    //                    .Item("monitoring").WithAttributes().Entity().BeginAttributes()
    //                        .Item("type").Scalar("monitoring")
    //                    .EndAttributes()
    //                .EndMap()
    //                .Item("home").BeginMap()
    //                .EndMap()
    //            .EndMap();
    //    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

