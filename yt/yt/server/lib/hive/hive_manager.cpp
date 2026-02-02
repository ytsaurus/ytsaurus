#include "hive_manager.h"

#include "config.h"
#include "hive_manager_v1.h"
#include "hive_manager_v2.h"

namespace NYT::NHiveServer {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static NConcurrency::TFlsSlot<TCellId> HiveMutationSenderId;
static NConcurrency::TFlsSlot<TReign> HiveMutationSenderReign;

bool IsHiveMutation()
{
    return static_cast<bool>(*HiveMutationSenderId);
}

TCellId GetHiveMutationSenderId()
{
    return *HiveMutationSenderId;
}

TReign GetHiveMutationSenderReign()
{
    return *HiveMutationSenderReign;
}

THiveMutationGuard::THiveMutationGuard(TCellId senderId, TReign senderReign)
{
    YT_VERIFY(!*HiveMutationSenderId);
    YT_VERIFY(!*HiveMutationSenderReign);
    *HiveMutationSenderId = senderId;
    *HiveMutationSenderReign = senderReign;
}

THiveMutationGuard::~THiveMutationGuard()
{
    *HiveMutationSenderId = {};
    *HiveMutationSenderReign = {};
}

TInverseHiveMutationGuard::TInverseHiveMutationGuard()
    : SenderId_(*HiveMutationSenderId)
    , SenderReign_(*HiveMutationSenderReign)
{
    YT_VERIFY(SenderId_);
    // NB: Reign may be zero.
    *HiveMutationSenderId = {};
    *HiveMutationSenderReign = 0;
}

TInverseHiveMutationGuard::~TInverseHiveMutationGuard()
{
    *HiveMutationSenderId = SenderId_;
    *HiveMutationSenderReign = SenderReign_;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const THiveEdge& edge, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v->%v", edge.SourceCellId, edge.DestinationCellId);
}

////////////////////////////////////////////////////////////////////////////////

IHiveManagerPtr CreateHiveManager(
    THiveManagerConfigPtr config,
    NHiveClient::ICellDirectoryPtr cellDirectory,
    NCellMasterClient::ICellDirectoryPtr masterCellDirectory,
    IAvenueDirectoryPtr avenueDirectory,
    TCellId selfCellId,
    IInvokerPtr automatonInvoker,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    NHydra::IUpstreamSynchronizerPtr upstreamSynchronizer,
    NRpc::IAuthenticatorPtr authenticator)
{
    auto factory = config->UseNew ? NV2::CreateHiveManager : NV1::CreateHiveManager;
    return factory(
        std::move(config),
        std::move(cellDirectory),
        std::move(masterCellDirectory),
        std::move(avenueDirectory),
        selfCellId,
        std::move(automatonInvoker),
        std::move(hydraManager),
        std::move(automaton),
        std::move(upstreamSynchronizer),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
