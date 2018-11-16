#include "multicell_manager.h"
#include "config.h"
#include "bootstrap.h"
#include "private.h"
#include "automaton.h"
#include "serialize.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "helpers.h"

#include <yt/core/ytree/ypath_client.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/rpc/retrying_channel.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/server/hive/hive_manager.h>
#include <yt/server/hive/mailbox.h>
#include <yt/server/hive/helpers.h>
#include <yt/server/hive/proto/hive_manager.pb.h>

#include <yt/server/hydra/mutation.h>

#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>

#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/cell_master/multicell_manager.pb.h>

namespace NYT {
namespace NCellMaster {

using namespace NElection;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NHiveServer;
using namespace NHiveClient;
using namespace NHiveClient::NProto;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;
static const auto RegisterRetryPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrimaryRegisterState,
    (None)
    (Registering)
    (Registered)
);

class TMulticellManager::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(
        TMulticellManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::MulticellManager)
        , Config_(config)
    {
        YCHECK(Config_);

        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterSecondaryMasterAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraOnSecondaryMasterRegisteredAtPrimary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraRegisterSecondaryMasterAtSecondary, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraStartSecondaryMasterRegistration, Unretained(this)));
        TMasterAutomatonPart::RegisterMethod(BIND(&TImpl::HydraSetCellStatistics, Unretained(this)));

        RegisterLoader(
            "MulticellManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MulticellManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }


    void PostToMaster(
        const TCrossCellMessage& message,
        TCellTag cellTag,
        bool reliable)
    {
        auto encapsulatedMessage = BuildHiveMessage(message);
        DoPostMessage(std::move(encapsulatedMessage), TCellTagList{cellTag}, reliable);
    }

    void PostToMasters(
        const TCrossCellMessage& message,
        const TCellTagList& cellTags,
        bool reliable)
    {
        if (cellTags.empty()) {
            return;
        }

        auto encapsulatedMessage = BuildHiveMessage(message);
        DoPostMessage(std::move(encapsulatedMessage), cellTags, reliable);
    }

    void PostToSecondaryMasters(
        const TCrossCellMessage& message,
        bool reliable)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());
        if (Bootstrap_->IsMulticell()) {
            PostToMasters(message, GetRegisteredMasterCellTags(), reliable);
        }
    }


    bool IsLocalMasterCellRegistered()
    {
        if (Bootstrap_->IsPrimaryMaster()) {
            return true;
        }

        if (RegisterState_ == EPrimaryRegisterState::Registered) {
            return true;
        }

        return false;
    }

    bool IsRegisteredSecondaryMaster(TCellTag cellTag)
    {
        return FindMasterEntry(cellTag) != nullptr;
    }

    const TCellTagList& GetRegisteredMasterCellTags()
    {
        return RegisteredMasterCellTags_;
    }

    int GetRegisteredMasterCellIndex(TCellTag cellTag)
    {
        return GetMasterEntry(cellTag)->Index;
    }


    TCellTag PickSecondaryMasterCell(double bias)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        if (RegisteredMasterMap_.empty()) {
            return InvalidCellTag;
        }

        // Compute the average number of chunks.
        int chunkCountSum = 0;
        for (const auto& pair : RegisteredMasterMap_) {
            const auto& entry = pair.second;
            chunkCountSum += entry.Statistics.chunk_count();
        }

        int avgChunkCount = chunkCountSum / RegisteredMasterMap_.size();

        // Split the candidates into two subsets: less-that-avg and more-than-avg.
        SmallVector<TCellTag, MaxSecondaryMasterCells> loCandidates;
        SmallVector<TCellTag, MaxSecondaryMasterCells> hiCandidates;
        for (const auto& pair : RegisteredMasterMap_) {
            auto cellTag = pair.first;
            const auto& entry = pair.second;
            if (entry.Statistics.chunk_count() < avgChunkCount) {
                loCandidates.push_back(cellTag);
            } else {
                hiCandidates.push_back(cellTag);
            }
        }

        // Sample candidates.
        // loCandidates have weight 2^8 + bias * 2^8.
        // hiCandidates have weight 2^8.
        ui64 scaledBias = static_cast<ui64>(bias * (1ULL << 8));
        ui64 weightPerLo = (1ULL << 8) + scaledBias;
        ui64 totalLoWeight = weightPerLo * loCandidates.size();
        ui64 weightPerHi = 1ULL << 8;
        ui64 totalHiWeight = weightPerHi * hiCandidates.size();
        ui64 totalTokens = totalLoWeight + totalHiWeight;
        auto* mutationContext = GetCurrentMutationContext();
        ui64 random = mutationContext->RandomGenerator().Generate<ui64>() % totalTokens;
        return random < totalLoWeight
            ? loCandidates[random / weightPerLo]
            : hiCandidates[(random - totalLoWeight) / weightPerHi];
    }

    NProto::TCellStatistics ComputeClusterStatistics()
    {
        auto result = GetLocalCellStatistics();
        for (const auto& pair : RegisteredMasterMap_) {
            const auto& entry = pair.second;
            result += entry.Statistics;
        }
        return result;
    }


    IChannelPtr GetMasterChannelOrThrow(TCellTag cellTag, EPeerKind peerKind)
    {
        auto channel = FindMasterChannel(cellTag, peerKind);
        if (!channel) {
            THROW_ERROR_EXCEPTION("Unknown cell tag %v",
                cellTag);
        }
        return channel;
    }

    IChannelPtr FindMasterChannel(TCellTag cellTag, EPeerKind peerKind)
    {
        auto key = std::make_tuple(cellTag, peerKind);
        auto it = MasterChannelCache_.find(key);
        if (it != MasterChannelCache_.end()) {
            return it->second;
        }

        const auto& cellDirectory = Bootstrap_->GetCellDirectory();
        auto cellId = Bootstrap_->GetCellId(cellTag);
        auto channel = cellDirectory->FindChannel(cellId, peerKind);
        if (!channel) {
            return nullptr;
        }

        auto isRetryableError = BIND([] (const TError& error) {
            return
                error.GetCode() == NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded ||
                IsRetriableError(error);
        });
        channel = CreateRetryingChannel(Config_->MasterConnection, channel, isRetryableError);
        channel = CreateDefaultTimeoutChannel(channel, Config_->MasterConnection->RpcTimeout);

        YCHECK(MasterChannelCache_.emplace(key, channel).second);

        return channel;
    }

    TMailbox* FindPrimaryMasterMailbox()
    {
        return PrimaryMasterMailbox_;
    }


    DEFINE_SIGNAL(void(TCellTag), ValidateSecondaryMasterRegistration);
    DEFINE_SIGNAL(void(TCellTag), ReplicateKeysToSecondaryMaster);
    DEFINE_SIGNAL(void(TCellTag), ReplicateValuesToSecondaryMaster);

private:
    const TMulticellManagerConfigPtr Config_;

    struct TMasterEntry
    {
        int Index = -1;
        NProto::TCellStatistics Statistics;

        void Save(NCellMaster::TSaveContext& context) const
        {
            using NYT::Save;
            Save(context, Index);
            Save(context, Statistics);
        }

        void Load(NCellMaster::TLoadContext& context)
        {
            using NYT::Load;
            Load(context, Index);
            Load(context, Statistics);
        }
    };

    // NB: Must ensure stable order.
    std::map<TCellTag, TMasterEntry> RegisteredMasterMap_;
    TCellTagList RegisteredMasterCellTags_;
    EPrimaryRegisterState RegisterState_;

    THashMap<TCellTag, TMailbox*> MasterMailboxCache_;
    TMailbox* PrimaryMasterMailbox_ = nullptr;

    TPeriodicExecutorPtr RegisterAtPrimaryMasterExecutor_;
    TPeriodicExecutorPtr CellStatisticsGossipExecutor_;

    //! Caches master channels returned by FindMasterChannel and GetMasterChannelOrThrow.
    std::map<std::tuple<TCellTag, EPeerKind>, IChannelPtr> MasterChannelCache_;


    virtual void OnAfterSnapshotLoaded()
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        RegisteredMasterCellTags_.resize(RegisteredMasterMap_.size());
        for (const auto& pair : RegisteredMasterMap_) {
            auto cellTag = pair.first;
            const auto& entry = pair.second;
            ValidateCellTag(cellTag);
            RegisteredMasterCellTags_[entry.Index] = cellTag;
            LOG_INFO("Master cell registered (CellTag: %v, CellIndex: %v)",
                cellTag,
                entry.Index);
        }

        if (RegisterState_ == EPrimaryRegisterState::Registered) {
            YCHECK(!PrimaryMasterMailbox_);
            const auto& hiveManager = Bootstrap_->GetHiveManager();
            PrimaryMasterMailbox_ = hiveManager->GetMailbox(Bootstrap_->GetPrimaryCellId());
        }
    }

    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        RegisteredMasterMap_.clear();
        RegisteredMasterCellTags_.clear();

        if (Bootstrap_->IsSecondaryMaster()) {
            RegisterMasterEntry(Bootstrap_->GetPrimaryCellTag());
        }

        RegisterState_ = EPrimaryRegisterState::None;
        PrimaryMasterMailbox_ = nullptr;
    }


    void LoadValues(TLoadContext& context)
    {
        using NYT::Load;
        Load(context, RegisteredMasterMap_);
        Load(context, RegisterState_);
    }

    void SaveValues(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RegisteredMasterMap_);
        Save(context, RegisterState_);
    }


    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        if (Bootstrap_->IsSecondaryMaster()) {
            RegisterAtPrimaryMasterExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
                BIND(&TImpl::OnStartSecondaryMasterRegistration, MakeWeak(this)),
                RegisterRetryPeriod);
            RegisterAtPrimaryMasterExecutor_->Start();

            CellStatisticsGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Periodic),
                BIND(&TImpl::OnCellStatisticsGossip, MakeWeak(this)),
                Config_->CellStatisticsGossipPeriod);
            CellStatisticsGossipExecutor_->Start();
        }
    }

    virtual void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        if (RegisterAtPrimaryMasterExecutor_) {
            RegisterAtPrimaryMasterExecutor_->Stop();
            RegisterAtPrimaryMasterExecutor_.Reset();
        }

        if (CellStatisticsGossipExecutor_) {
            CellStatisticsGossipExecutor_->Stop();
            CellStatisticsGossipExecutor_.Reset();
        }

        ClearCaches();
    }

    virtual void OnStopFollowing() override
    {
        TMasterAutomatonPart::OnStopFollowing();

        ClearCaches();
    }


    void ClearCaches()
    {
        MasterChannelCache_.clear();
        MasterMailboxCache_.clear();
    }


    void HydraRegisterSecondaryMasterAtPrimary(NProto::TReqRegisterSecondaryMasterAtPrimary* request)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        auto cellTag = request->cell_tag();
        try {
            ValidateSecondaryCellTag(cellTag);

            if (FindMasterEntry(cellTag))  {
                THROW_ERROR_EXCEPTION("Attempted to re-register secondary master %v", cellTag);
            }

            ValidateSecondaryMasterRegistration_.Fire(cellTag);

            RegisterMasterEntry(cellTag);

            ReplicateKeysToSecondaryMaster_.Fire(cellTag);
            ReplicateValuesToSecondaryMaster_.Fire(cellTag);

            for (const auto& pair : RegisteredMasterMap_) {
                if (pair.first == cellTag) {
                    continue;
                }
                
                {
                    // Inform others about the new secondary.
                    NProto::TReqRegisterSecondaryMasterAtSecondary request;
                    request.set_cell_tag(cellTag);
                    PostToMaster(request, pair.first, true);
                }
                {
                    // Inform the new secondary about others.
                    NProto::TReqRegisterSecondaryMasterAtSecondary request;
                    request.set_cell_tag(pair.first);
                    PostToMaster(request, cellTag, true);
                }
            }

            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            PostToMaster(response, cellTag, true);
        } catch (const std::exception& ex) {
            NProto::TRspRegisterSecondaryMasterAtPrimary response;
            ToProto(response.mutable_error(), TError(ex).Sanitize());
            PostToMaster(response, cellTag, true);
        }
    }

    void HydraOnSecondaryMasterRegisteredAtPrimary(NProto::TRspRegisterSecondaryMasterAtPrimary* response)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        if (response->has_error()) {
            auto error = FromProto<TError>(response->error());
            LOG_ERROR_UNLESS(IsRecovery(), error, "Error registering at primary master");
            RegisterState_ = EPrimaryRegisterState::None;
            return;
        }

        RegisterState_ = EPrimaryRegisterState::Registered;

        LOG_INFO_UNLESS(IsRecovery(), "Successfully registered at primary master");
    }

    void HydraRegisterSecondaryMasterAtSecondary(NProto::TReqRegisterSecondaryMasterAtSecondary* request)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        auto cellTag = request->cell_tag();
        try {
            ValidateSecondaryCellTag(cellTag);

            if (FindMasterEntry(cellTag))  {
                THROW_ERROR_EXCEPTION("Attempted to re-register secondary master %v", cellTag);
            }

            RegisterMasterEntry(cellTag);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error registering secondary master %v", cellTag);
        }
    }

    void HydraStartSecondaryMasterRegistration(NProto::TReqStartSecondaryMasterRegistration* /*request*/)
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        if (RegisterState_ != EPrimaryRegisterState::None) {
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Registering at primary master");

        RegisterState_ = EPrimaryRegisterState::Registering;

        if (!PrimaryMasterMailbox_) {
            const auto& hiveManager = Bootstrap_->GetHiveManager();
            PrimaryMasterMailbox_ = hiveManager->GetOrCreateMailbox(Bootstrap_->GetPrimaryCellId());
        }

        NProto::TReqRegisterSecondaryMasterAtPrimary request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        PostToMaster(request, PrimaryMasterCellTag, true);
    }

    void HydraSetCellStatistics(NProto::TReqSetCellStatistics* request)
    {
        YCHECK(Bootstrap_->IsPrimaryMaster());

        auto cellTag = request->cell_tag();
        LOG_INFO_UNLESS(IsRecovery(), "Received cell statistics gossip message (CellTag: %v)",
            cellTag);

        auto* entry = GetMasterEntry(cellTag);
        entry->Statistics = request->statistics();
    }


    void ValidateSecondaryCellTag(TCellTag cellTag)
    {
        const auto& config = Bootstrap_->GetConfig();
        for (auto cellConfig : config->SecondaryMasters) {
            if (CellTagFromId(cellConfig->CellId) == cellTag)
                return;
        }
        THROW_ERROR_EXCEPTION("Unknown secondary master cell tag %v", cellTag);
    }

    void ValidateCellTag(TCellTag cellTag)
    {
        const auto& config = Bootstrap_->GetConfig();
        if (CellTagFromId(config->PrimaryMaster->CellId) == cellTag)
            return;
        for (auto cellConfig : config->SecondaryMasters) {
            if (CellTagFromId(cellConfig->CellId) == cellTag)
                return;
        }
        THROW_ERROR_EXCEPTION("Unknown master cell tag %v", cellTag);
    }


    void RegisterMasterEntry(TCellTag cellTag)
    {
        YCHECK(RegisteredMasterMap_.size() == RegisteredMasterCellTags_.size());
        int index = static_cast<int>(RegisteredMasterMap_.size());
        auto pair = RegisteredMasterMap_.insert(std::make_pair(cellTag, TMasterEntry()));
        YCHECK(pair.second);
        auto& entry = pair.first->second;
        entry.Index = index;
        RegisteredMasterCellTags_.push_back(cellTag);
        LOG_INFO_UNLESS(IsRecovery(), "Master cell registered (CellTag: %v, CellIndex: %v)",
            cellTag,
            index);
    }

    TMasterEntry* FindMasterEntry(TCellTag cellTag)
    {
        auto it = RegisteredMasterMap_.find(cellTag);
        return it == RegisteredMasterMap_.end() ? nullptr : &it->second;
    }

    TMasterEntry* GetMasterEntry(TCellTag cellTag)
    {
        auto it = RegisteredMasterMap_.find(cellTag);
        YCHECK(it != RegisteredMasterMap_.end());
        return &it->second;
    }

    TMailbox* FindMasterMailbox(TCellTag cellTag)
    {
        if (cellTag == PrimaryMasterCellTag) {
            // This may be null.
            return PrimaryMasterMailbox_;
        }

        YCHECK(cellTag >= MinValidCellTag && cellTag <= MaxValidCellTag);
        auto it = MasterMailboxCache_.find(cellTag);
        if (it != MasterMailboxCache_.end()) {
            return it->second;
        }

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto cellId = Bootstrap_->GetCellId(cellTag);
        auto* mailbox = hiveManager->GetOrCreateMailbox(cellId);
        YCHECK(MasterMailboxCache_.emplace(cellTag, mailbox).second);
        return mailbox;
    }


    void OnStartSecondaryMasterRegistration()
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        const auto& worldInitializer = Bootstrap_->GetWorldInitializer();
        if (!worldInitializer->IsInitialized()) {
            return;
        }

        if (RegisterState_ != EPrimaryRegisterState::None) {
            return;
        }

        NProto::TReqStartSecondaryMasterRegistration request;
        CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
            ->CommitAndLog(Logger);
    }

    void OnCellStatisticsGossip()
    {
        YCHECK(Bootstrap_->IsSecondaryMaster());

        if (!IsLocalMasterCellRegistered()) {
            return;
        }

        LOG_INFO("Sending cell statistics gossip message");

        NProto::TReqSetCellStatistics request;
        request.set_cell_tag(Bootstrap_->GetCellTag());
        *request.mutable_statistics() = GetLocalCellStatistics();
        PostToMaster(request, PrimaryMasterCellTag, false);
    }

    NProto::TCellStatistics GetLocalCellStatistics()
    {
        NProto::TCellStatistics result;
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        result.set_chunk_count(chunkManager->Chunks().GetSize());
        result.set_lost_vital_chunk_count(chunkManager->LostVitalChunks().size());
        return result;
    }


    TRefCountedEncapsulatedMessagePtr BuildHiveMessage(
        const TCrossCellMessage& crossCellMessage)
    {
        if (const auto* protoPtr = crossCellMessage.Payload.TryAs<TCrossCellMessage::TProtoMessage>()) {
            return NHiveServer::SerializeMessage(*protoPtr->Message);
        }

        NObjectServer::NProto::TReqExecute hydraRequest;
        TSharedRefArray parts;
        if (const auto* clientPtr = crossCellMessage.Payload.TryAs<TCrossCellMessage::TClientMessage>()) {
            parts = clientPtr->Request->Serialize();
        } else if (const auto* servicePtr = crossCellMessage.Payload.TryAs<TCrossCellMessage::TServiceMessage>()) {
            auto requestMessage = servicePtr->Context->GetRequestMessage();
            auto requestHeader = servicePtr->Context->RequestHeader();
            auto updatedYPath = FromObjectId(servicePtr->ObjectId) + GetRequestYPath(requestHeader);
            SetRequestYPath(&requestHeader, updatedYPath);
            parts = SetRequestHeader(requestMessage, requestHeader);
        } else {
            Y_UNREACHABLE();
        }

        for (const auto& part : parts) {
            hydraRequest.add_request_parts(part.Begin(), part.Size());
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        hydraRequest.set_user_name(user->GetName());

        return NHiveServer::SerializeMessage(hydraRequest);
    }

    void DoPostMessage(
        TRefCountedEncapsulatedMessagePtr message,
        const TCellTagList& cellTags,
        bool reliable)
    {
        const auto& hiveManager = Bootstrap_->GetHiveManager();
        TMailboxList mailboxes;
        for (auto cellTag : cellTags) {
            auto* mailbox = FindMasterMailbox(cellTag);
            if (mailbox) {
                mailboxes.push_back(mailbox);
            } else {
                // Failure here indicates an attempt to send a reliable message to the primary master
                // before registering.
                YCHECK(!reliable);
            }
        }
        hiveManager->PostMessage(mailboxes, std::move(message), reliable);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMulticellManager::TMulticellManager(
    TMulticellManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TMulticellManager::~TMulticellManager() = default;

void TMulticellManager::PostToMaster(
    const TCrossCellMessage& message,
    TCellTag cellTag,
    bool reliable)
{
    Impl_->PostToMaster(message, cellTag, reliable);
}

void TMulticellManager::PostToMasters(
    const TCrossCellMessage& message,
    const NObjectClient::TCellTagList& cellTags,
    bool reliable)
{
    Impl_->PostToMasters(message, cellTags, reliable);
}

void TMulticellManager::PostToSecondaryMasters(
    const TCrossCellMessage& message,
    bool reliable)
{
    Impl_->PostToSecondaryMasters(message, reliable);
}

bool TMulticellManager::IsLocalMasterCellRegistered()
{
    return Impl_->IsLocalMasterCellRegistered();
}

bool TMulticellManager::IsRegisteredMasterCell(TCellTag cellTag)
{
    return Impl_->IsRegisteredSecondaryMaster(cellTag);
}

const TCellTagList& TMulticellManager::GetRegisteredMasterCellTags()
{
    return Impl_->GetRegisteredMasterCellTags();
}

int TMulticellManager::GetRegisteredMasterCellIndex(TCellTag cellTag)
{
    return Impl_->GetRegisteredMasterCellIndex(cellTag);
}

TCellTag TMulticellManager::PickSecondaryMasterCell(double bias)
{
    return Impl_->PickSecondaryMasterCell(bias);
}

NProto::TCellStatistics TMulticellManager::ComputeClusterStatistics()
{
    return Impl_->ComputeClusterStatistics();
}

IChannelPtr TMulticellManager::GetMasterChannelOrThrow(TCellTag cellTag, EPeerKind peerKind)
{
    return Impl_->GetMasterChannelOrThrow(cellTag, peerKind);
}

IChannelPtr TMulticellManager::FindMasterChannel(TCellTag cellTag, EPeerKind peerKind)
{
    return Impl_->FindMasterChannel(cellTag, peerKind);
}

TMailbox* TMulticellManager::FindPrimaryMasterMailbox()
{
    return Impl_->FindPrimaryMasterMailbox();
}

DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), ValidateSecondaryMasterRegistration, *Impl_);
DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), ReplicateKeysToSecondaryMaster, *Impl_);
DELEGATE_SIGNAL(TMulticellManager, void(TCellTag), ReplicateValuesToSecondaryMaster, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
