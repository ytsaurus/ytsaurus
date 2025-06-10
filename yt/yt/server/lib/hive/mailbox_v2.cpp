#include "mailbox_v2.h"

#include "helpers.h"
#include "persistent_mailbox_state_cookie.h"

#include <yt/yt/ytlib/hive/proto/hive_service.pb.h>

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NHiveServer::NV2 {

using namespace NHydra;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

bool TMessageIdRange::IsEmpty() const
{
    return Begin == End;
}

i64 TMessageIdRange::GetCount() const
{
    return End - Begin;
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentMailboxState::Save(TSaveContext& context) const
{
    using NYT::Save;

    OutcomingMessages_.Read([&] (const auto& outcomingMessages) {
        Save(context, outcomingMessages.FirstId);
        Save(context, outcomingMessages.Messages);
    });

    Save(context, GetNextPersistentIncomingMessageId());
}

void TPersistentMailboxState::Load(TLoadContext& context)
{
    using NYT::Load;

    OutcomingMessages_.Transform([&] (auto& outcomingMessages) {
        Load(context, outcomingMessages.FirstId);
        Load(context, outcomingMessages.Messages);
    });

    SetNextPersistentIncomingMessageId(Load<TMessageId>(context));
}

TPersistentMailboxStateCookie TPersistentMailboxState::SaveToCookie() const
{
    return OutcomingMessages_.Read([&] (const auto& outcomingMessages) {
        return TPersistentMailboxStateCookie{
            .NextPersistentIncomingMessageId = GetNextPersistentIncomingMessageId(),
            .FirstOutcomingMessageId = outcomingMessages.FirstId,
            .OutcomingMessages = {outcomingMessages.Messages.begin(), outcomingMessages.Messages.end()},
        };
    });
}

void TPersistentMailboxState::LoadFromCookie(TPersistentMailboxStateCookie&& cookie)
{
    OutcomingMessages_.Transform([&] (auto& outcomingMessages) {
        outcomingMessages.FirstId = cookie.FirstOutcomingMessageId;
        outcomingMessages.Messages.clear();
        for (auto&& message : cookie.OutcomingMessages) {
            outcomingMessages.Messages.push_back(std::move(message));
        }
    });

    SetNextPersistentIncomingMessageId(cookie.NextPersistentIncomingMessageId);
}

TMessageId TPersistentMailboxState::GetNextPersistentIncomingMessageId() const
{
    return NextPersistentIncomingMessageId_.load();
}

void TPersistentMailboxState::SetNextPersistentIncomingMessageId(TMessageId id)
{
    NextPersistentIncomingMessageId_.store(id);
}

TMessageId TPersistentMailboxState::AddOutcomingMessage(TOutcomingMessage message)
{
    return OutcomingMessages_.Transform([&] (auto& outcomingMessages) -> TMessageId {
        outcomingMessages.Messages.push_back(std::move(message));
        return outcomingMessages.FirstId + std::ssize(outcomingMessages.Messages) - 1;
    });
}

TMessageId TPersistentMailboxState::TrimLastestOutcomingMessages(int count)
{
    return OutcomingMessages_.Transform([&] (auto& outcomingMessages) -> TMessageId {
        YT_VERIFY(count <= std::ssize(outcomingMessages.Messages));
        outcomingMessages.Messages.erase(outcomingMessages.Messages.begin(), outcomingMessages.Messages.begin() + count);
        outcomingMessages.FirstId += count;
        return outcomingMessages.FirstId;
    });
}

TMessageIdRange TPersistentMailboxState::GetOutcomingMessageIdRange() const
{
    return OutcomingMessages_.Read([&] (const auto& outcomingMessages) -> TMessageIdRange {
        return {outcomingMessages.FirstId, outcomingMessages.FirstId + std::ssize(outcomingMessages.Messages)};
    });
}

void TPersistentMailboxState::IterateOutcomingMessages(
    TMessageId firstMessageId,
    const std::function<bool(const TOutcomingMessage&)>& visitor) const
{
    OutcomingMessages_.Read([&] (const auto& outcomingMessages) {
        for (auto currentMessageId = firstMessageId;
            currentMessageId < outcomingMessages.FirstId + std::ssize(outcomingMessages.Messages);
            ++currentMessageId)
        {
            if (!visitor(outcomingMessages.Messages[currentMessageId - outcomingMessages.FirstId])) {
                break;
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TMailboxRuntimeData::TMailboxRuntimeData(
    bool isLeader,
    TEndpointId endpointId,
    TPersistentMailboxStatePtr persistentState)
    : IsLeader(isLeader)
    , EndpointId(endpointId)
    , PersistentState(std::move(persistentState))
    , NextTransientIncomingMessageId(PersistentState->GetNextPersistentIncomingMessageId())
{ }

////////////////////////////////////////////////////////////////////////////////

TMailbox::TMailbox(TEndpointId endpointId)
    : EndpointId_(endpointId)
{ }

void TMailbox::Save(TSaveContext& context) const
{
    PersistentState_->Save(context);
}

void TMailbox::Load(TLoadContext& context)
{
    PersistentState_->Load(context);
}

bool TMailbox::IsCell() const
{
    return !IsAvenue();
}

bool TMailbox::IsAvenue() const
{
    return IsAvenueEndpointType(TypeFromId(EndpointId_));
}

TCellMailbox* TMailbox::AsCell()
{
    YT_VERIFY(IsCell());
    return static_cast<TCellMailbox*>(this);
}

const TCellMailbox* TMailbox::AsCell() const
{
    YT_VERIFY(IsCell());
    return static_cast<const TCellMailbox*>(this);
}

TAvenueMailbox* TMailbox::AsAvenue()
{
    YT_VERIFY(IsAvenue());
    return static_cast<TAvenueMailbox*>(this);
}

const TAvenueMailbox* TMailbox::AsAvenue() const
{
    YT_VERIFY(IsAvenue());
    return static_cast<const TAvenueMailbox*>(this);
}

const TPersistentMailboxStatePtr& TMailbox::GetPersistentState() const
{
    return PersistentState_;
}

void TMailbox::RecreatePersistentState()
{
    auto cookie = PersistentState_->SaveToCookie();
    PersistentState_ = New<TPersistentMailboxState>();
    PersistentState_->LoadFromCookie(std::move(cookie));
}

const TMailboxRuntimeDataPtr& TMailbox::GetRuntimeData() const
{
    return RuntimeData_;
}

////////////////////////////////////////////////////////////////////////////////

TCellMailboxRuntimeData::TCellMailboxRuntimeData(
    const NProfiling::TProfiler& profiler,
    bool isLeader,
    TEndpointId endpointId,
    TPersistentMailboxStatePtr persistentState,
    TIntrusivePtr<NConcurrency::TAsyncBatcher<void>> synchronizationBatcher)
    : TMailboxRuntimeData(
        isLeader,
        endpointId,
        std::move(persistentState))
    , SyncBatcher(std::move(synchronizationBatcher))
    , SyncTimeGauge(profiler.WithTag("source_cell_id", ToString(EndpointId)).TimeGauge("/cell_sync_time"))
    , OutcomingMessageQueueSizeGauge(profiler
        .WithTag("target_cell_id", ToString(EndpointId))
        .WithTag("target_cell_tag", ToString(CellTagFromId(EndpointId)))
        .Gauge("/outcoming_messages_queue_size"))
{ }

////////////////////////////////////////////////////////////////////////////////

TCellId TCellMailbox::GetCellId() const
{
    return EndpointId_;
}

const TCellMailboxRuntimeDataPtr& TCellMailbox::GetRuntimeData() const
{
    return RuntimeData_;
}

void TCellMailbox::SetRuntimeData(TCellMailboxRuntimeDataPtr runtimeData)
{
    TMailbox::RuntimeData_ = RuntimeData_ = std::move(runtimeData);
}

////////////////////////////////////////////////////////////////////////////////

const TAvenueMailboxRuntimeDataPtr& TAvenueMailbox::GetRuntimeData() const
{
    return RuntimeData_;
}

void TAvenueMailbox::SetRuntimeData(TAvenueMailboxRuntimeDataPtr runtimeData)
{
    TMailbox::RuntimeData_ = RuntimeData_ = std::move(runtimeData);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer::NV2
