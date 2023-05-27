#include "mailbox.h"
#include "helpers.h"
#include "hive_manager.h"

#include <yt/yt/ytlib/hive/proto/hive_service.pb.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NHiveServer {

using namespace NHydra;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void TPersistentMailboxState::TOutcomingMessage::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, SerializedMessage->Type);
    Save(context, SerializedMessage->Data);
}

void TPersistentMailboxState::TOutcomingMessage::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    SerializedMessage = New<TSerializedMessage>();
    Load(context, SerializedMessage->Type);
    Load(context, SerializedMessage->Data);
}

void TPersistentMailboxState::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, FirstOutcomingMessageId_);
    Save(context, OutcomingMessages_);
    Save(context, NextPersistentIncomingMessageId_);
}

void TPersistentMailboxState::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, FirstOutcomingMessageId_);
    Load(context, OutcomingMessages_);
    Load(context, NextPersistentIncomingMessageId_);
}

void ToProto(
    NProto::TPersistentMailboxState* protoMailbox,
    const TPersistentMailboxState& mailbox)
{
    using NYT::ToProto;

    protoMailbox->set_first_outcoming_message_id(mailbox.GetFirstOutcomingMessageId());
    protoMailbox->set_next_persistent_incoming_message_id(mailbox.GetNextPersistentIncomingMessageId());

    for (const auto& outcomingMessage : mailbox.OutcomingMessages()) {
        const auto& message = outcomingMessage.SerializedMessage;
        auto* protoMessage = protoMailbox->add_outcoming_messages();
        ToProto(protoMessage->mutable_type(), message->Type);
        ToProto(protoMessage->mutable_data(), message->Data);
    }
}

void FromProto(
    TPersistentMailboxState* mailbox,
    const NProto::TPersistentMailboxState& protoMailbox)
{
    using NYT::FromProto;

    mailbox->SetFirstOutcomingMessageId(protoMailbox.first_outcoming_message_id());
    mailbox->SetNextPersistentIncomingMessageId(protoMailbox.next_persistent_incoming_message_id());

    for (const auto& protoMessage : protoMailbox.outcoming_messages()) {
        auto message = New<TSerializedMessage>();
        FromProto(&message->Type, protoMessage.type());
        FromProto(&message->Data, protoMessage.data());
        mailbox->OutcomingMessages().push_back(TPersistentMailboxState::TOutcomingMessage{
            .SerializedMessage = std::move(message),
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

TMailbox::TMailbox(TCellId cellId)
    : EndpointId_(cellId)
    , RuntimeData_(New<TMailboxRuntimeData>())
{ }

void TMailbox::Save(TSaveContext& context) const
{
    TPersistentMailboxState::Save(context);
}

void TMailbox::Load(TLoadContext& context)
{
    TPersistentMailboxState::Load(context);

    UpdateLastOutcomingMessageId();
}

void TMailbox::UpdateLastOutcomingMessageId()
{
    RuntimeData_->LastOutcomingMessageId.store(
        FirstOutcomingMessageId_ +
        static_cast<int>(OutcomingMessages_.size()) - 1);
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

////////////////////////////////////////////////////////////////////////////////

TCellId TCellMailbox::GetCellId() const
{
    return EndpointId_;
}

////////////////////////////////////////////////////////////////////////////////

bool TAvenueMailbox::IsActive() const
{
    return !OutcomingMessages_.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
