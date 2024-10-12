#include "persistent_mailbox_state.h"

#include "hive_manager.h"

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NHiveServer {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

void TPersistentMailboxState::TOutcomingMessage::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, SerializedMessage->Type);
    Save(context, SerializedMessage->Data);
    Save(context, Time);
}

void TPersistentMailboxState::TOutcomingMessage::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    SerializedMessage = New<TSerializedMessage>();
    Load(context, SerializedMessage->Type);
    Load(context, SerializedMessage->Data);
    if (context.GetVersion() >= 7) {
        Load(context, Time);
    }
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

} // namespace NYT::NHiveServer
