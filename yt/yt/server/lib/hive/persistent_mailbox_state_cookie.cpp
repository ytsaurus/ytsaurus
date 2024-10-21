#include "persistent_mailbox_state_cookie.h"

#include "hive_manager.h"

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NHiveServer {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

void TOutcomingMessage::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, SerializedMessage->Type);
    Save(context, SerializedMessage->Data);
    Save(context, Time);
}

void TOutcomingMessage::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    SerializedMessage = New<TSerializedMessage>();
    Load(context, SerializedMessage->Type);
    Load(context, SerializedMessage->Data);
    if (context.GetVersion() >= 7) {
        Load(context, Time);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TPersistentMailboxStateCookie* protoCookie,
    const TPersistentMailboxStateCookie& cookie)
{
    using NYT::ToProto;

    protoCookie->set_first_outcoming_message_id(cookie.FirstOutcomingMessageId);
    protoCookie->set_next_persistent_incoming_message_id(cookie.NextPersistentIncomingMessageId);

    for (const auto& outcomingMessage : cookie.OutcomingMessages) {
        const auto& message = outcomingMessage.SerializedMessage;
        auto* protoMessage = protoCookie->add_outcoming_messages();
        ToProto(protoMessage->mutable_type(), message->Type);
        ToProto(protoMessage->mutable_data(), message->Data);
    }
}

void FromProto(
    TPersistentMailboxStateCookie* cookie,
    const NProto::TPersistentMailboxStateCookie& protoCookie)
{
    using NYT::FromProto;

    cookie->FirstOutcomingMessageId = protoCookie.first_outcoming_message_id();
    cookie->NextPersistentIncomingMessageId = protoCookie.next_persistent_incoming_message_id();

    for (const auto& protoMessage : protoCookie.outcoming_messages()) {
        auto message = New<TSerializedMessage>();
        FromProto(&message->Type, protoMessage.type());
        FromProto(&message->Data, protoMessage.data());
        cookie->OutcomingMessages.push_back(TOutcomingMessage{
            .SerializedMessage = std::move(message),
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
