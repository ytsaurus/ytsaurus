#include "mailbox.h"
#include "hive_manager.h"

//#include <yt/server/lib/hydra/composite_automaton.h>

#include <yt/ytlib/hive/proto/hive_service.pb.h>

//#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

namespace NYT::NHiveServer {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

void TMailbox::TOutcomingMessage::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, SerializedMessage->Type);
    Save(context, SerializedMessage->Data);
}

void TMailbox::TOutcomingMessage::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    SerializedMessage = New<TSerializedMessage>();
    // COMPAT(babenko)
    if (context.GetVersion() < 5) {
        NHiveClient::NProto::TEncapsulatedMessage message;
        Load(context, message);
        SerializedMessage->Type = message.type();
        SerializedMessage->Data = message.data();
    } else {
        Load(context, SerializedMessage->Type);
        Load(context, SerializedMessage->Data);
    }
}

////////////////////////////////////////////////////////////////////////////////

TMailbox::TMailbox(TCellId cellId)
    : CellId_(cellId)
{ }

void TMailbox::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, FirstOutcomingMessageId_);
    Save(context, OutcomingMessages_);
    Save(context, NextPersistentIncomingMessageId_);
}

void TMailbox::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, FirstOutcomingMessageId_);
    Load(context, OutcomingMessages_);
    Load(context, NextPersistentIncomingMessageId_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
