#include "mailbox.h"

#include <yt/server/hydra/composite_automaton.h>

#include <yt/ytlib/hive/proto/hive_service.pb.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

namespace NYT::NHiveServer {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

struct TEncapsulatedMessageSerializer
{
    template <class C>
    static void Save(C& context, const TRefCountedEncapsulatedMessagePtr& message)
    {
        NHiveClient::NProto::TEncapsulatedMessage sanitizedMessage(*message);
        sanitizedMessage.clear_trace_id();
        sanitizedMessage.clear_span_id();
        sanitizedMessage.clear_parent_span_id();
        NYT::Save(context, sanitizedMessage);
    }

    template <class C>
    static void Load(C& context, TRefCountedEncapsulatedMessagePtr& message)
    {
        message = New<TRefCountedEncapsulatedMessage>();
        NYT::Load(context, *message);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMailbox::TMailbox(TCellId cellId)
    : CellId_(cellId)
{ }

void TMailbox::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, FirstOutcomingMessageId_);
    TVectorSerializer<TEncapsulatedMessageSerializer>::Save(context, OutcomingMessages_);
    Save(context, NextIncomingMessageId_);
}

void TMailbox::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, FirstOutcomingMessageId_);
    TVectorSerializer<TEncapsulatedMessageSerializer>::Load(context, OutcomingMessages_);
    Load(context, NextIncomingMessageId_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
