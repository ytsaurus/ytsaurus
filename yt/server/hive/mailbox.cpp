#include "mailbox.h"

#include <yt/server/hydra/composite_automaton.h>

#include <yt/ytlib/hive/hive_service.pb.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NHiveServer {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

struct TEncapsulatedMessageSerializer
{
    template <class C>
    static void Save(C& context, const TRefCountedEncapsulatedMessagePtr& message)
    {
        NYT::Save(context, *message);
    }

    template <class C>
    static void Load(C& context, TRefCountedEncapsulatedMessagePtr& message)
    {
        message = New<TRefCountedEncapsulatedMessage>();
        NYT::Load(context, *message);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMailbox::TMailbox(const TCellId& cellId)
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

    // COMPAT(babenko)
    if (context.GetVersion() < 3) {
        Load(context, FirstOutcomingMessageId_);
        // LastIncomingMessage_ differs from NextIncomingMessageId_ by 1.
        NextIncomingMessageId_ = Load<TMessageId>(context) + 1;
        TVectorSerializer<TEncapsulatedMessageSerializer>::Load(context, OutcomingMessages_);
        // IncomingMessages_ must be empty.
        YCHECK(TSizeSerializer::Load(context) == 0);
    } else {
        Load(context, FirstOutcomingMessageId_);
        TVectorSerializer<TEncapsulatedMessageSerializer>::Load(context, OutcomingMessages_);
        Load(context, NextIncomingMessageId_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
