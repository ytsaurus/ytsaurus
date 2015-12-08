#include "mailbox.h"

#include <yt/server/hydra/composite_automaton.h>

#include <yt/ytlib/hive/hive_service.pb.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NHive {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TMailbox::TMailbox(const TCellId& cellId)
    : CellId_(cellId)
    , FirstOutcomingMessageId_(0)
    , LastIncomingMessageId_(-1)
    , PostMessagesInFlight_(false)
    , Connected_(false)
{ }

void TMailbox::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, FirstOutcomingMessageId_);
    Save(context, LastIncomingMessageId_);
    Save(context, OutcomingMessages_);
    Save(context, IncomingMessages_);
}

void TMailbox::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, FirstOutcomingMessageId_);
    Load(context, LastIncomingMessageId_);
    Load(context, OutcomingMessages_);
    Load(context, IncomingMessages_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
