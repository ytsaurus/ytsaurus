#include "stdafx.h"
#include "mailbox.h"

#include <core/misc/serialize.h>
#include <core/misc/protobuf_helpers.h>

#include <core/rpc/channel.h>

#include <server/hydra/composite_automaton.h>

namespace NYT {
namespace NHive {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TMailbox::TMailbox(const TCellId& cellId)
    : CellId_(cellId)
    , FirstOutcomingMessageId_(0)
    , LastIncomingMessageId_(-1)
    , InFlightMessageCount_(0)
    , Connected_(false)
{ }

void TMailbox::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, CellId_);
    Save(context, FirstOutcomingMessageId_);
    Save(context, LastIncomingMessageId_);
    Save(context, OutcomingMessages_);
    Save(context, IncomingMessages_);
}

void TMailbox::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, CellId_);
    Load(context, FirstOutcomingMessageId_);
    Load(context, LastIncomingMessageId_);
    Load(context, OutcomingMessages_);
    Load(context, IncomingMessages_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
