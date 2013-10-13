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

void TMessage::Save(TSaveContext& context ) const
{
    using NYT::Save;

    Save(context, Type);
    Save(context, Data);
}

void TMessage::Load(TLoadContext& context )
{
    using NYT::Load;

    Load(context, Type);
    Load(context, Data);
}

////////////////////////////////////////////////////////////////////////////////

TMailbox::TMailbox(const TCellGuid& cellGuid)
    : CellGuid_(cellGuid)
    , FirstPendingMessageId_(0)
    , LastReceivedMessageId_(-1)
    , InFlightMessageCount_(0)
    , Connected_(false)
{ }

void TMailbox::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, CellGuid_);
    Save(context, FirstPendingMessageId_);
    Save(context, LastReceivedMessageId_);
    Save(context, PendingMessages_);
}

void TMailbox::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, CellGuid_);
    Load(context, FirstPendingMessageId_);
    Load(context, LastReceivedMessageId_);
    Load(context, PendingMessages_);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NHive
} // namespace NYT
