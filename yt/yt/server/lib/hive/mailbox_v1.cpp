#include "mailbox_v1.h"
#include "helpers.h"

#include <yt/yt/ytlib/hive/proto/hive_service.pb.h>

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NHiveServer::NV1 {

using namespace NHydra;
using namespace NObjectClient;

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

} // namespace NYT::NHiveServer::NV1
