#include "replication_card_collocation.h"

#include "serialize.h"
#include "replication_card.h"

#include <yt/yt/client/tablet_client/config.h>

namespace NYT::NChaosNode {

using namespace NChaosClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TReplicationCardCollocation::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, ReplicationCards_);
    Save(context, State_);
    Save(context, Size_);
    Save(context, *Options_);
}

void TReplicationCardCollocation::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, ReplicationCards_);
    Load(context, State_);
    Load(context, Size_);
    if (context.GetVersion() >= EChaosReign::CollocationOptions) {
        Load(context, *Options_);
    }
}

bool TReplicationCardCollocation::IsMigrating() const
{
    return State_ != EReplicationCardCollocationState::Normal;
}

void TReplicationCardCollocation::ValidateNotMigrating() const
{
    if (IsMigrating()) {
        THROW_ERROR_EXCEPTION(NChaosClient::EErrorCode::ReplicationCollocationIsMigrating,
            "Collocation %v is in %Qlv state",
            Id_,
            State_);
    }
}

std::vector<TReplicationCardId> TReplicationCardCollocation::GetReplicationCardIds() const
{
    std::vector<TReplicationCardId> result;
    result.reserve(ReplicationCards_.size());
    for (auto* replicationCard : ReplicationCards_) {
        result.push_back(replicationCard->GetId());
    }
    std::sort(result.begin(), result.end());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
