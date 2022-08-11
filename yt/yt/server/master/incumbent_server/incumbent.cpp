#include "incumbent.h"

#include "incumbent_manager.h"

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

TIncumbentBase::TIncumbentBase(IIncumbentManagerPtr incumbentManager)
    : IncumbentManager_(std::move(incumbentManager))
{ }

bool TIncumbentBase::HasIncumbency(int shardIndex) const
{
    return IncumbentManager_->HasIncumbency(GetType(), shardIndex);
}

int TIncumbentBase::GetShardCount() const
{
    return GetIncumbentShardCount(GetType());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
