#include "incumbent_detail.h"

#include "incumbent_manager.h"
#include "private.h"

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = IncumbentServerLogger;

////////////////////////////////////////////////////////////////////////////////

TIncumbentBase::TIncumbentBase(
    IIncumbentManagerPtr incumbentManager,
    EIncumbentType type)
    : IncumbentManager_(std::move(incumbentManager))
    , Type_(type)
{ }

EIncumbentType TIncumbentBase::GetType() const
{
    return Type_;
}

////////////////////////////////////////////////////////////////////////////////

TShardedIncumbentBase::TShardedIncumbentBase(
    IIncumbentManagerPtr incumbentManager,
    EIncumbentType type)
    : TIncumbentBase(
        std::move(incumbentManager),
        type)
    , ActiveShardIndices_(NIncumbentClient::GetIncumbentShardCount(type))
{ }

bool TShardedIncumbentBase::IsShardActive(int shardIndex) const
{
    return ActiveShardIndices_[shardIndex];
}

int TShardedIncumbentBase::GetShardCount() const
{
    return GetIncumbentShardCount(GetType());
}

int TShardedIncumbentBase::GetActiveShardCount() const
{
    return ActiveShardCount_;
}

std::vector<int> TShardedIncumbentBase::ListActiveShardIndices() const
{
    std::vector<int> result;
    result.reserve(ActiveShardCount_);
    for (int index = 0; index < std::ssize(ActiveShardIndices_); ++index) {
        if (ActiveShardIndices_[index]) {
            result.push_back(index);
        }
    }
    return result;
}

void TShardedIncumbentBase::OnIncumbencyStarted(int shardIndex)
{
    if (ActiveShardIndices_[shardIndex]) {
        YT_LOG_ALERT("Attempt to re-start incumbency (IncumbentType: %v, ShardIndex: %v)",
            GetType(),
            shardIndex);
        return;
    }

    ActiveShardIndices_[shardIndex] = true;
    ++ActiveShardCount_;
}

void TShardedIncumbentBase::OnIncumbencyFinished(int shardIndex)
{
    if (ActiveShardIndices_[shardIndex]) {
        ActiveShardIndices_[shardIndex] = false;
        --ActiveShardCount_;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
