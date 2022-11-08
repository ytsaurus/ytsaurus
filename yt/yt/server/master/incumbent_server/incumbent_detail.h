#pragma once

#include "incumbent.h"

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

class TIncumbentBase
    : public IIncumbent
{
public:
    TIncumbentBase(
        IIncumbentManagerPtr incumbentManager,
        EIncumbentType type);

protected:
    EIncumbentType GetType() const override;

private:
    const IIncumbentManagerPtr IncumbentManager_;
    const EIncumbentType Type_;
};

////////////////////////////////////////////////////////////////////////////////

class TShardedIncumbentBase
    : public TIncumbentBase
{
public:
    TShardedIncumbentBase(
        IIncumbentManagerPtr incumbentManager,
        EIncumbentType type);

protected:
    void OnIncumbencyStarted(int shardIndex) override;
    void OnIncumbencyFinished(int shardIndex) override;

    bool IsShardActive(int shardIndex) const;
    int GetShardCount() const;
    int GetActiveShardCount() const;
    std::vector<int> ListActiveShardIndices() const;

private:
    std::vector<bool> ActiveShardIndices_;
    int ActiveShardCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
