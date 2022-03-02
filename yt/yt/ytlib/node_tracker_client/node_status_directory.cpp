#include "node_status_directory.h"

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TTrivialNodeStatusDirectory
    : public INodeStatusDirectory
{
public:
    void UpdateSuspicionMarkTime(
        TNodeId /*nodeId*/,
        TStringBuf /*address*/,
        bool /*suspicious*/,
        std::optional<TInstant> /*previousMarkTime*/) override
    { }

    std::vector<std::optional<TInstant>> RetrieveSuspicionMarkTimes(
        const std::vector<TNodeId>& nodeIds) const override
    {
        return std::vector<std::optional<TInstant>>(nodeIds.size(), std::nullopt);
    }

    THashMap<TNodeId, TInstant> RetrieveSuspiciousNodeIdsWithMarkTime(
        const std::vector<TNodeId>& /*nodeIds*/) const override
    {
        return {};
    }

    bool ShouldMarkNodeSuspicious(const TError& /*error*/) const override
    {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeStatusDirectoryPtr CreateTrivialNodeStatusDirectory()
{
    return New<TTrivialNodeStatusDirectory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
