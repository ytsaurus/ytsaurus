#include "node_status_directory.h"

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TTrivialNodeStatusDirectory
    : public INodeStatusDirectory
{
public:
    virtual void UpdateSuspicionMarkTime(
        TNodeId /*nodeId*/,
        TStringBuf /*address*/,
        bool /*suspicious*/,
        std::optional<TInstant> /*previousMarkTime*/)
    { }

    virtual std::vector<std::optional<TInstant>> RetrieveSuspicionMarkTimes(
        const std::vector<TNodeId>& nodeIds) const
    {
        return std::vector<std::optional<TInstant>>(nodeIds.size(), std::nullopt);
    }

    virtual std::vector<std::pair<TNodeId, TInstant>> RetrieveSuspiciousNodeIdsWithMarkTime(
        const std::vector<TNodeId>& /*nodeIds*/) const
    {
        return {};
    }

    virtual bool ShouldMarkNodeSuspicious(const TError& /*error*/) const
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
