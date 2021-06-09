#include "node_status_directory.h"

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TTrivialNodeStatusDirectory
    : public INodeStatusDirectory
{
public:
    virtual void UpdateSuspicionMarkTime(
        const TString& /*nodeAddress*/,
        bool /*suspicious*/,
        std::optional<TInstant> /*previousMarkTime*/)
    { }

    virtual std::vector<std::optional<TInstant>> RetrieveSuspicionMarkTimes(
        const std::vector<TString>& nodeAddresses) const
    {
        return std::vector<std::optional<TInstant>>(nodeAddresses.size(), std::nullopt);
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
