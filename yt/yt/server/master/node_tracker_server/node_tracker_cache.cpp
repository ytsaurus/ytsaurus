#include "node_tracker_cache.h"

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NNodeTrackerServer {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(INodeTrackerCache)

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerCache
    : public INodeTrackerCache
{
public:
    void ResetNodeDefaultAddresses() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        NodeDefaultAddresses_.Store(THashMap<TNodeId, std::string>{});
    }

    void UpdateNodeDefaultAddress(TNodeId nodeId, std::optional<TStringBuf> defaultAddress) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        NodeDefaultAddresses_.Transform([=] (auto& defaultAddresses) {
            if (defaultAddress.has_value()) {
                defaultAddresses[nodeId] = *defaultAddress;
            } else {
                defaultAddresses.erase(nodeId);
            }
        });
    }

    std::string GetNodeDefaultAddressOrThrow(TNodeId nodeId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NodeDefaultAddresses_.Read([=] (const auto& nodeAddresses) {
            auto it = nodeAddresses.find(nodeId);
            if (it == nodeAddresses.end()) {
                THROW_ERROR_EXCEPTION(
                    NNodeTrackerClient::EErrorCode::NoSuchNode,
                    "Invalid or expired node id %v",
                    nodeId);
            }
            return it->second;
        });
    }

private:
    TAtomicObject<THashMap<TNodeId, std::string>> NodeDefaultAddresses_;
};

////////////////////////////////////////////////////////////////////////////////

INodeTrackerCachePtr CreateNodeTrackerCache()
{
    return New<TNodeTrackerCache>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
