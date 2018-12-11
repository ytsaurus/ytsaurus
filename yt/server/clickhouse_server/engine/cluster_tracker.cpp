#include "cluster_tracker.h"

#include "auth_token.h"
#include "format_helpers.h"
#include "guarded_ptr.h"
#include "logging_helpers.h"
#include "type_helpers.h"

//#include <Common/Exception.h>

//#include <Poco/Logger.h>

//#include <common/logger_useful.h>

#include <util/string/cast.h>
#include <util/system/rwlock.h>

#include <memory>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

using NNative::NonexistingNodeRevision;

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeTracker;

using TClusterNodeTrackerPtr = std::shared_ptr<TClusterNodeTracker>;

////////////////////////////////////////////////////////////////////////////////

// Event handler for cluster node directory

// We need that indirection because tracker holds reference to context object
// and do not control its lifetime

class TClusterDirectoryEventHandler
    : public NNative::INodeEventHandler
{
private:
    TGuardedPtr<TClusterNodeTracker> Tracker;

public:
    TClusterDirectoryEventHandler(TClusterNodeTrackerPtr tracker)
        : Tracker(std::move(tracker))
    {}

    void OnUpdate(
        const TString& path,
        NNative::TNodeRevision newRevision) override;

    void OnRemove(const TString& path) override;

    void OnError(
        const TString& path,
        const TString& errorMessage) override;

    void Detach();
};

using TClusterDirectoryEventHandlerPtr = std::shared_ptr<TClusterDirectoryEventHandler>;

////////////////////////////////////////////////////////////////////////////////

namespace NEphemeralNodes {

static TClusterNodeName ToClusterNodeName(const THashMap<TString, TString>& attributes)
{
    return {.Host = attributes.at("host"), .Port = FromString<uint64_t>(attributes.at("tcp_port"))  };
}

}   // namespace NEphemeralNodes

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeTracker
    : public IClusterNodeTracker
    , public std::enable_shared_from_this<TClusterNodeTracker>
{
    using TClusterNodeMap = std::unordered_map<TClusterNodeName, IClusterNodePtr>;
private:
    NNative::IDirectoryPtr Directory;
    TClusterDirectoryEventHandlerPtr EventHandler{nullptr};

    TClusterNodeMap ClusterNodes;
    TRWMutex RWMutex;

    Settings Settings_;
    uint64_t ClickHousePort_;

    Poco::Logger* Logger;

public:
    TClusterNodeTracker(
        NNative::IDirectoryPtr directory,
        uint64_t clickhousePort);

    void StartTrack(const Context& context) override;
    void StopTrack() override;

    TClusterNodeTicket EnterCluster(const std::string& id, const std::string& host, ui16 tcpPort, ui16 httpPort) override;

    TClusterNodeNames ListAvailableNodes() override;
    TClusterNodes GetAvailableNodes() override;

    // Notifications

    void OnUpdate(NNative::TNodeRevision newRevision);
    void OnRemove();
    void OnError(const TString& errorMessage);

private:
    TClusterDirectoryEventHandlerPtr CreateEventHandler();
    TClusterNodeNames ProcessNodeList(NNative::TDirectoryListing listing);
    void UpdateClusterNodes(const TClusterNodeNames& nodeNames);
};

////////////////////////////////////////////////////////////////////////////////

TClusterNodeTracker::TClusterNodeTracker(
    NNative::IDirectoryPtr directory,
    uint64_t clickhousePort)
    : Directory(std::move(directory))
    , ClickHousePort_(clickhousePort)
    , Logger(&Poco::Logger::get("ClusterNodeTracker"))
{
}

void TClusterNodeTracker::StartTrack(const Context& context)
{
    Settings_ = context.getSettingsRef();
    EventHandler = CreateEventHandler();
    Directory->SubscribeToUpdate(/*expectedRevision=*/ NonexistingNodeRevision, EventHandler);
}

void TClusterNodeTracker::StopTrack()
{
    EventHandler->Detach();
}

TClusterNodeTicket TClusterNodeTracker::EnterCluster(const std::string& id, const std::string& host, ui16 tcpPort, ui16 httpPort)
{
    THashMap<TString, TString> attributes;
    attributes["host"] = host;
    attributes["tcp_port"] = ::ToString(tcpPort);
    attributes["http_port"] = ::ToString(httpPort);

    auto result = Directory->CreateAndKeepEphemeralNode(TString(id), attributes);

    // Synchronously update cluster directory.
    CH_LOG_DEBUG(Logger, "Forcing cluster directory update");
    OnUpdate(static_cast<NNative::TNodeRevision>(-2));
    return result;
}

TClusterNodeNames TClusterNodeTracker::ListAvailableNodes()
{
    auto nodes = GetAvailableNodes();

    TClusterNodeNames listing;
    for (const auto& node : nodes) {
        listing.insert(node->GetName());
    }
    return listing;
}

TClusterNodes TClusterNodeTracker::GetAvailableNodes()
{
    TReadGuard guard(RWMutex);

    TClusterNodes nodes;
    nodes.reserve(ClusterNodes.size());
    for (const auto& entry : ClusterNodes) {
         nodes.push_back(entry.second);
    }
    return nodes;
}

void TClusterNodeTracker::OnUpdate(NNative::TNodeRevision newRevision)
{
    CH_LOG_DEBUG(Logger, "Cluster directory updated: new revision = " << newRevision);

    NNative::TDirectoryListing listing;

    try {
        listing = Directory->ListNodes();
    } catch (...) {
        CH_LOG_WARNING(Logger, "Failed to list cluster directory: " << CurrentExceptionText());
        Directory->SubscribeToUpdate(NonexistingNodeRevision, EventHandler);
        return;
    }

    TClusterNodeNames nodeNames;
    try {
        nodeNames = ProcessNodeList(listing);
    } catch (...) {
        CH_LOG_WARNING(Logger, "Failed to process cluster nodes list: " << CurrentExceptionText());
    }

    UpdateClusterNodes(nodeNames);

    Directory->SubscribeToUpdate(listing.Revision, EventHandler);
}

void TClusterNodeTracker::OnRemove()
{
    CH_LOG_WARNING(Logger, "Cluster directory removed");
    Directory->SubscribeToUpdate(NonexistingNodeRevision, EventHandler);
}

void TClusterNodeTracker::OnError(const TString& errorMessage)
{
    CH_LOG_WARNING(Logger, "Error occurred during cluster directory polling: " << ToStdString(errorMessage));
    Directory->SubscribeToUpdate(NonexistingNodeRevision, EventHandler);
}

TClusterDirectoryEventHandlerPtr TClusterNodeTracker::CreateEventHandler()
{
    return std::make_shared<TClusterDirectoryEventHandler>(shared_from_this());
}

TClusterNodeNames TClusterNodeTracker::ProcessNodeList(NNative::TDirectoryListing listing)
{
    CH_LOG_INFO(
        Logger,
        "Discovered " << listing.Children.size() <<
        " node(s) in cluster directory at revision " << listing.Revision);

    TClusterNodeNames nodeNames;

    for (const auto& node : listing.Children) {
        auto name = NEphemeralNodes::ToClusterNodeName(node.Attributes);

        CH_LOG_DEBUG(
            Logger,
            "Discovered cluster node: " << name.ToString() <<
            ", ephemeral node name = " << ToStdString(node.Name));

        nodeNames.insert(name);
    }

    return nodeNames;
}

void TClusterNodeTracker::UpdateClusterNodes(const TClusterNodeNames& newNodeNames)
{
    TWriteGuard guard(RWMutex);

    TClusterNodeMap newClusterNodes;

    for (const auto& nodeName : newNodeNames) {
        auto found = ClusterNodes.find(nodeName);

        if (found != ClusterNodes.end()) {
            newClusterNodes.emplace(nodeName, found->second);
        } else {
            IClusterNodePtr newNode;
            try {
                newNode = CreateClusterNode(nodeName, Settings_, ClickHousePort_);
            } catch (...) {
                CH_LOG_WARNING(Logger, "Failed to create cluster node " << nodeName.ToString());
                // TODO: reschedule
                continue;
            }
            newClusterNodes.emplace(nodeName, newNode);
        }
    }

    ClusterNodes = std::move(newClusterNodes);
}

////////////////////////////////////////////////////////////////////////////////

void TClusterDirectoryEventHandler::OnUpdate(
    const TString& /*path*/,
    NNative::TNodeRevision newRevision)
{
    if (auto tracker = Tracker.Lock()) {
        tracker->OnUpdate(newRevision);
    }
}

void TClusterDirectoryEventHandler::OnRemove(
    const TString& /*path*/)
{
    if (auto tracker = Tracker.Lock()) {
        tracker->OnRemove();
    }
}

void TClusterDirectoryEventHandler::OnError(
    const TString& /*path*/,
    const TString& errorMessage)
{
    if (auto tracker = Tracker.Lock()) {
        tracker->OnError(errorMessage);
    }
}

void TClusterDirectoryEventHandler::Detach()
{
    Tracker.Release();
}

////////////////////////////////////////////////////////////////////////////////

IClusterNodeTrackerPtr CreateClusterNodeTracker(
    NNative::ICoordinationServicePtr coordinationService,
    NNative::IAuthorizationTokenPtr authToken,
    const std::string directoryPath,
    uint64_t clickhousePort)
{
    auto directory = coordinationService->OpenOrCreateDirectory(
        *authToken,
        ToString(directoryPath));

    return std::make_shared<TClusterNodeTracker>(
        std::move(directory),
        clickhousePort);
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
