#include "sequoia_attribute_fetcher.h"

#include "helpers.h"
#include "sequoia_session.h"
#include "sequoia_tree_visitor.h"

#include <yt/yt/server/master/security_server/detailed_master_memory.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/core/ytree/attribute_filter.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <stack>


namespace NYT::NCypressProxy {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

// TAttributeFetcherBase is used as common base for all attribute fetchers.
// The class sends requests to master and parses it's response to nodes with attributes.
class TAttributeFetcherBase
    : public ISequoiaAttributeFetcher
{
public:
    TAttributeFetcherBase(
        const IClientPtr client,
        const TSequoiaSessionPtr sequoiaSession)
        : Client_(client)
        , SequoiaSession_(sequoiaSession)
        , RequestTemplate_(TYPathProxy::Get())
    {
        SetSuppressAccessTracking(RequestTemplate_, true);
        SetSuppressExpirationTimeoutRenewal(RequestTemplate_, true);
    }

    virtual void SetNodesForGetRequest(
        TNodeId rootId,
        const std::vector<TNodeId>& /*nodesToFetchFromMaster*/)
    {
        SetRootNodeId(rootId);
    }

    virtual void SetNodesForListRequest(
        TNodeId rootId,
        const std::vector<TCypressChildDescriptor>& /*children*/)
    {
        SetRootNodeId(rootId);
    }

    virtual void SetSingleNode(TNodeId nodeId)
    {
        SetRootNodeId(nodeId);
    }

protected:
    const IClientPtr Client_;
    const TSequoiaSessionPtr SequoiaSession_;

    TYPathProxy::TReqGetPtr RequestTemplate_;
    std::vector<TNodeId> NodesToFetchFromMaster_;

    TFuture<THashMap<TNodeId, INodePtr>> FetchAttributesFromMaster()
    {
        VectorizedGetBatcher_ = std::make_unique<TMasterYPathProxy::TVectorizedGetBatcher>(
            Client_,
            RequestTemplate_,
            NodesToFetchFromMaster_,
            SequoiaSession_->GetCurrentCypressTransactionId());

        return VectorizedGetBatcher_->Invoke().Apply(
            BIND([rootNodeId = RootNodeId_] (const TMasterYPathProxy::TVectorizedGetBatcher::TVectorizedResponse& nodeIdToRspOrError) {
                THashMap<TNodeId, INodePtr> nodeIdToAttributes;

                for (const auto& [nodeId, rspOrError] : nodeIdToRspOrError) {
                    if (rspOrError.IsOK()) {
                        EmplaceOrCrash(
                            nodeIdToAttributes,
                            nodeId,
                            ConvertToNode(TYsonString(rspOrError.Value()->value())));
                    } else {
                        auto wrappedResolveError = WrapRetriableResolveError(rspOrError, nodeId);
                        if (wrappedResolveError.IsOK()) {
                            // Not a resolve error. Pass it through.
                            THROW_ERROR_EXCEPTION("Error getting requested information from master")
                                << rspOrError;
                        }

                        // A race on the target node should be retried.
                        if (nodeId == rootNodeId) {
                            THROW_ERROR wrappedResolveError;
                        }

                        // A race on a nested node should lead to that node being omitted silently.
                        // This leaves open the possibility of the classic kvk1920 race: even if a node
                        // was recreated via Create(path, force=true), we still may observe its absence.
                        continue;
                    }
                }
                return nodeIdToAttributes;
            }));
    }

    TNodeId GetRootNodeId() const
    {
        return RootNodeId_;
    }

private:

    TNodeId RootNodeId_;
    std::unique_ptr<TMasterYPathProxy::TVectorizedGetBatcher> VectorizedGetBatcher_;

    void SetRootNodeId(TNodeId rootNodeId)
    {
        // NB: even if RootNodeId_ == rootNodeId, this would still be unexpected usage pattern.
        YT_LOG_ALERT_IF(RootNodeId_,
            "Incorrect attribute fetcher usage detected: root node ID set multiple times "
            "(OldRootNodeId: %v, NewRootNodeId: %v)",
            RootNodeId_,
            rootNodeId);
        RootNodeId_ = rootNodeId;
    }

};

////////////////////////////////////////////////////////////////////////////////

// TSimpleAttributeFetcher fetches all attributes that are stored on master without changing anything.
class TSimpleAttributeFetcher
    : public TAttributeFetcherBase
{
public:
    TSimpleAttributeFetcher(
        const IClientPtr client,
        const TSequoiaSessionPtr sequoiaSession,
        const TAttributeFilter& attributeFilter)
        : TAttributeFetcherBase(client, sequoiaSession)
    {
        if (attributeFilter) {
            ToProto(RequestTemplate_->mutable_attributes(), attributeFilter);
        }
    }

    void SetNodesForGetRequest(
        TNodeId rootId,
        const std::vector<TNodeId>& nodesToFetchFromMaster) override
    {
        TAttributeFetcherBase::SetNodesForGetRequest(rootId, nodesToFetchFromMaster);
        NodesToFetchFromMaster_ = nodesToFetchFromMaster;
    }

    void SetNodesForListRequest(
        TNodeId rootId,
        const std::vector<TCypressChildDescriptor>& children) override
    {
        TAttributeFetcherBase::SetNodesForListRequest(rootId, children);

        for (const auto& child : children) {
            NodesToFetchFromMaster_.push_back(child.ChildId);
        }
    }

    void SetSingleNode(TNodeId nodeId) override
    {
        TAttributeFetcherBase::SetSingleNode(nodeId);

        NodesToFetchFromMaster_.push_back(nodeId);
    }

    void SetScalarOnlyNodesForGetRequest(const std::vector<TNodeId>& nodesToFetchFromMaster)
    {
        for (const auto& nodeId : nodesToFetchFromMaster) {
            auto nodeType = TypeFromId(nodeId);
            if (IsScalarType(nodeType)) {
                NodesToFetchFromMaster_.push_back(nodeId);
            }
        }
    }

    TFuture<THashMap<TNodeId, INodePtr>> FetchNodesWithAttributes() override
    {
        return FetchAttributesFromMaster();
    }
};

////////////////////////////////////////////////////////////////////////////////

// Keep consistent with NSecurityServer::TClusterResources.
// The reasons these are distinct classes are
// - Master uses bootsrtap with muster data for serialization and deserialization.
// - Medium indexes are used for DiskSpacePerMedium.

// TODO(grphil): Use NSecurityServer::TClusterResources instead of this class.
class TResourceUsage
{
public:
    i64 NodeCount = 0;
    i64 ChunkCount = 0;
    i64 TabletCount = 0;
    i64 TabletStaticMemory = 0;
    THashMap<std::string, i64> DiskSpacePerMedium;
    i64 ChunkHostCellMasterMemory = 0;
    NSecurityServer::TDetailedMasterMemory DetailedMasterMemory;

    TResourceUsage& operator += (const TResourceUsage& other)
    {
        NodeCount += other.NodeCount;
        ChunkCount += other.ChunkCount;
        TabletCount += other.TabletCount;
        TabletStaticMemory += other.TabletStaticMemory;
        ChunkHostCellMasterMemory += other.ChunkHostCellMasterMemory;
        DetailedMasterMemory += other.DetailedMasterMemory;

        for (const auto& [medium, space] : other.DiskSpacePerMedium) {
            DiskSpacePerMedium[medium] += space;
        }

        return *this;
    }
};

void Serialize(const TResourceUsage& resourceUsage, IYsonConsumer* consumer)
{
    i64 totalDiskSpace = 0;
    for (const auto& [medium, space] : resourceUsage.DiskSpacePerMedium) {
        totalDiskSpace += space;
    }

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_count").Value(resourceUsage.NodeCount)
            .Item("chunk_count").Value(resourceUsage.ChunkCount)
            .Item("tablet_count").Value(resourceUsage.TabletCount)
            .Item("tablet_static_memory").Value(resourceUsage.TabletStaticMemory)
            .Item("disk_space_per_medium").Value(resourceUsage.DiskSpacePerMedium)
            .Item("disk_space").Value(totalDiskSpace)
            .Item("chunk_host_cell_master_memory").Value(resourceUsage.ChunkHostCellMasterMemory)
            .Item("detailed_master_memory").Value(resourceUsage.DetailedMasterMemory)
            .Item("master_memory").Value(resourceUsage.DetailedMasterMemory.GetTotal())
        .EndMap();
}

void Deserialize(TResourceUsage& resourceUsage, NYTree::INodePtr node)
{
    if (node->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Expected map node for resource usage serialization, got %v instead", node->GetType());
    }
    auto map = node->AsMap();

    auto getInt64ValueOrDefault = [&] (const char* key) -> i64 {
        if (auto child = map->FindChild(key)) {
            return child->AsInt64()->GetValue();
        }
        return 0;
    };

    resourceUsage.NodeCount = getInt64ValueOrDefault("node_count");
    resourceUsage.ChunkCount = getInt64ValueOrDefault("chunk_count");
    resourceUsage.TabletCount = getInt64ValueOrDefault("tablet_count");
    resourceUsage.TabletStaticMemory = getInt64ValueOrDefault("tablet_static_memory");

    if (auto diskSpacePerMedium = map->FindChild("disk_space_per_medium")) {
        Deserialize(resourceUsage.DiskSpacePerMedium, diskSpacePerMedium);
    }

    resourceUsage.ChunkHostCellMasterMemory = getInt64ValueOrDefault("chunk_host_cell_master_memory");

    if (auto detailedMasterMemory = map->FindChild("detailed_master_memory")) {
        Deserialize(resourceUsage.DetailedMasterMemory, detailedMasterMemory);
    }
}

void Deserialize(TResourceUsage& resourceUsage, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(resourceUsage, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

class TNodeRecursiveAttributeCalculator
    : public INodeVisitor<TCypressNodeDescriptor>
{
public:
    TNodeRecursiveAttributeCalculator(
        const THashMap<TNodeId, INodePtr>* fetchedNodes,
        const THashSet<TNodeId>* requestedNodes)
        : FetchedNodes_(fetchedNodes)
        , RequestedNodes_(requestedNodes)
    { }

    const THashMap<TNodeId, INodePtr>& GetNodesWithCalculatedAttributes()
    {
        return NodesWithCalculatedAttributes_;
    }

private:
    const THashMap<TNodeId, INodePtr>* FetchedNodes_;
    const THashSet<TNodeId>* RequestedNodes_;

    THashMap<TNodeId, INodePtr> NodesWithCalculatedAttributes_;

    struct TNodeStackEntry
    {
        TResourceUsage ResourceUsage;
        INodePtr NodePtr;
    };

    std::stack<TNodeStackEntry, std::vector<TNodeStackEntry>> AttributesStack_;

    void OnNodeEntered(const TCypressNodeDescriptor& node) override
    {
        auto& stackEntry = AttributesStack_.emplace();

        if (auto it = FetchedNodes_->find(node.Id); it != FetchedNodes_->end()) {
            // In case of race between get and some other actions with nodes we may have no responses for some nodes.
            // Just like the error in Cf. IAttributeFetcherBase::FetchAttributesFromMaster.
            stackEntry.NodePtr = it->second;
            auto& attributes = stackEntry.NodePtr->Attributes();

            if (auto optionalResourceUsage = attributes.Find<TResourceUsage>("resource_usage")) {
                stackEntry.ResourceUsage = std::move(*optionalResourceUsage);
            }
        }
    }

    void OnNodeExited(const TCypressNodeDescriptor& node) override
    {
        auto [exitedNodeResourceUsage, exitedNodePtr] = std::move(AttributesStack_.top());
        AttributesStack_.pop();

        if (!AttributesStack_.empty() && exitedNodePtr) {
            AttributesStack_.top().ResourceUsage += exitedNodeResourceUsage;
        }

        if (RequestedNodes_->contains(node.Id) && exitedNodePtr) {
            exitedNodePtr->MutableAttributes()->Set(
                EInternedAttributeKey::RecursiveResourceUsage.Unintern(),
                exitedNodeResourceUsage);
            NodesWithCalculatedAttributes_[node.Id] = exitedNodePtr;
        }
    }

    bool ShouldContinue() override
    {
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////


// TRecursiveAttributeFetcher is used for fetching special attributes, that are calculating while traversing the subtree.
class TRecursiveAttributeFetcher
    : public TAttributeFetcherBase
{
public:
    TRecursiveAttributeFetcher(
        const IClientPtr client,
        const TSequoiaSessionPtr sequoiaSession,
        const TAttributeFilter& attributeFilter)
        : TAttributeFetcherBase(client, sequoiaSession)
    {
        YT_ASSERT(attributeFilter);
        ToProto(RequestTemplate_->mutable_attributes(), attributeFilter);
    }

    void SetNodesForGetRequest(
        TNodeId rootId,
        const std::vector<TNodeId>& nodesToFetchFromMaster) override
    {
        TAttributeFetcherBase::SetNodesForGetRequest(rootId, nodesToFetchFromMaster);
        for (const auto& nodeId : nodesToFetchFromMaster) {
            RequestedNodes_.insert(nodeId);
        }
    }

    void SetNodesForListRequest(
        TNodeId parentId,
        const std::vector<TCypressChildDescriptor>& children) override
    {
        TAttributeFetcherBase::SetNodesForListRequest(parentId, children);

        for (const auto& node : children) {
            RequestedNodes_.insert(node.ChildId);
        }
    }

    void SetSingleNode(TNodeId nodeId) override
    {
        TAttributeFetcherBase::SetSingleNode(nodeId);

        RequestedNodes_.insert(nodeId);
    }

    TFuture<THashMap<TNodeId, INodePtr>> FetchNodesWithAttributes() override
    {
        auto resolvedRoot = SequoiaSession_->FindNodePath(GetRootNodeId());
        if (!resolvedRoot) {
            YT_LOG_ALERT("Can not resolve node path for sequoia node (NodeId: %v)", GetRootNodeId());
            THROW_ERROR_EXCEPTION("Node path not found for node %v", GetRootNodeId());
        }

        auto subtree = SequoiaSession_->FetchSubtree(resolvedRoot->Path);

        for (const auto& node : subtree.Nodes) {
            NodesToFetchFromMaster_.push_back(node.Id);
        }

        return FetchAttributesFromMaster().Apply(
            BIND([subtree = std::move(subtree), this, this_ = MakeStrong(this)] (const THashMap<TNodeId, INodePtr>& resourceUsage) {
                TNodeRecursiveAttributeCalculator attributesCalculator(&resourceUsage, &RequestedNodes_);

                auto isAncestorCallback = [] (const TCypressNodeDescriptor& maybeAncestor, const TCypressNodeDescriptor& child) {
                    return child.Path.Underlying().StartsWith(maybeAncestor.Path.Underlying());
                };

                TraverseSequoiaTree(
                    std::move(subtree.Nodes),
                    &attributesCalculator,
                    isAncestorCallback);

                return attributesCalculator.GetNodesWithCalculatedAttributes();
            }));
    }

private:
    THashSet<TNodeId> RequestedNodes_;
};

////////////////////////////////////////////////////////////////////////////////

// TCompositeSequoiaAttributeFetcher is used for fetching all attributes for sequoia nodes.
// The attributes are fetched and calculated using additional fetchers.
class TCompositeSequoiaAttributeFetcher
    : public ISequoiaAttributeFetcher
{
    DECLARE_NEW_FRIEND()

public:
    using TCompositeSequoiaAttributeFetcherPtr = TIntrusivePtr<TCompositeSequoiaAttributeFetcher>;

    static TCompositeSequoiaAttributeFetcherPtr ForGetRequest(
        const IClientPtr client,
        const TSequoiaSessionPtr sequoiaSession,
        const TAttributeFilter& attributeFilter,
        TNodeId rootId,
        const std::vector<TNodeId>& nodesToFetchFromMaster)
    {
        auto result = New<TCompositeSequoiaAttributeFetcher>(
            client,
            sequoiaSession,
            attributeFilter,
            /*fetchSimpleAttributes*/ true);
        result->SetNodesForGetRequest(rootId, nodesToFetchFromMaster);
        return result;
    }

    static TCompositeSequoiaAttributeFetcherPtr ForListRequest(
        const IClientPtr client,
        const TSequoiaSessionPtr sequoiaSession,
        const TAttributeFilter& attributeFilter,
        const TNodeId parentId,
        const std::vector<TCypressChildDescriptor>& children)
    {
        auto result = New<TCompositeSequoiaAttributeFetcher>(
            client,
            sequoiaSession,
            attributeFilter,
            /*fetchSimpleAttributes*/ true);
        result->SetNodesForListRequest(parentId, children);
        return result;
    }

    static TCompositeSequoiaAttributeFetcherPtr ForSingleNode(
        const IClientPtr client,
        const TSequoiaSessionPtr sequoiaSession,
        const TAttributeFilter& attributeFilter,
        const TNodeId nodeId)
    {
        auto result = New<TCompositeSequoiaAttributeFetcher>(
            client,
            sequoiaSession,
            attributeFilter,
            /*fetchSimpleAttributes*/ false);
        result->SetSingleNode(nodeId);
        return result;
    }

    const TAttributeFilter& GetAttributeFilter() const
    {
        return AttributeFilter_;
    }

private:
    // NB: private ctor protects from multiple calls to any of
    // SetNodesForGetRequest, SetNodesForListRequest and SetSingleNode which
    // would lead to setting RootNodeId_ multiple times which, in turn, would
    // complicate the logic for retrying "no such object" errors.
    TCompositeSequoiaAttributeFetcher(
        const IClientPtr client,
        const TSequoiaSessionPtr sequoiaSession,
        const TAttributeFilter& attributeFilter,
        bool fetchSimpleAttributes)
        : Client_(client)
        , SequoiaSession_(sequoiaSession)
        , AttributeFilter_(attributeFilter)
    {
        if (AttributeFilter_) {
            AttributeFilterKeys_ = AttributeFilter_.Normalize();
        }

        MaybeCreateRecursiveAttributeFetcher();

        if (fetchSimpleAttributes) {
            MaybeCreateSimpleAttributeFetcher();
        }
    }

    void SetNodesForGetRequest(
        TNodeId rootId,
        const std::vector<TNodeId>& nodesToFetchFromMaster)
    {
        for (auto& fetcher : Fetchers_) {
            fetcher->SetNodesForGetRequest(rootId, nodesToFetchFromMaster);
        }

        // For get request we will need to fetch values for scalar nodes.
        // If the simple attribute fetcher is created, the values will be fetched along with attributes.
        // Otherwise, we will need to fetch create simple attribute fetcher and use it to fetch values for scalar nodes.
        if (!IsSimpleAttributeFetcherCreated_) {
            auto simpleAttributesFetcher = New<TSimpleAttributeFetcher>(Client_, SequoiaSession_, TAttributeFilter());
            simpleAttributesFetcher->SetScalarOnlyNodesForGetRequest(nodesToFetchFromMaster);
            Fetchers_.push_back(simpleAttributesFetcher);
        }
    }

    void SetNodesForListRequest(
        TNodeId parentId,
        const std::vector<TCypressChildDescriptor>& children)
    {
        for (auto& fetcher : Fetchers_) {
            fetcher->SetNodesForListRequest(parentId, children);
        }
    }

    void SetSingleNode(TNodeId nodeId)
    {
        for (auto& fetcher : Fetchers_) {
            fetcher->SetSingleNode(nodeId);
        }
    }

    TFuture<THashMap<TNodeId, INodePtr>> FetchNodesWithAttributes() override
    {
        std::vector<TFuture<THashMap<TNodeId, INodePtr>>> attributeFutures;
        for (auto& fetcher : Fetchers_) {
            attributeFutures.push_back(fetcher->FetchNodesWithAttributes());
        }

        // We add the simple fetcher last, but it will probably contain the most attributes.
        // So we will process its result first for optimization purposes.
        std::ranges::reverse(attributeFutures);

        return AllSet(std::move(attributeFutures)).Apply(
            BIND([] (const std::vector<TErrorOr<THashMap<TNodeId, INodePtr>>>& fetchedNodes) {
                THashMap<TNodeId, INodePtr> result;

                for (const auto& nodeMapOrError : fetchedNodes) {
                    auto& nodeMap = nodeMapOrError.ValueOrThrow();

                    for (const auto& [nodeId, node] : nodeMap) {
                        auto [it, inserted] = result.emplace(nodeId, node);
                        if (inserted) {
                            continue;
                        }
                        auto* attributeDictionary = it->second->MutableAttributes();

                        for (const auto& [attribute, value] : node->Attributes().ListPairs()) {
                            if (attributeDictionary->FindYson(attribute)) {
                                THROW_ERROR_EXCEPTION("Attribute %v is fetched with multiple attribute fetchers", attribute);
                            } else {
                                attributeDictionary->SetYson(attribute, value);
                            }
                        }
                    }
                }

                return result;
            }));
    }

    void MaybeCreateRecursiveAttributeFetcher()
    {
        if (!AttributeFilterKeys_.contains(EInternedAttributeKey::RecursiveResourceUsage.Unintern())) {
            return;
        }

        TAttributeFilter recursiveAttributeFilter({EInternedAttributeKey::ResourceUsage.Unintern()});

        Fetchers_.push_back(New<TRecursiveAttributeFetcher>(
            Client_,
            SequoiaSession_,
            recursiveAttributeFilter
        ));

        // The resource usage will also be fetched by the recursive attribute fetcher in case recursive resource usage is required.
        AttributeFilter_.Remove({EInternedAttributeKey::ResourceUsage.Unintern(), EInternedAttributeKey::RecursiveResourceUsage.Unintern()});
    }

    void MaybeCreateSimpleAttributeFetcher()
    {
        if (AttributeFilter_ && !AttributeFilter_.IsEmpty()) {
            Fetchers_.push_back(New<TSimpleAttributeFetcher>(
                Client_,
                SequoiaSession_,
                AttributeFilter_
            ));
            IsSimpleAttributeFetcherCreated_ = true;
        }
    }

    const IClientPtr Client_;
    const TSequoiaSessionPtr SequoiaSession_;

    TAttributeFilter AttributeFilter_;
    TAttributeFilter::TKeyToFilter AttributeFilterKeys_;

    std::vector<TIntrusivePtr<TAttributeFetcherBase>> Fetchers_;
    bool IsSimpleAttributeFetcherCreated_ = false;
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaAttributeFetcherPtr CreateAttributeFetcherForGetRequest(
    const IClientPtr client,
    const TSequoiaSessionPtr sequoiaSession,
    const TAttributeFilter& attributeFilter,
    const TNodeId rootId,
    const std::vector<TNodeId>& nodesToFetchFromMaster)
{
    return TCompositeSequoiaAttributeFetcher::ForGetRequest(client, sequoiaSession, attributeFilter, rootId, nodesToFetchFromMaster);
}

ISequoiaAttributeFetcherPtr CreateAttributeFetcherForListRequest(
    const IClientPtr client,
    const TSequoiaSessionPtr sequoiaSession,
    const TAttributeFilter& attributeFilter,
    const TNodeId parentId,
    const std::vector<TCypressChildDescriptor>& children)
{
    return TCompositeSequoiaAttributeFetcher::ForListRequest(client, sequoiaSession, attributeFilter, parentId, children);
}

std::tuple<ISequoiaAttributeFetcherPtr, TAttributeFilter> CreateSpecialAttributeFetcherAndLeftAttributesForNode(
    const IClientPtr client,
    const TSequoiaSessionPtr sequoiaSession,
    const TAttributeFilter& attributeFilter,
    const TNodeId rootId)
{
    auto attributeFetcher = TCompositeSequoiaAttributeFetcher::ForSingleNode(
        client,
        sequoiaSession,
        attributeFilter,
        rootId);
    std::tuple<ISequoiaAttributeFetcherPtr, TAttributeFilter> result{nullptr, attributeFetcher->GetAttributeFilter()};
    std::get<ISequoiaAttributeFetcherPtr>(result) = std::move(attributeFetcher);
    return result;
}

} // namespace NYT::NCypressServer
