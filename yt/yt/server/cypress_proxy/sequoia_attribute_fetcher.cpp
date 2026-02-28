#include "sequoia_attribute_fetcher.h"

#include "helpers.h"
#include "sequoia_session.h"
#include "sequoia_tree_visitor.h"

#include <yt/yt/server/master/security_server/detailed_master_memory.h>

#include <yt/yt/server/lib/security_server/helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/yt/core/ytree/attribute_filter.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>


namespace NYT::NCypressProxy {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NSequoiaClient;
using namespace NServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

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

    TResourceUsage& operator+=(const TResourceUsage& other)
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

// "Frontier" is a set of a subtree nodes with children outside of the requested scope.
// This class traveres the global Sequoia tree and accumulates recursive attributes
// statistics for these nodes.
class TFrontierRecursiveAttributeCalculator
    : public INodeVisitor<TCypressNodeDescriptor>
{
public:
    TFrontierRecursiveAttributeCalculator(
        const TNodeIdToAttributes* fetchedAttributes,
        const THashSet<TNodeId>* requestedNodes,
        THashMap<TNodeId, TResourceUsage>* frontierNodesResourceUsage)
        : FetchedAttributes_(fetchedAttributes)
        , RequestedNodes_(requestedNodes)
        , FrontierNodesResourceUsage_(frontierNodesResourceUsage)
    {
        YT_VERIFY(fetchedAttributes);
        YT_VERIFY(requestedNodes);
        YT_VERIFY(frontierNodesResourceUsage);
    }

private:
    const TNodeIdToAttributes* const FetchedAttributes_;
    const THashSet<TNodeId>* const RequestedNodes_;
    THashMap<TNodeId, TResourceUsage>* const FrontierNodesResourceUsage_;

    std::vector<std::optional<TResourceUsage>> RequestedNodeStack_;

    bool ShouldVisit(const TCypressNodeDescriptor& descriptor) override
    {
        if (RequestedNodes_->contains(descriptor.Id)) {
            return true;
        }

        const auto& node = GetOrCrash(*FetchedAttributes_, descriptor.Id);
        return !std::holds_alternative<TMissingNodeTag>(node);
    }

    void OnNodeEntered(const TCypressNodeDescriptor& descriptor) override
    {
        if (RequestedNodes_->contains(descriptor.Id)) {
            RequestedNodeStack_.emplace_back();
            return;
        }

        auto& resourceUsage = RequestedNodeStack_.back();
        if (!resourceUsage) {
            resourceUsage.emplace();
        }

        const auto& node = GetOrCrash(*FetchedAttributes_, descriptor.Id);
        const auto* attributes = std::get_if<IAttributeDictionaryPtr>(&node);
        YT_VERIFY(attributes);

        if (auto value = (*attributes)->Find<TResourceUsage>(EInternedAttributeKey::ResourceUsage.Unintern())) {
            *resourceUsage += *value;
        }
    }

    void OnNodeExited(const TCypressNodeDescriptor& descriptor) override
    {
        if (!RequestedNodes_->contains(descriptor.Id)) {
            return;
        }

        if (auto& resourceUsage = RequestedNodeStack_.back()) {
            EmplaceOrCrash(*FrontierNodesResourceUsage_, descriptor.Id, std::move(*resourceUsage));
        }

        RequestedNodeStack_.pop_back();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRecursiveAttributeCalculator
    : public INodeVisitor<TCypressChildDescriptor>
{
public:
    TRecursiveAttributeCalculator(
        const TNodeIdToAttributes* fetchedAttributes,
        const THashMap<TNodeId, TResourceUsage>* frontierNodesResourceUsage,
        TNodeId rootId)
        : FetchedAttributes_(fetchedAttributes)
        , FrontierNodesResourceUsage_(frontierNodesResourceUsage)
        , RootId_(rootId)
        , NodeStack_({TResourceUsage{}}) // Initialize struct for the root.
    {
        YT_VERIFY(fetchedAttributes);
        YT_VERIFY(frontierNodesResourceUsage);
    }

    void Finalize()
    {
        YT_VERIFY(std::ssize(NodeStack_) == 1);
        ProcessNode(RootId_);
    }

private:
    const TNodeIdToAttributes* const FetchedAttributes_;
    const THashMap<TNodeId, TResourceUsage>* const FrontierNodesResourceUsage_;
    const TNodeId RootId_;

    std::vector<TResourceUsage> NodeStack_;

    bool ShouldVisit(const TCypressChildDescriptor& descriptor) override
    {
        const auto& node = GetOrCrash(*FetchedAttributes_, descriptor.ChildId);
        return !std::holds_alternative<TMissingNodeTag>(node);
    }

    void OnNodeEntered(const TCypressChildDescriptor& /*descriptor*/) override
    {
        NodeStack_.emplace_back();
    }

    void OnNodeExited(const TCypressChildDescriptor& descriptor) override
    {
        YT_VERIFY(std::ssize(NodeStack_) > 1);
        ProcessNode(descriptor.ChildId);
    }

    IAttributeDictionary* GetNodeAttributes(TNodeId nodeId)
    {
        const auto& node = GetOrCrash(*FetchedAttributes_, nodeId);
        return Visit(node,
            [] (const INodePtr& node) {
                return node->MutableAttributes();
            },
            [] (const IAttributeDictionaryPtr& attributes) {
                return attributes.Get();
            },
            [] (TMissingNodeTag) -> IAttributeDictionary* {
                YT_ABORT();
            });
    }

    void ProcessNode(TNodeId nodeId)
    {
        auto* attributes = GetNodeAttributes(nodeId);
        auto resourceUsage = std::move(NodeStack_.back());
        NodeStack_.pop_back();

        if (auto value = attributes->Find<TResourceUsage>(EInternedAttributeKey::ResourceUsage.Unintern())) {
            resourceUsage += *value;
        }

        if (auto it = FrontierNodesResourceUsage_->find(nodeId);
            it != FrontierNodesResourceUsage_->end())
        {
            resourceUsage += it->second;
        }

        attributes->Set(
            EInternedAttributeKey::RecursiveResourceUsage.Unintern(),
            resourceUsage);

        if (nodeId != RootId_) {
            NodeStack_.back() += resourceUsage; // Propagate to the parent.
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires std::is_same_v<T, TInstant> || std::is_same_v<T, TDuration>
struct TEffectiveExpirationField
{
    T Value;
    TString Path;
};

struct TEffectiveExpiration
{
    std::optional<TEffectiveExpirationField<TInstant>> Time;
    std::optional<TEffectiveExpirationField<TDuration>> Timeout;
};

template <class T>
void Serialize(const TEffectiveExpirationField<T>& field, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("value").Value(field.Value)
            .Item("path").Value(field.Path)
        .EndMap();
}

void Serialize(const TEffectiveExpiration& effectiveExpiration, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("time").Value(effectiveExpiration.Time)
            .Item("timeout").Value(effectiveExpiration.Timeout)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

class TEffectiveAttributeCalculator
    : public INodeVisitor<TCypressChildDescriptor>
{
public:
    static TEffectiveAttributeCalculator Initialize(
        const std::vector<TInternedAttributeKey>& attributeKeys,
        const TNodeIdToAttributes* fetchedAttributes,
        TNodeAncestry rootAncestry)
    {
        auto calculator = TEffectiveAttributeCalculator(attributeKeys, fetchedAttributes);

        calculator.CurrentStack_.push_back(TInheritedState::CreateEmpty());
        for (const auto& descriptor : rootAncestry) {
            calculator.ProcessNode(descriptor.Id, descriptor.Path, /*serialize*/ false);
        }
        return calculator;
    }

    static TEffectiveAttributeCalculator InheritFrom(
        TEffectiveAttributeCalculator&& other,
        const TNodeIdToAttributes* fetchedAttributes,
        const TCypressNodeDescriptor& root)
    {
        auto calculator = TEffectiveAttributeCalculator(std::move(other), fetchedAttributes, root.Path);
        calculator.ProcessNode(root.Id, root.Path, /*serialize*/ true);
        return calculator;
    }

private:
    const std::vector<TInternedAttributeKey>& AttributeKeys_;
    const TNodeIdToAttributes* const FetchedAttributes_;

    std::optional<TAbsolutePath> CurrentPath_;

    struct TInheritedState
    {
        IConstNodePtr Annotation;
        IConstNodePtr AnnotationPath;
        std::shared_ptr<const TSerializableAccessControlList> Acl;
        TEffectiveExpiration Expiration;
        IConstAttributeDictionaryPtr InheritableAttributes;

        static TInheritedState CreateEmpty()
        {
            static const TInheritedState EmptyState = {
                .Annotation = GetEphemeralNodeFactory()->CreateEntity(),
                .AnnotationPath = GetEphemeralNodeFactory()->CreateEntity(),
                .Acl = std::make_shared<TSerializableAccessControlList>(),
                .Expiration = TEffectiveExpiration{},
                .InheritableAttributes = EmptyAttributes().Clone(),
            };

            return EmptyState;
        }
    };
    std::vector<TInheritedState> CurrentStack_;

    TEffectiveAttributeCalculator(
        const std::vector<TInternedAttributeKey>& attributeKeys,
        const TNodeIdToAttributes* fetchedAttributes)
        : AttributeKeys_(attributeKeys)
        , FetchedAttributes_(fetchedAttributes)
    {
        YT_VERIFY(fetchedAttributes);
    }

    TEffectiveAttributeCalculator(
        TEffectiveAttributeCalculator&& other,
        const TNodeIdToAttributes* fetchedAttributes,
        TAbsolutePath rootPath)
        : AttributeKeys_(other.AttributeKeys_)
        , FetchedAttributes_(fetchedAttributes)
        , CurrentPath_(std::move(rootPath))
        , CurrentStack_(std::move(other.CurrentStack_))
    {
        YT_VERIFY(fetchedAttributes);
    }

    bool ShouldVisit(const TCypressChildDescriptor& descriptor) override
    {
        const auto& node = GetOrCrash(*FetchedAttributes_, descriptor.ChildId);
        return !std::holds_alternative<TMissingNodeTag>(node);
    }

    void OnNodeEntered(const TCypressChildDescriptor& descriptor) override
    {
        CurrentStack_.emplace_back(CurrentStack_.back());
        CurrentPath_->Append(descriptor.ChildKey);
        ProcessNode(descriptor.ChildId, *CurrentPath_, /*serialize*/ true);
    }

    void OnNodeExited(const TCypressChildDescriptor& /*descriptor*/) override
    {
        CurrentPath_->RemoveLastSegment();
        CurrentStack_.pop_back();
    }

    void ProcessNode(TNodeId nodeId, TAbsolutePathBuf path, bool serialize)
    {
        auto& state = CurrentStack_.back();
        auto* node = GetNodeAttributes(nodeId);
        for (const auto& key : AttributeKeys_) {
            switch (key) {
                case EInternedAttributeKey::Annotation: {
                    InheritAnnotationAttribute(key.Unintern(), &state.Annotation, node, serialize);
                    break;
                }
                case EInternedAttributeKey::AnnotationPath: {
                    InheritAnnotationAttribute(key.Unintern(), &state.AnnotationPath, node, serialize);
                    break;
                }
                case EInternedAttributeKey::EffectiveAcl: {
                    InheritEffectiveAclAttribute(&state.Acl, node, serialize);
                    break;
                }
                case EInternedAttributeKey::EffectiveExpiration: {
                    InheritEffectiveExpirationAttribute(&state.Expiration, node, path, serialize);
                    break;
                }
                case EInternedAttributeKey::EffectiveInheritableAttributes: {
                    InheritEffectiveInheritableAttributes(&state.InheritableAttributes, node, serialize);
                    break;
                }
                default:
                    YT_ABORT();
            }
        }
    }

    IAttributeDictionary* GetNodeAttributes(TNodeId nodeId)
    {
        const auto& node = GetOrCrash(*FetchedAttributes_, nodeId);
        return Visit(node,
            [] (const INodePtr& node) {
                return node->MutableAttributes();
            },
            [] (const IAttributeDictionaryPtr& attributes) {
                return attributes.Get();
            },
            [] (TMissingNodeTag) -> IAttributeDictionary* {
                YT_ABORT();
            });
    }

    static void InheritAnnotationAttribute(
        TStringBuf key,
        IConstNodePtr* inheritedState,
        IAttributeDictionary* node,
        bool serialize)
    {
        auto value = node->Get<INodePtr>(key);

        if (value->GetType() != ENodeType::Entity) {
            *inheritedState = value;
        }

        if (serialize) {
            node->Set(key, *inheritedState);
        }
    }

    static void InheritEffectiveAclAttribute(
        std::shared_ptr<const TSerializableAccessControlList>* inheritedState,
        IAttributeDictionary* node,
        bool serialize)
    {
        bool inherit = node->Get<bool>(EInternedAttributeKey::InheritAcl.Unintern());
        auto acl = node->Get<TSerializableAccessControlList>(EInternedAttributeKey::Acl.Unintern());

        if (inherit &&
            acl.Entries.empty() &&
            std::ranges::all_of((*inheritedState)->Entries, [] (const TSerializableAccessControlEntry& ace) {
                return GetInheritedInheritanceMode(ace.InheritanceMode, 1) == ace.InheritanceMode;
            }))
        {
            if (serialize) {
                node->Set(EInternedAttributeKey::EffectiveAcl.Unintern(), **inheritedState);
            }
            return;
        }

        auto effectiveAcl = inherit ? **inheritedState : TSerializableAccessControlList{};

        auto it = std::partition(
            acl.Entries.begin(),
            acl.Entries.end(),
            [] (const TSerializableAccessControlEntry& ace) {
                return GetInheritedInheritanceMode(ace.InheritanceMode, 0).has_value();
            });

        effectiveAcl.Entries.insert(
            effectiveAcl.Entries.end(),
            std::make_move_iterator(acl.Entries.begin()),
            std::make_move_iterator(it));

        if (serialize) {
            node->Set(EInternedAttributeKey::EffectiveAcl.Unintern(), effectiveAcl);
        }

        effectiveAcl.Entries.insert(
            effectiveAcl.Entries.end(),
            std::make_move_iterator(it),
            std::make_move_iterator(acl.Entries.end()));

        std::erase_if(
            effectiveAcl.Entries,
            [] (TSerializableAccessControlEntry& ace) -> bool {
                if (auto inheritedMode = GetInheritedInheritanceMode(ace.InheritanceMode, 1)) {
                    ace.InheritanceMode = *inheritedMode;
                    return false;
                }
                return true;
            });

        *inheritedState = std::make_shared<const TSerializableAccessControlList>(std::move(effectiveAcl));
    }

    static void InheritEffectiveExpirationAttribute(
        TEffectiveExpiration* state,
        IAttributeDictionary* node,
        TAbsolutePathBuf path,
        bool serialize)
    {
        if (auto time = node->Find<TInstant>(EInternedAttributeKey::ExpirationTime.Unintern());
            time && (!state->Time || *time < state->Time->Value))
        {
            auto& field = state->Time.emplace();
            field.Value = *time;
            field.Path = path.ToRealPath().Underlying();
        }

        if (auto timeout = node->Find<TDuration>(EInternedAttributeKey::ExpirationTimeout.Unintern());
            timeout && (!state->Timeout || *timeout < state->Timeout->Value))
        {
            auto& field = state->Timeout.emplace();
            field.Value = *timeout;
            field.Path = path.ToRealPath().Underlying();
        }

        if (serialize) {
            node->Set(EInternedAttributeKey::EffectiveExpiration.Unintern(), *state);
        }
    }

    static void InheritEffectiveInheritableAttributes(
        IConstAttributeDictionaryPtr* inheritedState,
        IAttributeDictionary* node,
        bool serialize)
    {
        const auto& key = EInternedAttributeKey::EffectiveInheritableAttributes.Unintern();

        auto value = node->Find<IMapNodePtr>(key);
        if (!value) {
            return;
        }

        if (value->GetChildCount() > 0) {
            auto inheritableAttributes = (*inheritedState)->Clone();
            inheritableAttributes->MergeFrom(value);
            *inheritedState = std::move(inheritableAttributes);
        }

        if (serialize) {
            node->Set(key, *inheritedState);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETreeScope,
    ((RequestedValues)      (1))
    ((RequestedAttributes)  (2))
    ((Ancestry)             (3))
    ((Descendants)          (4))
);

class TSequoiaAttributeRequest
{
public:
    explicit TSequoiaAttributeRequest(const TAttributeFilter& attributeFilter)
        : AttributeFilter_(attributeFilter)
    {
        if (!AttributeFilter_) {
            return;
        }
        auto attributeKeys = AttributeFilter_.Normalize();
        InitializeRecursiveAttributes(attributeKeys);
        InitializeEffectiveAttributes(attributeKeys);
    }

    const std::vector<TInternedAttributeKey>& GetEffectiveAttributeKeys() const
    {
        return EffectiveAttributeKeys_;
    }

    const std::vector<TInternedAttributeKey>& GetRecursiveAttributeKeys() const
    {
        return RecursiveAttributeKeys_;
    }

    bool ShouldFetchAttributes() const
    {
        return AttributeFilter_.operator bool();
    }

    TAttributeFilter GetBaseAttributeFilter(ETreeScope scope) const
    {
        if (!AttributeFilter_) {
            return AttributeFilter_;
        }

        switch (scope) {
            case ETreeScope::RequestedValues:
            case ETreeScope::RequestedAttributes: {
                auto keys = AttributeFilter_.Keys();
                keys.insert(keys.end(), EffectiveBaseAttributes_.begin(), EffectiveBaseAttributes_.end());
                keys.insert(keys.end(), RecursiveBaseAttributes_.begin(), RecursiveBaseAttributes_.end());
                return TAttributeFilter(keys, AttributeFilter_.Paths());
            }
            case ETreeScope::Ancestry:
                return TAttributeFilter(EffectiveBaseAttributes_);
            case ETreeScope::Descendants:
                return TAttributeFilter(RecursiveBaseAttributes_);
            default:
                YT_ABORT();
        };
    }

private:
    TAttributeFilter AttributeFilter_;
    std::vector<std::string> EffectiveBaseAttributes_;
    std::vector<std::string> RecursiveBaseAttributes_;
    std::vector<TInternedAttributeKey> EffectiveAttributeKeys_;
    std::vector<TInternedAttributeKey> RecursiveAttributeKeys_;

    void InitializeRecursiveAttributes(const TAttributeFilter::TKeyToFilter& attributeKeys)
    {
        if (!attributeKeys.contains(EInternedAttributeKey::RecursiveResourceUsage.Unintern())) {
            return;
        }

        RecursiveAttributeKeys_.push_back(EInternedAttributeKey::RecursiveResourceUsage);
        RecursiveBaseAttributes_.push_back(EInternedAttributeKey::ResourceUsage.Unintern());

        AttributeFilter_.Remove({EInternedAttributeKey::ResourceUsage.Unintern(), EInternedAttributeKey::RecursiveResourceUsage.Unintern()});
    }

    void InitializeEffectiveAttributes(const TAttributeFilter::TKeyToFilter& attributeKeys)
    {
        static constexpr std::array SupportedEffectiveAttributeKeys = {
            EInternedAttributeKey::Annotation,
            EInternedAttributeKey::AnnotationPath,
            EInternedAttributeKey::EffectiveAcl,
            EInternedAttributeKey::EffectiveExpiration,
            EInternedAttributeKey::EffectiveInheritableAttributes,
        };

        std::vector<std::string> effectiveAttributes;

        for (const auto& key : SupportedEffectiveAttributeKeys) {
            const auto& attribute = key.Unintern();
            if (!attributeKeys.contains(attribute)) {
                continue;
            }

            EffectiveAttributeKeys_.push_back(key);
            effectiveAttributes.push_back(attribute);

            switch (key) {
                case EInternedAttributeKey::Annotation: {
                    EffectiveBaseAttributes_.push_back(EInternedAttributeKey::Annotation.Unintern());
                    break;
                }
                case EInternedAttributeKey::AnnotationPath: {
                    EffectiveBaseAttributes_.push_back(EInternedAttributeKey::AnnotationPath.Unintern());
                    break;
                }
                case EInternedAttributeKey::EffectiveAcl: {
                    EffectiveBaseAttributes_.push_back(EInternedAttributeKey::Acl.Unintern());
                    EffectiveBaseAttributes_.push_back(EInternedAttributeKey::InheritAcl.Unintern());
                    break;
                }
                case EInternedAttributeKey::EffectiveExpiration: {
                    EffectiveBaseAttributes_.push_back(EInternedAttributeKey::ExpirationTime.Unintern());
                    EffectiveBaseAttributes_.push_back(EInternedAttributeKey::ExpirationTimeout.Unintern());
                    break;
                }
                case EInternedAttributeKey::EffectiveInheritableAttributes: {
                    EffectiveBaseAttributes_.push_back(EInternedAttributeKey::EffectiveInheritableAttributes.Unintern());
                    break;
                }
                default:
                    break;
            };
        }

        SortUnique(EffectiveBaseAttributes_);
        AttributeFilter_.Remove(effectiveAttributes);
        AttributeFilter_.Remove(EffectiveBaseAttributes_);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires
        std::same_as<T, TNodeId> ||
        std::same_as<T, const std::vector<TCypressChildDescriptor>*> ||
        std::same_as<T, const TNodeIdToChildDescriptors*>
class TSequoiaAttributeFetcher
    : public TRefCounted
{
    using TRequestedNodes = T;

public:
    TSequoiaAttributeFetcher(
        const TSequoiaSessionPtr& sequoiaSession,
        const TAttributeFilter& attributeFilter,
        TNodeId rootId,
        TRequestedNodes requestedNodes,
        TNodeAncestry rootAncestry,
        const std::vector<TNodeId>* scalarNodeIds = nullptr)
        : SequoiaSession_(sequoiaSession)
        , AttributeRequest_(attributeFilter)
        , RootId_(rootId)
        , RequestedNodes_(requestedNodes)
        , RootAncestry_(rootAncestry)
        , ScalarNodeIds_(scalarNodeIds)
    {
        if constexpr (std::is_same_v<T, const TNodeIdToChildDescriptors*>) {
            YT_VERIFY(ScalarNodeIds_);
        }
    }

    TFuture<TNodeIdToAttributes> FetchNodesWithAttributes()
    {
        if (!AttributeRequest_.ShouldFetchAttributes() && (!ScalarNodeIds_ || ScalarNodeIds_->empty())) {
            return MakeFuture<TNodeIdToAttributes>({});
        }

        auto ancestryFuture = MaybePrecomputeEffectiveAttributes().AsUnique();
        auto descendantsFuture = MaybePrecomputeRecursiveAttributes().AsUnique();
        auto baseAttributesFuture = FetchRequestedNodesBaseAttributes().AsUnique();
        auto readyFutures = std::vector{ancestryFuture.AsVoid(), descendantsFuture.AsVoid(), baseAttributesFuture.AsVoid()};

        return AllSucceeded(std::move(readyFutures))
            .Apply(BIND([
                this,
                this_ = MakeStrong(this),
                ancestryFuture = std::move(ancestryFuture),
                descendantsFuture = std::move(descendantsFuture),
                baseAttributesFuture = std::move(baseAttributesFuture)
            ] {
                // NB: AllSucceeded() guarantees that all futures contain values.
                auto fetchedBaseAttributes = baseAttributesFuture.BlockingGet().Value();

                auto traverseRequestedTree = [&] (INodeVisitor<TCypressChildDescriptor>* visitor) {
                    if constexpr (std::is_same_v<T, const std::vector<TCypressChildDescriptor>*>) {
                        constexpr auto isAncestorCallback = [] (const TCypressChildDescriptor&, const TCypressChildDescriptor&) {
                            return false;
                        };
                        TraverseSequoiaTree(*RequestedNodes_, visitor, isAncestorCallback);
                    } else if constexpr (std::is_same_v<T, const TNodeIdToChildDescriptors*>) {
                        TraverseSequoiaTree(RootId_, *RequestedNodes_, visitor);
                    } else if constexpr(!std::is_same_v<T, TNodeId>) {
                        static_assert(false);
                    }
                };

                if (auto inheritedState = ancestryFuture.BlockingGet().Value()) {
                    auto calculator = TEffectiveAttributeCalculator::InheritFrom(
                        std::move(*inheritedState),
                        &fetchedBaseAttributes,
                        RootAncestry_.Back());
                    traverseRequestedTree(&calculator);
                }

                if (auto frontierNodesResourceUsage = descendantsFuture.BlockingGet().Value()) {
                    auto calculator = TRecursiveAttributeCalculator(
                        &fetchedBaseAttributes,
                        &(*frontierNodesResourceUsage),
                        RootAncestry_.Back().Id);
                    traverseRequestedTree(&calculator);
                    // TODO(danilalexeev): YT-26172. This should be a part of tree traversal.
                    calculator.Finalize();
                }

                return fetchedBaseAttributes;
            }));
    }

private:
    const TSequoiaSessionPtr SequoiaSession_;
    const TSequoiaAttributeRequest AttributeRequest_;

    const TNodeId RootId_;
    const TRequestedNodes RequestedNodes_;
    const TNodeAncestry RootAncestry_;
    const std::vector<TNodeId>* ScalarNodeIds_;

    std::ranges::view auto GetRequestedNodeIds() const
    {
        if constexpr (std::is_same_v<T, TNodeId>) {
            return std::views::single(RequestedNodes_);
        } else if constexpr (std::is_same_v<T, const std::vector<TCypressChildDescriptor>*>) {
            return *RequestedNodes_
                | std::views::transform([] (const TCypressChildDescriptor& descriptor) { return descriptor.ChildId; });
        } else if constexpr (std::is_same_v<T, const TNodeIdToChildDescriptors*>) {
            return *RequestedNodes_ | std::views::keys;
        } else {
            static_assert(false);
        }
    }

    TYPathProxy::TReqGetPtr CreateRequestTemplate(ETreeScope scope) const
    {
        auto req = TYPathProxy::Get(scope == ETreeScope::RequestedValues ? "&" : "&/@");
        if (auto attributeFilter = AttributeRequest_.GetBaseAttributeFilter(scope)) {
            ToProto(req->mutable_attributes(), attributeFilter);
        }
        SetSuppressAccessTracking(req, true);
        SetSuppressExpirationTimeoutRenewal(req, true);
        return req;
    }

    template <bool IsScalars>
    static TNodeIdToAttributes ConvertToAttributesMap(
        const std::vector<TNodeId>& nodeIdsToValidateNotMissing,
        const TMasterYPathProxy::TVectorizedGetBatcher::TVectorizedResponse& nodeIdToRspOrError)
    {
        for (auto nodeId : nodeIdsToValidateNotMissing) {
            const auto& rspOrError = GetOrCrash(nodeIdToRspOrError, nodeId);
            if (rspOrError.IsOK()) {
                if (auto error = WrapRetriableResolveError(rspOrError, nodeId); !error.IsOK()) {
                    // A race on the target node should be retried.
                    THROW_ERROR error;
                }
            }
        }

        TNodeIdToAttributes result;
        for (const auto& [nodeId, rspOrError] : nodeIdToRspOrError) {
            auto& value = EmplaceOrCrash(result, nodeId, TMissingNodeTag{})->second;
            if (!rspOrError.IsOK()) {
                if (auto error = WrapRetriableResolveError(rspOrError, nodeId); error.IsOK()) {
                    // Not a resolve error. Pass it through.
                    THROW_ERROR_EXCEPTION("Error getting requested information from master")
                        << rspOrError;
                }

                // A race on a nested node should lead to that node being omitted silently.
                // This leaves open the possibility of the classic kvk1920 race: even if a node
                // was recreated via Create(path, force=true), we still may observe its absence.
                continue;
            }

            auto yson = TYsonString(rspOrError.Value()->value());
            if constexpr (IsScalars) {
                value = ConvertToNode(yson);
            } else {
                value = ConvertToAttributes(yson);
            }
        }
        return result;
    }

    TFuture<std::optional<TEffectiveAttributeCalculator>> MaybePrecomputeEffectiveAttributes() const
    {
        if (AttributeRequest_.GetEffectiveAttributeKeys().empty()) {
            return MakeFuture<std::optional<TEffectiveAttributeCalculator>>(std::nullopt);
        }

        auto ancestry = RootAncestry_.Slice(0, std::ssize(RootAncestry_) - 1);

        std::vector<TNodeId> nodeIds;
        for (const auto& descriptor : ancestry) {
            nodeIds.push_back(descriptor.Id);
        }

        auto requestTemplate = CreateRequestTemplate(ETreeScope::Ancestry);
        auto batcher = TMasterYPathProxy::TVectorizedGetBatcher(
            SequoiaSession_->GetNativeAuthenticatedClient(),
            requestTemplate,
            nodeIds,
            SequoiaSession_->GetCurrentCypressTransactionId());

        return batcher.Invoke()
            .Apply(BIND(&TSequoiaAttributeFetcher::ConvertToAttributesMap<false>, std::move(nodeIds)))
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TNodeIdToAttributes& result) -> std::optional<TEffectiveAttributeCalculator> {
                return TEffectiveAttributeCalculator::Initialize(
                    AttributeRequest_.GetEffectiveAttributeKeys(),
                    &result,
                    ancestry);
            }));
    }

    TFuture<std::optional<THashMap<TNodeId, TResourceUsage>>> MaybePrecomputeRecursiveAttributes() const
    {
        if (AttributeRequest_.GetRecursiveAttributeKeys().empty()) {
            return MakeFuture<std::optional<THashMap<TNodeId, TResourceUsage>>>(std::nullopt);
        }

        auto nodeIdsView = GetRequestedNodeIds();
        auto requestedNodes = THashSet<TNodeId>(std::ranges::begin(nodeIdsView), std::ranges::end(nodeIdsView));

        // TODO(danilalexeev): YT-26172. Do not switch context.
        auto subtree = SequoiaSession_->FetchSubtree(RootAncestry_.Back().Path);

        std::vector<TNodeId> targetNodeIds;
        targetNodeIds.reserve(std::max(std::ssize(subtree.Nodes) - std::ssize(requestedNodes), 0l));
        for (const auto& descriptor : subtree.Nodes) {
            if (!requestedNodes.contains(descriptor.Id)) {
                targetNodeIds.push_back(descriptor.Id);
            }
        }

        // Fast path.
        if (targetNodeIds.empty()) {
            return MakeFuture(std::make_optional(THashMap<TNodeId, TResourceUsage>{}));
        }

        auto requestTemplate = CreateRequestTemplate(ETreeScope::Descendants);
        auto batcher = TMasterYPathProxy::TVectorizedGetBatcher(
            SequoiaSession_->GetNativeAuthenticatedClient(),
            requestTemplate,
            targetNodeIds,
            SequoiaSession_->GetCurrentCypressTransactionId());

        // TODO(danilalexeev): YT-26461. Implement paging.
        return batcher.Invoke()
            .Apply(BIND(&TSequoiaAttributeFetcher::ConvertToAttributesMap<false>, std::vector<TNodeId>{}))
            .Apply(BIND([
                requestedNodes = std::move(requestedNodes),
                subtree = std::move(subtree)
            ] (const TNodeIdToAttributes& result) -> std::optional<THashMap<TNodeId, TResourceUsage>> {
                THashMap<TNodeId, TResourceUsage> frontierNodesResourceUsage;

                auto calculator = TFrontierRecursiveAttributeCalculator(
                    &result,
                    &requestedNodes,
                    &frontierNodesResourceUsage);

                constexpr auto isAncestorCallback = [] (const TCypressNodeDescriptor& maybeAncestor, const TCypressNodeDescriptor& child) {
                    return IsAncestorPath(maybeAncestor.Path, child.Path);
                };

                auto nodes = TRange(subtree.Nodes);

                if constexpr (std::is_same_v<T, const std::vector<TCypressChildDescriptor>*>) {
                    nodes = nodes.Slice(1, std::ssize(nodes));
                }

                TraverseSequoiaTree(
                    nodes,
                    &calculator,
                    isAncestorCallback);

                return frontierNodesResourceUsage;
            }));
    }

    TFuture<std::optional<TNodeIdToAttributes>> MaybeFetchRequestedNodesValuesScope() const
    {
        if (!ScalarNodeIds_) {
            return MakeFuture<std::optional<TNodeIdToAttributes>>(std::nullopt);
        }

        auto requestTemplate = CreateRequestTemplate(ETreeScope::RequestedValues);
        auto batcher = TMasterYPathProxy::TVectorizedGetBatcher(
            SequoiaSession_->GetNativeAuthenticatedClient(),
            requestTemplate,
            *ScalarNodeIds_,
            SequoiaSession_->GetCurrentCypressTransactionId());

        return batcher.Invoke()
            .Apply(BIND(TSequoiaAttributeFetcher::ConvertToAttributesMap<true>, std::vector<TNodeId>{}))
            .AsUnique()
            .Apply(BIND([] (TNodeIdToAttributes&& result) -> std::optional<TNodeIdToAttributes> {
                return std::move(result);
            }));
    }

    TFuture<std::optional<TNodeIdToAttributes>> MaybeFetchRequestedNodesAttributesScope() const
    {
        if (!AttributeRequest_.ShouldFetchAttributes()) {
            return MakeFuture<std::optional<TNodeIdToAttributes>>(std::nullopt);
        }

        std::vector<TNodeId> targetNodeIds;
        if constexpr (std::is_same_v<TRequestedNodes, const TNodeIdToChildDescriptors*>) {
            auto view = GetRequestedNodeIds()
                | std::views::filter([] (TNodeId nodeId) {
                    return !IsScalarType(TypeFromId(nodeId));
                });
            targetNodeIds = std::vector<TNodeId>(std::ranges::begin(view), std::ranges::end(view));
        } else {
            auto view = GetRequestedNodeIds();
            targetNodeIds = std::vector<TNodeId>(std::ranges::begin(view), std::ranges::end(view));
        }
        if constexpr (std::is_same_v<TRequestedNodes, const std::vector<TCypressChildDescriptor>*>) {
            targetNodeIds.push_back(RootId_);
        }

        auto requestTemplate = CreateRequestTemplate(ETreeScope::RequestedAttributes);
        auto batcher = TMasterYPathProxy::TVectorizedGetBatcher(
            SequoiaSession_->GetNativeAuthenticatedClient(),
            requestTemplate,
            targetNodeIds,
            SequoiaSession_->GetCurrentCypressTransactionId());

        return batcher.Invoke()
            .Apply(BIND(TSequoiaAttributeFetcher::ConvertToAttributesMap<false>, std::vector{RootId_}))
            .AsUnique()
            .Apply(BIND([] (TNodeIdToAttributes&& result) -> std::optional<TNodeIdToAttributes> {
                return std::move(result);
            }));
    }

    TFuture<TNodeIdToAttributes> FetchRequestedNodesBaseAttributes() const
    {
        auto valuesFuture = MaybeFetchRequestedNodesValuesScope().AsUnique();
        auto attributesFuture = MaybeFetchRequestedNodesAttributesScope().AsUnique();
        auto readyFutures = std::vector{valuesFuture.AsVoid(), attributesFuture.AsVoid()};

        return AllSucceeded(std::move(readyFutures))
            .Apply(BIND([
                valuesFuture = std::move(valuesFuture),
                attributesFuture = std::move(attributesFuture)
            ] {
                // NB: AllSucceeded() guarantees that all futures contain values.
                auto values = valuesFuture.BlockingGet().Value();
                auto attributes = attributesFuture.BlockingGet().Value();
                YT_VERIFY(values || attributes);

                if (attributes) {
                    if (values) {
                        for (auto& [key, value] : *values) {
                            EmplaceOrCrash(*attributes, key, std::move(value));
                        }
                    }
                    return std::move(*attributes);
                } else {
                    return std::move(*values);
                }
            }));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool HasSpecialAttributes(const TAttributeFilter& attributeFilter)
{
    auto request = TSequoiaAttributeRequest(attributeFilter);
    return
        !request.GetEffectiveAttributeKeys().empty() ||
        !request.GetRecursiveAttributeKeys().empty();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TNodeIdToAttributes> FetchAttributesForGetRequest(
    const TSequoiaSessionPtr& sequoiaSession,
    const TAttributeFilter& attributeFilter,
    NCypressClient::TNodeId rootId,
    const TNodeIdToChildDescriptors* nodeIdToChildren,
    TNodeAncestry rootAncestry,
    const std::vector<TNodeId>* scalarNodeIds)
{
    using T = std::decay_t<decltype(nodeIdToChildren)>;
    auto fetcher = New<TSequoiaAttributeFetcher<T>>(
        sequoiaSession,
        attributeFilter,
        rootId,
        nodeIdToChildren,
        rootAncestry,
        scalarNodeIds);
    return fetcher->FetchNodesWithAttributes();
}

TFuture<TNodeIdToAttributes> FetchAttributesForListRequest(
    const TSequoiaSessionPtr& sequoiaSession,
    const TAttributeFilter& attributeFilter,
    TNodeId rootId,
    const std::vector<TCypressChildDescriptor>* children,
    TNodeAncestry rootAncestry)
{
    using T = std::decay_t<decltype(children)>;
    auto fetcher = New<TSequoiaAttributeFetcher<T>>(
        sequoiaSession,
        attributeFilter,
        rootId,
        children,
        rootAncestry);
    return fetcher->FetchNodesWithAttributes();
}

TFuture<IAttributeDictionaryPtr> FetchAttributesForNode(
    const TSequoiaSessionPtr& sequoiaSession,
    const TAttributeFilter& attributeFilter,
    TNodeId rootId,
    TNodeAncestry rootAncestry)
{
    YT_VERIFY(attributeFilter);
    auto fetcher = New<TSequoiaAttributeFetcher<TNodeId>>(
        sequoiaSession,
        attributeFilter,
        rootId,
        rootId,
        rootAncestry);
    return fetcher->FetchNodesWithAttributes()
        .AsUnique()
        .Apply(BIND([=] (TNodeIdToAttributes&& nodeIdToAttributes) -> IAttributeDictionaryPtr {
            auto& attributes = GetOrCrash(nodeIdToAttributes, rootId);
            YT_VERIFY(std::holds_alternative<IAttributeDictionaryPtr>(attributes));
            return std::get<IAttributeDictionaryPtr>(std::move(attributes));
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
