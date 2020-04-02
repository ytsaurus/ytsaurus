#include "group.h"
#include "group_tree.h"

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/interned_attributes.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/ytlib/discovery_client/helpers.h>
#include <yt/ytlib/discovery_client/public.h>

namespace NYT::NDiscoveryServer {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NDiscoveryClient;

//////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TGroupNode)

class TGroupNode
    : public TRefCounted
{
public:
    TGroupNode(
        TString key,
        TYPath path,
        TWeakPtr<TGroupNode> parent)
        : Key_(std::move(key))
        , Path_(std::move(path))
        , Parent_(std::move(parent))
    { }

    TGroupNodePtr FindChild(const TString& key)
    {
        auto it = KeyToChild_.find(key);
        return it == KeyToChild_.end() ? nullptr : it->second;
    }

    const THashMap<TString, TGroupNodePtr>& GetChildren()
    {
        return KeyToChild_;
    }

    void AddChild(const TString& key, const TGroupNodePtr& child)
    {
        YT_VERIFY(KeyToChild_.emplace(key, child).second);
    }

    void RemoveChild(const TString& key)
    {
        YT_VERIFY(KeyToChild_.erase(key) > 0);
    }

    int GetChildCount()
    {
        return static_cast<int>(KeyToChild_.size());
    }

    const TGroupPtr& GetGroup()
    {
        return Group_;
    }

    void SetGroup(TGroupPtr group)
    {
        YT_VERIFY(!Group_);
        Group_ = std::move(group);
    }

    const TString& GetKey()
    {
        return Key_;
    }

    const TString& GetPath()
    {
        return Path_;
    }

    const TWeakPtr<TGroupNode>& GetParent()
    {
        return Parent_;
    }

private:
    const TString Key_;
    const TYPath Path_;
    const TWeakPtr<TGroupNode> Parent_;

    TGroupPtr Group_;
    THashMap<TString, TGroupNodePtr> KeyToChild_;
};

DEFINE_REFCOUNTED_TYPE(TGroupNode)

////////////////////////////////////////////////////////////////////////////////

class TGroupTree::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(NLogging::TLogger logger)
        : Root_(New<TGroupNode>(
            /* key */ TString(),
            /* path */ TYPath(),
            /* parent */ nullptr))
        , Logger(std::move(logger))
    {
        NConcurrency::TWriterGuard guard(Lock_);

        YT_VERIFY(IdToNode_.emplace("", Root_).second);
    }

    TYsonString List(const TYPath& path)
    {
        NConcurrency::TReaderGuard guard(Lock_);

        auto [node, unresolvedPath] = ResolvePath(path);
        const auto& group = node->GetGroup();

        if (unresolvedPath.empty()) {
            if (group) {
                return BuildYsonStringFluently()
                    .DoListFor(group->ListMembers(), [] (TFluentList fluent, const auto& member) {
                        fluent.Item().Value(member->GetId());
                    });
            } else {
                return BuildYsonStringFluently()
                    .DoListFor(node->GetChildren(), [] (TFluentList fluent, const auto& child) {
                        const auto& key = child.first;
                        fluent.Item().Value(key);
                    });
            }
        }

        NYPath::TTokenizer tokenizer(unresolvedPath);

        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        const auto& key = tokenizer.GetLiteralValue();

        if (!group) {
            THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
                "There is no group %Qv at %v",
                key,
                GetNodePath(node));
        }

        auto member = group->GetMemberOrThrow(key);

        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::EndOfStream);

        auto reader = member->CreateReader();
        auto* attributes = reader.GetAttributes();
        return BuildYsonStringFluently()
            .BeginList()
                .Item().Value(PriorityAttribute)
                .Item().Value(RevisionAttribute)
                .Item().Value(LastHeartbeatTimeAttribute)
                .Item().Value(LastAttributesUpdateTimeAttribute)
                .DoFor(attributes->ListKeys(), [] (TFluentList fluent, const auto& value) {
                    fluent.Item().Value(value);
                })
            .EndList();
    }

    TYsonString Get(const TYPath& path)
    {
        NConcurrency::TReaderGuard guard(Lock_);

        auto [node, unresolvedPath] = ResolvePath(path);
        const auto& group = node->GetGroup();

        if (unresolvedPath.empty()) {
            if (group) {
                return BuildYsonStringFluently()
                    .DoListFor(group->ListMembers(), [&] (TFluentList fluent, const auto& member) {
                        WriteMemberAttributes(member, fluent);
                    });
            }

            return BuildYsonStringFluently()
                .DoListFor(node->GetChildren(), [&] (TFluentList fluent, const auto& item) {
                    const auto& child = item.second;
                    fluent
                        .Item().BeginMap()
                            .Item(EInternedAttributeKey::Id.Unintern()).Value(child->GetKey())
                            .Item(EInternedAttributeKey::Type.Unintern()).Value(GetNodeType(child))
                            .Item(EInternedAttributeKey::ChildCount.Unintern()).Value(GetNodeCount(child))
                        .EndMap();
                });
        }

        NYPath::TTokenizer tokenizer(unresolvedPath);

        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::Literal: {
                const auto& key = tokenizer.GetLiteralValue();
                if (!group) {
                    THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
                        "There is no group %Qv at %v",
                        key,
                        GetNodePath(node));
                }

                auto member = group->GetMemberOrThrow(key);

                if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                    return BuildYsonStringFluently()
                        .DoList(BIND(&TImpl::WriteMemberAttributes, Unretained(this), member));
                }

                tokenizer.Expect(NYPath::ETokenType::Slash);
                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::At);
                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::Literal);

                auto attributeKey = tokenizer.GetLiteralValue();
                auto internedKey = TInternedAttributeKey::Lookup(attributeKey);

                tokenizer.Advance();

                switch (internedKey) {
                    case EInternedAttributeKey::Priority:
                        tokenizer.Expect(NYPath::ETokenType::EndOfStream);
                        return BuildYsonStringFluently()
                            .Value(member->GetPriority());
                    case EInternedAttributeKey::LastHeartbeatTime:
                        tokenizer.Expect(NYPath::ETokenType::EndOfStream);
                        return BuildYsonStringFluently()
                            .Value(member->GetLastHeartbeatTime());
                    case EInternedAttributeKey::LastAttributesUpdateTime:
                        tokenizer.Expect(NYPath::ETokenType::EndOfStream);
                        return BuildYsonStringFluently()
                            .Value(member->GetLastAttributesUpdateTime());
                }

                auto reader = member->CreateReader();
                auto* attributes = reader.GetAttributes();

                if (internedKey == EInternedAttributeKey::Revision) {
                    tokenizer.Expect(NYPath::ETokenType::EndOfStream);
                    return BuildYsonStringFluently()
                        .Value(reader.GetRevision());
                }

                const auto& value = attributes->GetYson(attributeKey);
                if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                    return value;
                }

                auto node = ConvertToNode(value);
                return SyncYPathGet(node, ToString(tokenizer.GetInput()));
            }

            case NYPath::ETokenType::At: {
                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::Literal);

                auto attributeKey = tokenizer.GetLiteralValue();

                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::EndOfStream);

                auto internedKey = TInternedAttributeKey::Lookup(attributeKey);
                switch (internedKey) {
                    case EInternedAttributeKey::Id:
                        return BuildYsonStringFluently()
                            .Value(node->GetKey());
                    case EInternedAttributeKey::ChildCount:
                        return BuildYsonStringFluently()
                            .Value(GetNodeCount(node));
                    case EInternedAttributeKey::Type:
                        return BuildYsonStringFluently()
                            .Value(GetNodeType(node));
                    default:
                        tokenizer.ThrowUnexpected();
                }
            }

            default:
                tokenizer.ThrowUnexpected();
        }
    }

    bool Exists(const TYPath& path)
    {
        NConcurrency::TReaderGuard guard(Lock_);

        auto [node, unresolvedSuffix] = ResolvePath(path);
        if (unresolvedSuffix.empty()) {
            return true;
        }

        const auto& group = node->GetGroup();

        NYPath::TTokenizer tokenizer(unresolvedSuffix);
        switch (tokenizer.Advance()) {
            case NYPath::ETokenType::Literal: {
                if (!group) {
                    return false;
                }

                const auto& key = tokenizer.GetLiteralValue();
                auto member = group->FindMember(key);
                if (!member) {
                    return false;
                }

                if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                    return true;
                }

                tokenizer.Expect(NYPath::ETokenType::Slash);
                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::At);
                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::Literal);

                auto attributeKey = tokenizer.GetLiteralValue();
                if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream && IsMemberSystemAttribute(attributeKey)) {
                    return true;
                }

                auto reader = member->CreateReader();
                auto* attributes = reader.GetAttributes();

                const auto& value = attributes->FindYson(attributeKey);
                if (!value) {
                    return false;
                }

                if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
                    return true;
                }

                auto node = ConvertToNode(value);
                return SyncYPathExists(node, ToString(tokenizer.GetInput()));
            }

            case NYPath::ETokenType::At: {
                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::Literal);

                auto attributeKey = tokenizer.GetLiteralValue();
                auto internedKey = TInternedAttributeKey::Lookup(attributeKey);

                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::EndOfStream);

                return internedKey == EInternedAttributeKey::Id ||
                    internedKey == EInternedAttributeKey::ChildCount ||
                    internedKey == EInternedAttributeKey::Type;
            }

            default:
                tokenizer.ThrowUnexpected();
        }
    }

    TGroupPtr FindGroup(const TYPath& path)
    {
        NConcurrency::TReaderGuard guard(Lock_);
        return DoFindGroup(path);
    }

    THashMap<TGroupId, TGroupPtr> GetOrCreateGroups(const std::vector<TGroupId>& groupIds)
    {
        THashMap<TGroupId, TGroupPtr> result;
        std::vector<TGroupId> nonexistingGroupIds;

        // Fast path.
        {
            NConcurrency::TReaderGuard guard(Lock_);
            for (const auto& id : groupIds) {
                if (result.contains(id)) {
                    continue;
                }
                auto group = DoFindGroup(id);
                if (group) {
                    YT_VERIFY(result.emplace(id, group).second);
                } else {
                    nonexistingGroupIds.push_back(id);
                }
            }
        }

        if (nonexistingGroupIds.empty()) {
            return result;
        }

        // Slow path.
        {
            NConcurrency::TWriterGuard guard(Lock_);
            for (const auto& id : nonexistingGroupIds) {
                if (result.contains(id)) {
                    continue;
                }
                NYPath::TTokenizer tokenizer(id);
                TString key;
                auto currentNode = Root_;
                for (auto token = tokenizer.Advance(); token != NYPath::ETokenType::EndOfStream; token = tokenizer.Advance()) {
                    if (currentNode->GetGroup()) {
                        THROW_ERROR_EXCEPTION("Cannot create group %Qv since the prefix %v of its path matches the path of another group",
                            id,
                            currentNode->GetPath());
                    }

                    tokenizer.Expect(NYPath::ETokenType::Slash);
                    token = tokenizer.Advance();
                    tokenizer.Expect(NYPath::ETokenType::Literal);

                    key = tokenizer.GetLiteralValue();

                    auto nextNode = currentNode->FindChild(key);
                    if (nextNode) {
                        currentNode = nextNode;
                        continue;
                    }

                    auto currentPath = tokenizer.GetPrefixPlusToken();
                    auto newNode = New<TGroupNode>(key, ToString(currentPath), MakeWeak(currentNode));
                    YT_VERIFY(IdToNode_.emplace(currentPath, newNode).second);
                    currentNode->AddChild(key, newNode);
                    currentNode = newNode;
                }

                if (currentNode->GetChildCount() > 0) {
                    THROW_ERROR_EXCEPTION("Cannot create group %v since its path matches the prefix of another group's path", id);
                }

                auto group = New<TGroup>(
                    id,
                    BIND(&TImpl::OnGroupEmptied, MakeWeak(this), id, currentNode),
                    Logger);
                currentNode->SetGroup(group);
                YT_VERIFY(result.emplace(id, group).second);
            }
        }
        return result;
    }

private:
    const TGroupNodePtr Root_;
    const NLogging::TLogger Logger;

    NConcurrency::TReaderWriterSpinLock Lock_;
    THashMap<TGroupId, TGroupNodePtr> IdToNode_;

    TString GetNodePath(const TGroupNodePtr& node)
    {
        const auto& path = node->GetPath();
        return path.empty() ? "root node" : Format("node %v", path);
    }

    std::pair<TGroupNodePtr, TYPath> ResolvePath(const TYPath& path)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        NYPath::TTokenizer tokenizer(path);
        auto currentNode = Root_;

        for (auto token = tokenizer.Advance(); token != NYPath::ETokenType::EndOfStream; token = tokenizer.Advance()) {
            auto suffix = tokenizer.GetSuffix();

            tokenizer.Expect(NYPath::ETokenType::Slash);

            if (tokenizer.Advance() != NYPath::ETokenType::Literal) {
                return {currentNode, ToString(suffix)};
            }

            const auto& key = tokenizer.GetLiteralValue();
            auto newNode = currentNode->FindChild(key);
            if (!newNode) {
                return {currentNode, ToString(suffix)};
            }

            currentNode = newNode;
        }
        return {currentNode, ""};
    }

    void WriteMemberAttributes(const TMemberPtr& member, TFluentList fluent)
    {
        auto reader = member->CreateReader();
        auto* attributes = reader.GetAttributes();
        fluent
            .Item().BeginMap()
                .Item(PriorityAttribute).Value(member->GetPriority())
                .Item(RevisionAttribute).Value(reader.GetRevision())
                .Item(LastHeartbeatTimeAttribute).Value(member->GetLastHeartbeatTime())
                .Item(LastAttributesUpdateTimeAttribute).Value(member->GetLastAttributesUpdateTime())
                .Items(*attributes)
            .EndMap();
    }

    TStringBuf GetNodeType(const TGroupNodePtr& node)
    {
        return node->GetGroup() ? AsStringBuf("group") : AsStringBuf("node");
    }

    int GetNodeCount(const TGroupNodePtr& node)
    {
        const auto& group = node->GetGroup();
        return group ? group->GetMemberCount() : node->GetChildCount();
    }

    TGroupPtr DoFindGroup(const TYPath& path)
    {
        auto it = IdToNode_.find(path);
        return it == IdToNode_.end() ? nullptr : it->second->GetGroup();
    }

    void OnGroupEmptied(const TGroupId& groupId, TGroupNodePtr node)
    {
        NConcurrency::TWriterGuard guard(Lock_);

        if (!DoFindGroup(groupId)) {
            YT_LOG_WARNING("Empty group is already deleted (GroupId: %v)", groupId);
            return;
        }

        auto currentPath = groupId;
        while (node != Root_ && node->GetChildCount() > 0) {
            YT_VERIFY(IdToNode_.erase(currentPath) > 0);

            const auto& key = node->GetKey();
            node = node->GetParent().Lock();
            if (!node) {
                YT_LOG_WARNING("Parent node was already deleted (Path: %v)", currentPath);
                break;
            }
            node->RemoveChild(key);
            currentPath = node->GetPath();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TGroupTree::TGroupTree(
    NLogging::TLogger logger)
    : Impl_(New<TImpl>(
        std::move(logger)))
{ }

TGroupTree::~TGroupTree() = default;

NYson::TYsonString TGroupTree::List(const TYPath& path)
{
    return Impl_->List(path);
}

NYson::TYsonString TGroupTree::Get(const TYPath& path)
{
    return Impl_->Get(path);
}

bool TGroupTree::Exists(const TYPath& path)
{
    return Impl_->Exists(path);
}

TGroupPtr TGroupTree::FindGroup(const TYPath& path)
{
    return Impl_->FindGroup(path);
}

THashMap<TGroupId, TGroupPtr> TGroupTree::GetOrCreateGroups(const std::vector<TGroupId>& groupIds)
{
    return Impl_->GetOrCreateGroups(groupIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
