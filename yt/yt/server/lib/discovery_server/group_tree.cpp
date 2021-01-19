#include "group.h"
#include "group_tree.h"

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/interned_attributes.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/core/concurrency/spinlock.h>

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

    void DropGroup()
    {
        YT_VERIFY(Group_);
        Group_.Reset();
    }

    void SetGroup(TGroupPtr group)
    {
        YT_VERIFY(!Group_);
        YT_VERIFY(group);
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

namespace {

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetNodeType(const TGroupNodePtr& node)
{
    return node->GetGroup() ? TStringBuf("group") : TStringBuf("node");
}

int GetMemberCount(const TGroupNodePtr& node)
{
    const auto& group = node->GetGroup();
    return group ? group->GetMemberCount() : 0;
}

int GetNodeCount(const TGroupNodePtr& node)
{
    return node->GetChildCount();
}

TString GetNodePath(const TGroupNodePtr& node)
{
    const auto& path = node->GetPath();
    return path.empty() ? "root node" : Format("node %v", path);
}

////////////////////////////////////////////////////////////////////////////////
// Functions which help to generate response for get/list in YPathService.

void BuildMemberAttributesFragment(
    const TMemberPtr& member,
    const std::optional<std::vector<TString>>& attributeKeys,
    TFluentMap fluent)
{
    auto reader = member->CreateReader();
    auto* attributes = reader.GetAttributes();

    auto contains = [&] (const TString& attribute) {
        return !attributeKeys || std::find(attributeKeys->begin(), attributeKeys->end(), attribute) != attributeKeys->end();
    };

    if (contains(PriorityAttribute)) {
        fluent.Item(PriorityAttribute).Value(member->GetPriority());
    }
    if (contains(RevisionAttribute)) {
        fluent.Item(RevisionAttribute).Value(reader.GetRevision());
    }
    if (contains(LastHeartbeatTimeAttribute)) {
        fluent.Item(LastHeartbeatTimeAttribute).Value(member->GetLastHeartbeatTime());
    }
    if (contains(LastAttributesUpdateTimeAttribute)) {
        fluent.Item(LastAttributesUpdateTimeAttribute).Value(member->GetLastAttributesUpdateTime());
    }
    // User attributes.
    for (auto [attributeKey, attributeValue] : attributes->ListPairs()) {
        if (contains(attributeKey)) {
            fluent.Item(std::move(attributeKey)).Value(std::move(attributeValue));
        }
    }
}

void BuildMemberAttributesAsMap(
    const TMemberPtr& member,
    const std::optional<std::vector<TString>>& /* attributeKeys */,
    TFluentAny fluent)
{
    auto reader = member->CreateReader();
    auto* attributes = reader.GetAttributes();

    fluent
        .BeginMap()
            .Item(PriorityAttribute).Value(member->GetPriority())
            .Item(RevisionAttribute).Value(reader.GetRevision())
            .Item(LastHeartbeatTimeAttribute).Value(member->GetLastHeartbeatTime())
            .Item(LastAttributesUpdateTimeAttribute).Value(member->GetLastAttributesUpdateTime())
            .Items(*attributes)
        .EndMap();
}

void BuildMember(
    const TMemberPtr& member,
    const std::optional<std::vector<TString>>& attributeKeys,
    TFluentAny fluent)
{
    if (attributeKeys && attributeKeys->empty()) {
        fluent.Entity();
    } else {
        fluent
            .BeginAttributes()
                .Do(BIND(BuildMemberAttributesFragment, std::cref(member), std::cref(attributeKeys)))
            .EndAttributes()
            .Entity();
    }
}

void BuildGroupMembers(
    const TGroupPtr& group,
    const std::optional<std::vector<TString>>& attributeKeys,
    TFluentAny fluent)
{
    fluent.DoMapFor(group->ListMembers(), [&] (TFluentMap fluent, const auto& member) {
        fluent.Item(member->GetId()).Do(BIND(BuildMember, std::cref(member), std::cref(attributeKeys)));
    });
}

void BuildGroupNodeAttributesFragment(
    const TGroupNodePtr& node,
    const std::optional<std::vector<TString>>& attributeKeys,
    TFluentMap fluent)
{
    auto contains = [&] (const TString& attribute) {
        return !attributeKeys || std::find(attributeKeys->begin(), attributeKeys->end(), attribute) != attributeKeys->end();
    };

    if (contains(EInternedAttributeKey::Type.Unintern())) {
        fluent = fluent.Item(EInternedAttributeKey::Type.Unintern()).Value(GetNodeType(node));
    }
    if (contains(EInternedAttributeKey::ChildCount.Unintern())) {
        fluent = fluent.Item(EInternedAttributeKey::ChildCount.Unintern()).Value(GetNodeCount(node));
    }

    const auto& group = node->GetGroup();
    if (group) {
        if (contains(EInternedAttributeKey::MemberCount.Unintern())) {
            fluent = fluent.Item(EInternedAttributeKey::MemberCount.Unintern()).Value(group->GetMemberCount());
        }
        if (contains(EInternedAttributeKey::Members.Unintern())) {
            fluent = fluent
                .Item(EInternedAttributeKey::Members.Unintern())
                .Do(BIND(BuildGroupMembers, std::cref(group), std::cref(attributeKeys)));
        }
    }
}

void BuildGroupNodeAttributesAsMap(
    const TGroupNodePtr& node,
    const std::optional<std::vector<TString>>& attributeKeys,
    TFluentAny fluent)
{
    const auto& group = node->GetGroup();
    fluent
        .BeginMap()
            .Item(EInternedAttributeKey::Type.Unintern()).Value(GetNodeType(node))
            .Item(EInternedAttributeKey::ChildCount.Unintern()).Value(GetNodeCount(node))
            .DoIf(static_cast<bool>(group), [&] (TFluentMap fluent) {
                fluent
                    .Item(EInternedAttributeKey::MemberCount.Unintern()).Value((group->GetMemberCount()))
                    .Item(EInternedAttributeKey::Members.Unintern())
                        .Do(BIND(BuildGroupMembers, std::cref(group), std::cref(attributeKeys)));
            })
        .EndMap();
}

void BuildGroupNode(
    const TGroupNodePtr& node,
    const std::optional<std::vector<TString>>& attributeKeys,
    TFluentAny fluent)
{
    if (attributeKeys && attributeKeys->empty()) {
        fluent.DoMapFor(node->GetChildren(), [&] (TFluentMap fluent, const auto& child) {
            const auto& [key, childNode] = child;
            fluent.Item(key).Do(BIND(BuildGroupNode, std::cref(childNode), std::cref(attributeKeys)));
        });
    } else {
        fluent
            .BeginAttributes()
                .Do(BIND(BuildGroupNodeAttributesFragment, std::cref(node), std::cref(attributeKeys)))
            .EndAttributes()
            .DoMapFor(node->GetChildren(), [&] (TFluentMap fluent, const auto& child) {
                const auto& [key, childNode] = child;
                fluent.Item(key).Do(BIND(BuildGroupNode, std::cref(childNode), std::cref(attributeKeys)));
            });
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonString DoListGroupNode(
    const TGroupNodePtr& node,
    const std::optional<std::vector<TString>>& attributeKeys)
{
    return BuildYsonStringFluently()
        .DoListFor(node->GetChildren(), [&] (TFluentList fluent, const auto& child) {
            const auto& [key, childNode] = child;
            if (attributeKeys && attributeKeys->empty()) {
                fluent.Item().Value(key);
            } else {
                fluent
                    .Item()
                    .BeginAttributes()
                        .Do(BIND(BuildGroupNodeAttributesFragment, std::cref(childNode), std::cref(attributeKeys)))
                    .EndAttributes()
                    .Value(key);
            }
        });
}

TYsonString DoListGroupNodeAttributes(
    const TGroupNodePtr& node,
    const std::optional<std::vector<TString>>& /* attributeKyes */)
{
    return BuildYsonStringFluently()
        .BeginList()
            .Item().Value(EInternedAttributeKey::Type.Unintern())
            .Item().Value(EInternedAttributeKey::ChildCount.Unintern())
            .DoIf(static_cast<bool>(node->GetGroup()), [] (TFluentList fluent) {
                fluent
                    .Item().Value(EInternedAttributeKey::Members.Unintern())
                    .Item().Value(EInternedAttributeKey::MemberCount.Unintern());
            })
        .EndList();
}

TYsonString DoListMembers(
    const TGroupPtr& group,
    const std::optional<std::vector<TString>>& attributeKeys)
{
    return BuildYsonStringFluently()
        .DoListFor(group->ListMembers(), [&] (TFluentList fluent, const auto& member) {
            if (attributeKeys && attributeKeys->empty()) {
                fluent.Item().Value(member->GetId());
            } else {
                fluent
                    .Item()
                    .BeginAttributes()
                        .Do(BIND(BuildMemberAttributesFragment, std::cref(member), std::cref(attributeKeys)))
                    .EndAttributes()
                    .Value(member->GetId());
            }
        });
}

TYsonString DoListMemberAttributes(
    const TMemberPtr& member,
    const std::optional<std::vector<TString>>& /* attributeKeys */)
{
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

////////////////////////////////////////////////////////////////////////////////

TYsonString DoGetGroupNode(const TGroupNodePtr& node, const std::optional<std::vector<TString>>& attributeKeys)
{
    return BuildYsonStringFluently().Do(BIND(BuildGroupNode, std::cref(node), std::cref(attributeKeys)));
}

TYsonString DoGetGroupNodeAttributes(const TGroupNodePtr& node, const std::optional<std::vector<TString>>& attributeKeys)
{
    return BuildYsonStringFluently().Do(BIND(BuildGroupNodeAttributesAsMap, std::cref(node), std::cref(attributeKeys)));
}

TYsonString DoGetGroupMembers(const TGroupPtr& group, const std::optional<std::vector<TString>>& attributeKeys)
{
    return BuildYsonStringFluently()
        .Do(BIND(BuildGroupMembers, std::cref(group), std::cref(attributeKeys)));
}

TYsonString DoGetMember(const TMemberPtr& member, const std::optional<std::vector<TString>>& attributeKeys)
{
    return BuildYsonStringFluently()
        .Do(BIND(BuildMember, std::cref(member), std::cref(attributeKeys)));
}

TYsonString DoGetMemberAttributes(const TMemberPtr& member, const std::optional<std::vector<TString>>& attributeKeys)
{
    return BuildYsonStringFluently().Do(BIND(BuildMemberAttributesAsMap, std::cref(member), std::cref(attributeKeys)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

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
        auto guard = WriterGuard(Lock_);

        YT_VERIFY(IdToNode_.emplace("", Root_).second);
    }

    TYsonString List(const TYPath& path, const std::optional<std::vector<TString>>& attributeKeys)
    {
        auto guard = ReaderGuard(Lock_);

        auto [node, unresolvedPath] = ResolvePath(path);

        // list /group_id
        if (unresolvedPath.empty()) {
            return DoListGroupNode(node, attributeKeys);
        }

        NYPath::TTokenizer tokenizer(unresolvedPath);
        tokenizer.Advance();

        tokenizer.Expect(NYPath::ETokenType::At);
        tokenizer.Advance();

        // list /group_id/@
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return DoListGroupNodeAttributes(node, attributeKeys);
        }

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto nodeAttributeKey = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        auto internedKey = TInternedAttributeKey::Lookup(nodeAttributeKey);

        // Only "members" attribute is listable.
        if (internedKey != EInternedAttributeKey::Members) {
            if (internedKey == EInternedAttributeKey::ChildCount ||
                internedKey == EInternedAttributeKey::Type ||
                internedKey == EInternedAttributeKey::MemberCount)
            {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::NoSuchMethod,
                    "Method List is not supported");
            } else {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::ResolveError,
                    "Attribute %Qv does not exist",
                    nodeAttributeKey);
            }
        }

        const auto& group = node->GetGroup();
        if (!group) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "There is no group at %v",
                GetNodePath(node));
        }

        // list /group_id/@members
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return DoListMembers(group, attributeKeys);
        }

        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto memberId = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        const auto& member = group->FindMember(memberId);

        if (!member) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "There is no member %Qv in group %v",
                memberId,
                group->GetId());
        }

        // Member is not listable itself, can only list attributes.
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();

        tokenizer.Expect(NYPath::ETokenType::At);
        tokenizer.Advance();

        // list /group_id/@members/member_id/@
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return DoListMemberAttributes(member, attributeKeys);
        }

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto memberAttributeKey = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        // list /group_id/@members/member_id/@systemAttribute
        if (memberAttributeKey == PriorityAttribute ||
            memberAttributeKey == RevisionAttribute ||
            memberAttributeKey == LastHeartbeatTimeAttribute ||
            memberAttributeKey == LastAttributesUpdateTimeAttribute)
        {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::NoSuchMethod,
                "Attribute %Qv is not listable",
                memberAttributeKey);
        }

        auto reader = member->CreateReader();
        auto* memberAttributes = reader.GetAttributes();

        auto userAttributeYson = memberAttributes->GetYson(memberAttributeKey);

        // list /group_id/@members/member_id/@user_attribute/path
        return ConvertToYsonString(SyncYPathList(ConvertToNode(userAttributeYson), TYPath(tokenizer.GetInput())));
    }

    TYsonString Get(const TYPath& path, const std::optional<std::vector<TString>>& attributeKeys)
    {
        auto guard = ReaderGuard(Lock_);

        auto [node, unresolvedPath] = ResolvePath(path);

        // get /group_id
        if (unresolvedPath.empty()) {
            return DoGetGroupNode(node, attributeKeys);
        }

        NYPath::TTokenizer tokenizer(unresolvedPath);
        tokenizer.Advance();

        tokenizer.Expect(NYPath::ETokenType::At);
        tokenizer.Advance();

        // get /group_id/@
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return DoGetGroupNodeAttributes(node, attributeKeys);
        }

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto nodeAttributeKey = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        const auto& group = node->GetGroup();
        auto internedKey = TInternedAttributeKey::Lookup(nodeAttributeKey);

        // get /group_id/@attribute
        switch (internedKey) {
            case EInternedAttributeKey::ChildCount: {
                tokenizer.Expect(NYPath::ETokenType::EndOfStream);
                return ConvertToYsonString(GetNodeCount(node));
            }
            case EInternedAttributeKey::Type: {
                tokenizer.Expect(NYPath::ETokenType::EndOfStream);
                return ConvertToYsonString(GetNodeType(node));
            }
            case EInternedAttributeKey::MemberCount: {
                if (!group) {
                    THROW_ERROR_EXCEPTION(
                        NYTree::EErrorCode::ResolveError,
                        "Attribute %Qv does not exist for non-group nodes",
                        internedKey.Unintern());
                }
                tokenizer.Expect(NYPath::ETokenType::EndOfStream);
                return ConvertToYsonString(group->GetMemberCount());
            }
            case EInternedAttributeKey::Members: {
                if (!group) {
                    THROW_ERROR_EXCEPTION(
                        NYTree::EErrorCode::ResolveError,
                        "Attribute %Qv does not exist for non-group nodes",
                        internedKey.Unintern());
                }
                // We will handle EInternedAttributeKey::Members after switch.
                break;
            }
            default: {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::ResolveError,
                    "Attribute %Qv does not exist",
                    nodeAttributeKey);
            }
        }

        // get /group_id/@members
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return DoGetGroupMembers(group, attributeKeys);
        }

        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto memberId = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        const auto& member = group->FindMember(memberId);

        if (!member) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "There is no member %Qv in group %v",
                memberId,
                group->GetId());
        }

        // get /group_id/@members/member_id
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return DoGetMember(member, attributeKeys);
        }

        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();

        tokenizer.Expect(NYPath::ETokenType::At);
        tokenizer.Advance();

        // get /group_id/@members/member_id/@
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return DoGetMemberAttributes(member, attributeKeys);
        }

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto memberAttributeKey = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        auto reader = member->CreateReader();
        auto* memberAttributes = reader.GetAttributes();

        // get /group_id/@members/member_id/@systemAttribute
        if (memberAttributeKey == PriorityAttribute) {
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);
            return ConvertToYsonString(member->GetPriority());
        }
        if (memberAttributeKey == RevisionAttribute) {
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);
            return ConvertToYsonString(reader.GetRevision());
        }
        if (memberAttributeKey == LastHeartbeatTimeAttribute) {
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);
            return ConvertToYsonString(member->GetLastHeartbeatTime());
        }
        if (memberAttributeKey == LastAttributesUpdateTimeAttribute) {
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);
            return ConvertToYsonString(member->GetLastAttributesUpdateTime());
        }

        auto userAttributeYson = memberAttributes->GetYson(memberAttributeKey);

        // get /group_id/@members/member_id/@user_attribute
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return userAttributeYson;
        }

        // get /group_id/@members/member_id/@user_attribute/path
        return SyncYPathGet(ConvertToNode(userAttributeYson), TYPath(tokenizer.GetInput()));
    }

    bool Exists(const TYPath& path)
    {
        auto guard = ReaderGuard(Lock_);

        auto [node, unresolvedSuffix] = ResolvePath(path);
        
        // exists /group_id
        if (unresolvedSuffix.empty()) {
            return true;
        }

        const auto& group = node->GetGroup();

        NYPath::TTokenizer tokenizer(unresolvedSuffix);
        tokenizer.Advance();

        // ResolvePath should find terminal node if it exists.
        if (tokenizer.GetType() == NYPath::ETokenType::Literal) {
            return false;
        }

        tokenizer.Expect(NYPath::ETokenType::At);
        tokenizer.Advance();

        // exists /group_id/@
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return true;
        }

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto nodeAttributeKey = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        auto internedKey = TInternedAttributeKey::Lookup(nodeAttributeKey);

        // exists /group_id/@child_count or exists /group_id/@type
        if (internedKey == EInternedAttributeKey::ChildCount || internedKey == EInternedAttributeKey::Type) {
            return tokenizer.GetType() == NYPath::ETokenType::EndOfStream;
        }
            
        // member_count and members exist only if there is a group in node.
        if (!group) {
            return false;
        }

        // exists /group_id/@member_count
        if (internedKey == EInternedAttributeKey::MemberCount) {
            return tokenizer.GetType() == NYPath::ETokenType::EndOfStream;
        }

        if (internedKey != EInternedAttributeKey::Members) {
            return false;
        }

        // exists /group_id/@members
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return true;
        }

        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto memberId = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        const auto& member = group->FindMember(memberId);

        if (!member) {
            return false;
        }

        // exists /group_id/@members/member_id
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return true;
        }

        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();

        tokenizer.Expect(NYPath::ETokenType::At);
        tokenizer.Advance();

        // exists /group_id/@members/member_id/@
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return true;
        }

        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto memberAttributeKey = tokenizer.GetLiteralValue();
        tokenizer.Advance();

        // exists /group_id/@members/member_id/@systemAttribute
        if (memberAttributeKey == PriorityAttribute ||
            memberAttributeKey == RevisionAttribute ||
            memberAttributeKey == LastHeartbeatTimeAttribute ||
            memberAttributeKey == LastAttributesUpdateTimeAttribute)
        {
            return tokenizer.GetType() == NYPath::ETokenType::EndOfStream;
        }

        auto reader = member->CreateReader();
        auto* memberAttributes = reader.GetAttributes();

        auto userAttributeYson = memberAttributes->FindYson(memberAttributeKey);
        if (!userAttributeYson) {
            return false;
        }

        // exists /group_id/@members/member_id/@user_attribute
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return true;
        }
    
        // exists /group_id/@members/member_id/@user_attribute/path
        return SyncYPathExists(ConvertToNode(userAttributeYson), TYPath(tokenizer.GetInput()));
    }

    TGroupPtr FindGroup(const TYPath& path)
    {
        auto guard = ReaderGuard(Lock_);
        return DoFindGroup(path);
    }

    THashMap<TGroupId, TGroupPtr> GetOrCreateGroups(const std::vector<TGroupId>& groupIds)
    {
        THashMap<TGroupId, TGroupPtr> result;
        std::vector<TGroupId> nonexistingGroupIds;

        // Fast path.
        {
            auto guard = ReaderGuard(Lock_);
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

        auto createGroup = [&] (const TGroupId& id) {
                NYPath::TTokenizer tokenizer(id);
                TString key;
                auto currentNode = Root_;
                for (auto token = tokenizer.Advance(); token != NYPath::ETokenType::EndOfStream; token = tokenizer.Advance()) {
                    if (tokenizer.GetType() != NYPath::ETokenType::Slash) {
                        YT_LOG_WARNING("Invalid group id (GroupId: %v)", id);
                        RemovePath(currentNode->GetPath(), currentNode);
                        return;
                    }
                    if (tokenizer.Advance() != NYPath::ETokenType::Literal) {
                        YT_LOG_WARNING("Invalid group id (GroupId: %v)", id);
                        RemovePath(currentNode->GetPath(), currentNode);
                        return;
                    }

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

                auto group = New<TGroup>(
                    id,
                    BIND(&TImpl::OnGroupEmptied, MakeWeak(this), id, MakeWeak(currentNode)),
                    Logger);
                currentNode->SetGroup(group);
                YT_VERIFY(result.emplace(id, group).second);
        };

        // Slow path.
        {
            auto guard = WriterGuard(Lock_);
            for (const auto& id : nonexistingGroupIds) {
                if (result.contains(id)) {
                    continue;
                }

                createGroup(id);
            }
        }
        return result;
    }

private:
    const TGroupNodePtr Root_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, Lock_);
    THashMap<TGroupId, TGroupNodePtr> IdToNode_;

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

    TGroupPtr DoFindGroup(const TYPath& path)
    {
        auto it = IdToNode_.find(path);
        return it == IdToNode_.end() ? nullptr : it->second->GetGroup();
    }

    void OnGroupEmptied(const TGroupId& groupId, const TWeakPtr<TGroupNode>& weakNode)
    {
        auto node = weakNode.Lock();
        if (!node) {
            return;
        }

        auto guard = WriterGuard(Lock_);

        if (!DoFindGroup(groupId)) {
            YT_LOG_WARNING("Empty group is already deleted (GroupId: %v)", groupId);
            return;
        }

        if (GetMemberCount(node) != 0) {
            YT_LOG_WARNING("Trying to delete not empty group (GroupId: %v)", groupId);
            return;
        }
        // Group should be deleted even if node has children.
        node->DropGroup();

        RemovePath(groupId, node);
    }

    void RemovePath(TGroupId path, TGroupNodePtr node)
    {
        VERIFY_WRITER_SPINLOCK_AFFINITY(Lock_);

        while (node != Root_ && node->GetChildCount() == 0 && GetMemberCount(node) == 0) {
            YT_VERIFY(IdToNode_.erase(path) > 0);

            auto key = node->GetKey();
            node = node->GetParent().Lock();
            if (!node) {
                YT_LOG_WARNING("Parent node was already deleted (Path: %v)", path);
                break;
            }
            node->RemoveChild(key);
            path = node->GetPath();
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

NYson::TYsonString TGroupTree::List(const TYPath& path, const std::optional<std::vector<TString>>& attributeKeys)
{
    return Impl_->List(path, attributeKeys);
}

NYson::TYsonString TGroupTree::Get(const TYPath& path, const std::optional<std::vector<TString>>& attributeKeys)
{
    return Impl_->Get(path, attributeKeys);
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
