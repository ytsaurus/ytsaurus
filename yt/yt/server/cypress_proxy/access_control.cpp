#include "access_control.h"

#include "acd_fetcher.h"
#include "private.h"
#include "sequoia_session.h"
#include "sequoia_tree_visitor.h"
#include "user_directory.h"

#include <yt/yt/server/lib/security_server/permission_checker.h>

namespace NYT::NCypressProxy {

using namespace NCypressClient;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void LogAndThrowAuthorizationError(
    const TPermissionCheckResult& result,
    EPermission permission,
    const TUserDescriptorPtr& user,
    const TUserDirectoryPtr& userDirectory,
    TNodeAncestry nodeAncestry,
    const TSequoiaSession::TSubtree* subtreeNodes = {})
{
    YT_VERIFY(result.Action == ESecurityAction::Deny);

    auto target = TPermissionCheckTarget{
        .ObjectId = nodeAncestry.Back().Id,
    };

    std::string resultObjectName;
    std::string resultSubjectName;
    if (result.ObjectId && result.SubjectId) {
        auto nodeRange = std::ranges::join_view(
            std::array{
                TRange(nodeAncestry),
                subtreeNodes
                    ? TRange(subtreeNodes->Nodes)
                    : TRange<TCypressNodeDescriptor>()
            });

        auto it = std::ranges::find_if(
            nodeRange,
            [=] (const TCypressNodeDescriptor& descriptor) {
                return descriptor.Id == result.ObjectId;
            });

        if (it == std::ranges::end(nodeRange)) {
            YT_LOG_ALERT(
                "Missing path for node with matching ACE "
                "(PathMissingObjectId: %v, TargetNodeId: %v)",
                result.ObjectId,
                nodeAncestry.Back().Id);

            THROW_ERROR_EXCEPTION("Path not found for node %v with matching ACE",
                result.ObjectId);
        }

        resultObjectName = FormatCypressNodeName(it->Path.ToRealPath().Underlying());

        auto subject = userDirectory->GetSubjectByIdOrThrow(result.SubjectId);
        resultSubjectName = subject->Name;
    }

    NSecurityServer::LogAndThrowAuthorizationError(
        Logger(),
        target,
        result,
        permission,
        user->Name,
        nodeAncestry.Back().Path.GetBaseName(),
        nodeAncestry.Back().Path.ToRealPath().Underlying(),
        resultObjectName,
        resultSubjectName);
}

ESecurityAction FastCheckPermission(const TUserDescriptorPtr& user)
{
    if (user->Name == RootUserName ||
        user->RecursiveMemberOf.contains(SuperusersGroupName))
    {
        return ESecurityAction::Allow;
    }
    // TODO(danilalexeev): YT-24575. Deny permission for banned users.
    return ESecurityAction::Undefined;
}

////////////////////////////////////////////////////////////////////////////////

using TAceFilterCallback = std::function<bool(const TSerializableAccessControlEntry&)>;

TAceFilterCallback CreateUserTagsAceFilter(const TBooleanFormulaTags& tags)
{
    return [&] (const TSerializableAccessControlEntry& ace) -> bool {
        return !ace.SubjectTagFilter.has_value() || ace.SubjectTagFilter->IsSatisfiedBy(tags);
    };
}

class TMatchAceSubjectCallback
{
public:
    TMatchAceSubjectCallback(
        TUserDescriptorPtr user,
        TUserDirectoryPtr userDirectory)
        : User_(std::move(user))
        , UserDirectory_(std::move(userDirectory))
    { }

    TSubjectId operator()(const std::string& subject) const
    {
        // TODO(danilalexeev): YT-24542. Support the "owner" keyword.
        static const TStringBuf OwnerKeyword = "owner";
        if (subject == OwnerKeyword) {
            THROW_ERROR_EXCEPTION(
                "Permission validation failed: the keyword %Qv is not yet supported in Sequoia",
                OwnerKeyword);
        }

        return CheckSubjectMatch(subject);
    }

private:
    const TUserDescriptorPtr User_;
    const TUserDirectoryPtr UserDirectory_;

    TSubjectId CheckSubjectMatch(const std::string& subject) const
    {
        // Check if the subject is a user.
        if (auto descriptor = UserDirectory_->FindUserByNameOrAlias(subject)) {
            // NB: Pointer comparison is unreliable as user descriptors are
            // recreated on each synchronization epoch.
            return descriptor->Name == User_->Name ? descriptor->SubjectId : NullObjectId;
        }

        // Overwise, the subject is a group.
        if (auto descriptor = UserDirectory_->FindGroupByNameOrAlias(subject)) {
            return User_->RecursiveMemberOf.contains(descriptor->Name)
                ? descriptor->SubjectId
                : NullObjectId;
        }

        THROW_ERROR_EXCEPTION(
            "Permission validation failed: unknown ACE subject %Qv",
            subject);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TPermissionCheckResponse CheckPermissionForNode(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry,
    EPermission permission,
    const TPermissionCheckBasicOptions& options,
    TUserDirectoryPtr userDirectory)
{
    auto user = sequoiaSession->GetCurrentAuthenticatedUser();
    if (auto fastAction = FastCheckPermission(user); fastAction != ESecurityAction::Undefined) {
        return MakeFastCheckPermissionResponse(fastAction, options);
    }

    auto aceFilter = CreateUserTagsAceFilter(user->Tags);

    using TChecker = NSecurityServer::TPermissionChecker<
        TSerializableAccessControlEntry,
        TMatchAceSubjectCallback>;
    auto checker = TChecker(
        permission,
        TMatchAceSubjectCallback(
            std::move(user),
            std::move(userDirectory)),
        &options);
    YT_VERIFY(checker.ShouldProceed());

    auto ancestryAcds = sequoiaSession
        ->GetAcdFetcher()
        ->Fetch({nodeAncestry});

    int depth = 0;
    for (const auto& acd : ancestryAcds | std::views::reverse) {
        for (const auto& ace : acd->Acl.Entries | std::views::filter(aceFilter)) {
            checker.ProcessAce(ace, acd->NodeId, depth);

            if (!checker.ShouldProceed()) {
                return std::move(checker).GetResponse();
            }
        }
        if (!acd->Inherit) {
            break;
        }
        ++depth;
    }
    return std::move(checker).GetResponse();
}

void ValidatePermissionForNode(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry,
    EPermission permission,
    TUserDirectoryPtr userDirectory)
{
    TPermissionCheckBasicOptions options;
    auto response = CheckPermissionForNode(
        sequoiaSession,
        nodeAncestry,
        permission,
        options,
        userDirectory);
    if (response.Action == ESecurityAction::Allow) {
        return;
    }

    auto user = sequoiaSession->GetCurrentAuthenticatedUser();
    LogAndThrowAuthorizationError(
        response,
        permission,
        std::move(user),
        std::move(userDirectory),
        nodeAncestry);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSubtreePermissionChecker
{
public:
    TSubtreePermissionChecker(
        EPermission permission,
        TUserDescriptorPtr user,
        TUserDirectoryPtr userDirectory)
        : AceFilter_(CreateUserTagsAceFilter(user->Tags))
        , MatchAceSubjectCallback_(std::move(user), std::move(userDirectory))
        , Underlying_(permission, MatchAceSubjectCallback_)
    { }

    void Put(const TAccessControlDescriptor* acd)
    {
        Underlying_.Put(
            acd->Acl.Entries | std::views::filter(AceFilter_),
            acd->NodeId,
            acd->Inherit);
    }

    void Pop()
    {
        Underlying_.Pop();
    }

    TPermissionCheckResult CheckPermission() const
    {
        return Underlying_.CheckPermission();
    }

private:
    const TAceFilterCallback AceFilter_;

    TMatchAceSubjectCallback MatchAceSubjectCallback_;

    using TChecker = NSecurityServer::TSubtreePermissionChecker<
        TSerializableAccessControlEntry,
        const TMatchAceSubjectCallback&>;
    TChecker Underlying_;
};

// A struct representing a node and its corresponding ACD.
struct TNode
{
    const TCypressNodeDescriptor* Descriptor;
    const TAccessControlDescriptor* Acd;
};

// A visitor class to handle permission checks during tree traversal.
class TNodeVisitor
    : public NCypressProxy::INodeVisitor<TNode>
{
public:
    explicit TNodeVisitor(TSubtreePermissionChecker* checker)
        : PermissionChecker_(checker)
    { }

    TPermissionCheckResult GetResult() &&
    {
        if (Result_.has_value()) {
            YT_VERIFY(Result_->Action != ESecurityAction::Allow);
            return std::move(*Result_);
        }
        return TPermissionCheckResult{.Action = ESecurityAction::Allow};
    }

private:
    TSubtreePermissionChecker* const PermissionChecker_;

    std::optional<TPermissionCheckResult> Result_;

    void OnNodeEntered(const TNode& node) override
    {
        PermissionChecker_->Put(node.Acd);
    }

    void OnNodeExited(const TNode& /*node*/) override
    {
        PermissionChecker_->Pop();
    }

    bool ShouldContinue() override
    {
        auto result = PermissionChecker_->CheckPermission();
        if (result.Action == ESecurityAction::Allow) {
            return true;
        }
        Result_ = result;
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TPermissionCheckResult CheckPermissionForSubtree(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry,
    const TSequoiaSession::TSubtree& nodeSubtree,
    EPermission permission,
    bool descendantsOnly,
    TUserDirectoryPtr userDirectory)
{
    auto user = sequoiaSession->GetCurrentAuthenticatedUser();
    if (auto fastAction = FastCheckPermission(user); fastAction != ESecurityAction::Undefined) {
        return MakeFastCheckPermissionResponse(fastAction, TPermissionCheckBasicOptions{});
    }

    TSubtreePermissionChecker checker(
        permission,
        std::move(user),
        std::move(userDirectory));

    auto subtreeNodes = TRange(nodeSubtree.Nodes);

    YT_VERIFY(nodeAncestry.Back().Id == subtreeNodes.Front().Id);
    if (descendantsOnly) {
        YT_VERIFY(!subtreeNodes.empty());
        subtreeNodes = subtreeNodes.Slice(1, subtreeNodes.size());
    } else {
        YT_VERIFY(!nodeAncestry.empty());
        nodeAncestry = nodeAncestry.Slice(0, nodeAncestry.size() - 1);
    }

    auto joinedAcds = sequoiaSession
        ->GetAcdFetcher()
        ->Fetch({nodeAncestry, subtreeNodes});

    auto ancestryAcds = joinedAcds | std::views::take(nodeAncestry.size());
    auto subtreeAcds = joinedAcds | std::views::drop(nodeAncestry.size());

    // Process the ACDs that do not match the permission check scope.
    for (const auto* acd : ancestryAcds) {
        checker.Put(acd);
    }

    auto subtreeTraversal = std::views::iota(0, std::ssize(subtreeNodes))
        | std::views::transform([&] (int i) -> TNode {
            return {&subtreeNodes[i], subtreeAcds[i]};
        });

    auto visitor = TNodeVisitor(&checker);

    auto isParentCallback = [] (const TNode& maybeParent, const TNode& child) {
        // TODO(danilalexeev): YT-24575. Use parent IDs.
        const auto& parentPath = maybeParent.Descriptor->Path;
        const auto& childPath = child.Descriptor->Path;
        return childPath.Underlying().StartsWith(parentPath.Underlying());
    };

    TraverseSequoiaTree(
        std::move(subtreeTraversal),
        &visitor,
        std::move(isParentCallback));

    return std::move(visitor).GetResult();
}

void ValidatePermissionForSubtree(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry,
    const TSequoiaSession::TSubtree& nodeSubtree,
    EPermission permission,
    bool descendantsOnly,
    TUserDirectoryPtr userDirectory)
{
    auto response = CheckPermissionForSubtree(
        sequoiaSession,
        nodeAncestry,
        nodeSubtree,
        permission,
        descendantsOnly,
        userDirectory);
    if (response.Action == ESecurityAction::Allow) {
        return;
    }

    auto user = sequoiaSession->GetCurrentAuthenticatedUser();
    LogAndThrowAuthorizationError(
        response,
        permission,
        std::move(user),
        std::move(userDirectory),
        nodeAncestry,
        &nodeSubtree);
}

////////////////////////////////////////////////////////////////////////////////

TSerializableAccessControlList ComputeEffectiveAclForNode(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry)
{
    auto ancestryAcds = sequoiaSession
        ->GetAcdFetcher()
        ->Fetch({nodeAncestry});

    TSerializableAccessControlList result;
    int depth = 0;
    for (const auto* acd : ancestryAcds | std::views::reverse) {
        for (const auto& ace : acd->Acl.Entries) {
            if (auto inheritedMode = GetInheritedInheritanceMode(ace.InheritanceMode, depth)) {
                auto adjustedAce = ace;
                adjustedAce.InheritanceMode = *inheritedMode;
                result.Entries.push_back(std::move(adjustedAce));
            }
        }
        if (!acd->Inherit) {
            break;
        }
        ++depth;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
