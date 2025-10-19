#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

TError WrapCypressProxyRegistrationError(TError error);

////////////////////////////////////////////////////////////////////////////////

// Checks whether #error contains a resolve error for the #resolvedNodeId.
// If so, wraps it into a Sequoia retriable error and returns that.
// Otherwise, returns an OK error.
TError WrapRetriableResolveError(const TError& error, NCypressClient::TNodeId resolvedNodeId);

////////////////////////////////////////////////////////////////////////////////

void SetAccessTrackingOptions(
    const NRpc::IClientRequestPtr& request,
    const NApi::TSuppressableAccessTrackingOptions& commandOptions);

////////////////////////////////////////////////////////////////////////////////

void ValidateLinkNodeCreation(
    const TSequoiaSessionPtr& session,
    const NYPath::TYPath& targetPath,
    const TResolveResult& resolveResult);

////////////////////////////////////////////////////////////////////////////////

std::vector<NSequoiaClient::TPrerequisiteRevision> GetPrerequisiteRevisions(const NRpc::NProto::TRequestHeader& header);

TError CheckPrerequisitesAfterRequestInvocation(
    const NRpc::NProto::TRequestHeader& header,
    const TSequoiaSessionPtr& session,
    const NSequoiaClient::ISequoiaClientPtr& sequoiaClient,
    const NYPath::TYPath& originalTargetPath,
    const std::vector<NSequoiaClient::TPrerequisiteRevision>& prerequisiteRevisions,
    const std::vector<NObjectClient::TTransactionId>& prerequisiteTransactionIds);

TErrorOr<std::vector<NSequoiaClient::TResolvedPrerequisiteRevision>> ResolvePrerequisiteRevisions(
    const NRpc::NProto::TRequestHeader& header,
    const TSequoiaSessionPtr& session,
    const NYPath::TYPath& originalTargetPath,
    const std::vector<NSequoiaClient::TPrerequisiteRevision>& prerequisiteRevisions);

TError CheckPrerequisiteRevisionsPaths(
    const NRpc::NProto::TRequestHeader& header,
    const NYPath::TYPath& originalTargetPath,
    const std::vector<NSequoiaClient::TPrerequisiteRevision>& prerequisiteRevisions);

TError CheckPrerequisiteTransactions(
    const NSequoiaClient::ISequoiaClientPtr& sequoiaClient,
    const std::vector<NObjectClient::TTransactionId>& prerequisiteTransactionIds);

////////////////////////////////////////////////////////////////////////////////

struct TSlashRootDesignatorTag
{ };

using TRootDesignator = std::variant<NObjectClient::TObjectId, TSlashRootDesignatorTag>;

//! Returns the root designator, throws if path does not contain any.
//! Validates GUID in case of object root designator.
std::pair<TRootDesignator, NYPath::TYPathBuf> GetRootDesignator(NYPath::TYPathBuf path);

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> TokenizeUnresolvedSuffix(NYPath::TYPathBuf unresolvedSuffix);
NSequoiaClient::TAbsolutePath JoinNestedNodesToPath(
    const NSequoiaClient::TAbsolutePath& parentPath,
    const std::vector<std::string>& childKeys);

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(NCypressClient::EObjectType type);
bool IsSequoiaCompositeNodeType(NCypressClient::EObjectType type);
void ValidateSupportedSequoiaType(NCypressClient::EObjectType type);
[[noreturn]] void ThrowAlreadyExists(const NSequoiaClient::TAbsolutePath& path);
[[noreturn]] void ThrowCannotHaveChildren(const NSequoiaClient::TAbsolutePath& path);
[[noreturn]] void ThrowCannotReplaceNode(const NSequoiaClient::TAbsolutePath& path);
[[noreturn]] void ThrowNoSuchChild(const NSequoiaClient::TAbsolutePath& existingPath, TStringBuf missingPath);

////////////////////////////////////////////////////////////////////////////////

struct TParsedReqCreate
{
    NObjectClient::EObjectType Type;
    NYTree::IAttributeDictionaryPtr ExplicitAttributes;
};

//! On parse error replies it to underlying context and returns |nullopt|.
std::optional<TParsedReqCreate> TryParseReqCreate(const ISequoiaServiceContextPtr& context);

////////////////////////////////////////////////////////////////////////////////

void ConsumeAttributes(NYson::IAsyncYsonConsumer* consumer, const NYTree::IAttributeDictionaryPtr& attributes);

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TCopyOptions* options,
    const NCypressClient::NProto::TReqCopy& protoOptions);

void FromProto(
    TMultisetAttributesSubrequest* subrequest,
    const NYTree::NProto::TReqMultisetAttributes::TSubrequest& protoSubrequest);

////////////////////////////////////////////////////////////////////////////////

//! Fetches single object from follower using vectorized read. Therefore, there
//! is no resolve step on master.
//! All access/expiration tracking is suppressed.
TFuture<NYTree::INodePtr> FetchSingleObject(
    const NApi::NNative::IClientPtr& client,
    NCypressClient::TVersionedObjectId objectId,
    const NYTree::TAttributeFilter& attributeFilter);
TFuture<NYTree::IAttributeDictionaryPtr> FetchSingleObjectAttributes(
    const NApi::NNative::IClientPtr& client,
    NCypressClient::TVersionedObjectId objectId,
    const NYTree::TAttributeFilter& attributeFilter);

////////////////////////////////////////////////////////////////////////////////

std::string GetRequestQueueNameForKey(const std::pair<std::string, EUserWorkloadType>& userNameAndWorkloadType);

std::string GetDistributedWeightThrottlerId(const std::string& prefix);

////////////////////////////////////////////////////////////////////////////////

std::string BuildMultipleTransactionSelectCondition(
    TRange<NCypressClient::TTransactionId> transactionIds);

////////////////////////////////////////////////////////////////////////////////

class TInheritedAttributesCalculator
{
public:
    TInheritedAttributesCalculator() = default;

    // As in "change dir" aka "cd" except that non-composite nodes can be entered, too.
    // The nodes are supposed to be traversed pre-order, as induced by the result of FetchSubtree.
    void ChangeNode(NSequoiaClient::TAbsolutePath path, const NYTree::IAttributeDictionary* inheritableAttributes);

    const NYTree::IConstAttributeDictionaryPtr& GetCurrentInheritedAttributes() const;
    const NYTree::IConstAttributeDictionaryPtr& GetParentInheritedAttributes() const;

private:
    struct TNodeDescriptor
    {
        NSequoiaClient::TAbsolutePath Path;
        NYTree::IConstAttributeDictionaryPtr InheritedAttributes;
    };

    std::vector<TNodeDescriptor> Ancestry_;
};

NYTree::IConstAttributeDictionaryPtr CalculateInheritedAttributes(
    TNodeAncestry ancestry,
    const TNodeIdToConstAttributes& inheritableAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
