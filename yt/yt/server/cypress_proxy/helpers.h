#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

void SetAccessTrackingOptions(
    const NRpc::IClientRequestPtr& request,
    const NApi::TSuppressableAccessTrackingOptions& commandOptions);

////////////////////////////////////////////////////////////////////////////////

void ValidateLinkNodeCreation(
    const TSequoiaSessionPtr& session,
    NSequoiaClient::TRawYPath targetPath,
    const TResolveResult& resolveResult);

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> TokenizeUnresolvedSuffix(NSequoiaClient::TYPathBuf unresolvedSuffix);
NSequoiaClient::TAbsoluteYPath JoinNestedNodesToPath(
    const NSequoiaClient::TAbsoluteYPath& parentPath,
    const std::vector<std::string>& childKeys);

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedSequoiaType(NCypressClient::EObjectType type);
bool IsSequoiaCompositeNodeType(NCypressClient::EObjectType type);
void ValidateSupportedSequoiaType(NCypressClient::EObjectType type);
void ThrowAlreadyExists(const NSequoiaClient::TAbsoluteYPath& path);
void ThrowNoSuchChild(const NSequoiaClient::TAbsoluteYPath& existingPath, TStringBuf missingPath);

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
TFuture<NYTree::INodePtr> FetchSingleObject(
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

} // namespace NYT::NCypressProxy
