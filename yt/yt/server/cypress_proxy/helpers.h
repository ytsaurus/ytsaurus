#pragma once

#include "public.h"

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

void ValidateLinkNodeCreation(
    const TSequoiaSessionPtr& session,
    NSequoiaClient::TRawYPath targetPath,
    const TResolveResult& resolveResult);

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> TokenizeUnresolvedSuffix(const NSequoiaClient::TYPath& unresolvedSuffix);
NSequoiaClient::TAbsoluteYPath JoinNestedNodesToPath(
    const NSequoiaClient::TAbsoluteYPath& parentPath,
    const std::vector<TString>& childKeys);

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
std::optional<TParsedReqCreate> TryParseReqCreate(ISequoiaServiceContextPtr context);

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TCopyOptions* options,
    const NCypressClient::NProto::TReqCopy& protoOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
