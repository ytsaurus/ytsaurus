#pragma once

#include "public.h"

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>
#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TCopyOptions
{
    NCypressClient::ENodeCloneMode Mode = NCypressClient::ENodeCloneMode::Copy;
    bool PreserveAcl = false;
    bool PreserveAccount = false;
    bool PreserveOwner = false;
    bool PreserveCreationTime = false;
    bool PreserveModificationTime = false;
    bool PreserveExpirationTime = false;
    bool PreserveExpirationTimeout = false;
    bool PessimisticQuotaCheck = false;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<NYPath::TYPathBuf> TokenizeUnresolvedSuffix(const NYPath::TYPath& unresolvedSuffix);

NYPath::TYPath JoinNestedNodesToPath(
    const NYPath::TYPath& parentPath,
    const std::vector<NYPath::TYPathBuf>& childKeys);

bool IsSupportedSequoiaType(NCypressClient::EObjectType type);

bool IsSequoiaCompositeNodeType(NCypressClient::EObjectType type);

void ValidateSupportedSequoiaType(NCypressClient::EObjectType type);

void ThrowAlreadyExists(const NYPath::TYPath& path);

void ThrowNoSuchChild(const NYPath::TYPath& existingPath, const NYPath::TYPathBuf& missingPath);

////////////////////////////////////////////////////////////////////////////////

std::vector<NSequoiaClient::NRecords::TPathToNodeId> SelectSubtree(
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

NCypressClient::TNodeId LookupNodeId(
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NCypressServer::NProto::TReqCloneNode::TCloneOptions* protoOptions,
    const TCopyOptions& options);
void FromProto(
    TCopyOptions* options,
    const NCypressClient::NProto::TReqCopy& protoOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
