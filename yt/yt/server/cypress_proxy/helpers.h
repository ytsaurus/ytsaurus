#pragma once

#include "public.h"
#include "action_helpers.h"

#include <yt/yt/ytlib/sequoia_client/records/path_to_node_id.record.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TRecursiveCreateResult
{
    TString SubtreeRootKey;
    NCypressClient::TNodeId SubtreeRootId;
    NCypressClient::TNodeId DesignatedNodeId;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<NYPath::TYPathBuf> TokenizeUnresolvedSuffix(const NYPath::TYPath& unresolvedSuffix);
NYPath::TYPath JoinNestedNodesToPath(
    const NYPath::TYPath& parentPath,
    const std::vector<NYPath::TYPathBuf>& childKeys);

////////////////////////////////////////////////////////////////////////////////

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

NCypressClient::TNodeId CopySubtree(
    const std::vector<NSequoiaClient::NRecords::TPathToNodeId>& sourceNodes,
    const NYPath::TYPath& sourceRootPath,
    const NYPath::TYPath& destinationRootPath,
    const TCopyOptions& options,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! This function intentionally does not support the removal of a scion with a #removeRoot flag set to true.
TFuture<void> RemoveSubtree(
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    bool removeRoot = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
