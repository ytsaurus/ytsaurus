#pragma once

#include "public.h"

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>
#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/yt/yson_string/string.h>

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

void WriteSequoiaNodeRows(
    NCypressClient::TNodeId id,
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void DeleteSequoiaNodeRows(
    NCypressClient::TNodeId id,
    const NSequoiaClient::TMangledSequoiaPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void SetNode(
    NCypressClient::TNodeId id,
    const NYson::TYsonString& value,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void CreateNode(
    NCypressClient::EObjectType type,
    NCypressClient::TNodeId id,
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

NCypressClient::TNodeId CopyNode(
    NCypressClient::TNodeId sourceNodeId,
    const NYPath::TYPath& destinationNodePath,
    const TCopyOptions& options,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void RemoveNode(
    NCypressClient::TNodeId id,
    const NSequoiaClient::TMangledSequoiaPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Changes visible to user must be applied at coordinator with late prepare mode.
void AttachChild(
    NCypressClient::TNodeId parentId,
    NCypressClient::TNodeId childId,
    const NYPath::TYPath& childKey,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void DetachChild(
    NCypressClient::TNodeId parentId,
    const TString& childKey,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void LockRowInPathToIdTable(
    const NYPath::TYPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    NTableClient::ELockType lockType = NTableClient::ELockType::SharedStrong);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NCypressServer::NProto::TReqCloneNode::TCloneOptions* protoOptions,
    const TCopyOptions& options);
void FromProto(
    TCopyOptions* options,
    const NCypressClient::NProto::TReqCopy& protoOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
