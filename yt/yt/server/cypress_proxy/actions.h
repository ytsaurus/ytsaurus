#pragma once

#include "public.h"

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/ypath_detail.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/yt/yson_string/string.h>

// NB: almost always these helpers should not be called directly since they
// don't take into account transactions and branches. Moreover, in the future
// tx action ordering should be moved from Sequoia tx to |TSequoiaSession| which
// means that _all_ interactions with Cypress should be done via
// |TSequoiaSession|.
// TODO(kvk1920 or someone else): place all these helpers inside
// |TSequoiaSession| and make them private.

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

void SetNode(
    NCypressClient::TNodeId id,
    const NYson::TYsonString& value,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void CreateNode(
    NCypressClient::TNodeId id,
    NSequoiaClient::TAbsoluteYPathBuf path,
    const NYTree::IAttributeDictionary* explicitAttributes,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

NCypressClient::TNodeId CopyNode(
    const NSequoiaClient::NRecords::TNodeIdToPath& sourceNode,
    NSequoiaClient::TAbsoluteYPathBuf destinationNodePath,
    const TCopyOptions& options,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Removes node but not detaches it from its parent.
void RemoveNode(
    NCypressClient::TNodeId id,
    const NSequoiaClient::TMangledSequoiaPath& path,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

//! Changes visible to user must be applied at coordinator with late prepare mode.
void AttachChild(
    NCypressClient::TNodeId parentId,
    NCypressClient::TNodeId childId,
    const TString& childKey,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void DetachChild(
    NCypressClient::TNodeId parentId,
    const TString& childKey,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction);

void LockRowInNodeIdToPathTable(
    NCypressClient::TNodeId nodeId,
    const NSequoiaClient::ISequoiaTransactionPtr& transaction,
    NTableClient::ELockType lockType);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NCypressServer::NProto::TReqCloneNode::TCloneOptions* protoOptions,
    const TCopyOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
