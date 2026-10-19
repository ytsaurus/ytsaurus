#pragma once

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yson/node/node.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

struct TPathToNodeChangeRecord
    : public NYTree::TYsonStructLite
{
    NSequoiaClient::TMangledSequoiaPath Path;
    NCypressClient::TNodeId NodeId = NCypressClient::NullObjectId;
    NCypressClient::TTransactionId TransactionId = NCypressClient::NullTransactionId;
    std::vector<NCypressClient::TTransactionId> TransactionAncestors;

    TNode ToNode() const;

    static TPathToNodeChangeRecord FromNode(const TNode& node);

    REGISTER_YSON_STRUCT_LITE(TPathToNodeChangeRecord);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

void RunWithBootstrap(
    const std::string& masterConfigPath,
    const std::string& snapshotPath,
    const std::function<void(NCellMaster::TBootstrap*)>& callback,
    bool abortProcessAfterFinish);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
