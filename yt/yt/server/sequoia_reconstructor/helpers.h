#pragma once

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSequoiaReconstructor {

YT_DEFINE_GLOBAL(const NLogging::TLogger, SequoiaReconstructorLogger, "SequoiaReconstructor");

////////////////////////////////////////////////////////////////////////////////

struct TPathToNodeChangeRecord
    : public NYTree::TYsonStructLite
{
    NSequoiaClient::TMangledSequoiaPath Path;
    NCypressClient::TNodeId NodeId = NCypressClient::NullObjectId;
    NCypressClient::TTransactionId TransactionId = NCypressClient::NullTransactionId;
    std::vector<NCypressClient::TTransactionId> TransactionAncestors;

    REGISTER_YSON_STRUCT_LITE(TPathToNodeChangeRecord);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
