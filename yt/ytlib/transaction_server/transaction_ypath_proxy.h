#pragma once

#include "transaction_ypath.pb.h"

#include <ytlib/ytree/ypath_proxy.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManifest
    : public TConfigurable
{
    TTransactionId ParentId;

    TTransactionManifest()
    {
        Register("parent_id", ParentId)
            .Default(NullTransactionId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTransactionYPathProxy
    : NYTree::TYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Commit);
    DEFINE_YPATH_PROXY_METHOD(NProto, Abort);
    DEFINE_YPATH_PROXY_METHOD(NProto, RenewLease);
    DEFINE_YPATH_PROXY_METHOD(NProto, Release);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
