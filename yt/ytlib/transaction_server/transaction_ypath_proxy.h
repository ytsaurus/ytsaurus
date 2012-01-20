#include "transaction_ypath.pb.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/object_server/object_ypath_proxy.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

extern NYTree::TYPath RootTransactionPath;

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): add expiration timeout
struct TTransactionManifest
    : public TConfigurable
{
    TDuration Timeout;

    TTransactionManifest()
    {
        Register("timeout", Timeout).Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTransactionYPathProxy
    : public NObjectServer::TObjectYPathProxy
{
    DEFINE_YPATH_PROXY_METHOD(NProto, Commit);
    DEFINE_YPATH_PROXY_METHOD(NProto, Abort);
    DEFINE_YPATH_PROXY_METHOD(NProto, RenewLease);

    DEFINE_YPATH_PROXY_METHOD(NProto, CreateObject);
    DEFINE_YPATH_PROXY_METHOD(NProto, ReleaseObject);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
