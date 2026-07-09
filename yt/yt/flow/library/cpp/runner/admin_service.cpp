#include "admin_service.h"

#include "private.h"

#include <yt/yt/flow/library/cpp/common/admin_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/yt/system/exit.h>

namespace NYT::NFlow {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TAdminService
    : public TServiceBase
{
public:
    TAdminService(
        IInvokerPtr invoker,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            std::move(invoker),
            TAdminServiceProxy::GetDescriptor(),
            NodeLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Die));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NProto, Die)
    {
        AbortProcessSilently(request->exit_code());
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateAdminService(
    IInvokerPtr invoker,
    IAuthenticatorPtr authenticator)
{
    return New<TAdminService>(
        std::move(invoker),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
