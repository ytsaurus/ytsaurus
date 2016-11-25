#include "admin_service.h"

#include "admin_service_proxy.h"
#include "private.h"

#include <yt/core/rpc/service_detail.h>
#include <yt/core/rpc/public.h>

namespace NYT {
namespace NAdmin {

using namespace NLogging;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TAdminService
    : public TServiceBase
{
public:
    TAdminService(
        const IInvokerPtr& invoker)
        : TServiceBase(
            invoker,
            TAdminServiceProxy::GetDescriptor(),
            AdminLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Die));
    }

private:
    void ValidateRoot(const TStringBuf& user)
    {
        if (user != RootUserName) {
            THROW_ERROR_EXCEPTION("Only root is allowed to use AdminService");
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Die)
    {
        ValidateRoot(context->GetUser());

        _exit(request->exit_code());
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateAdminService(const IInvokerPtr& invoker)
{
    return New<TAdminService>(invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // NAdmin
} // NYT
