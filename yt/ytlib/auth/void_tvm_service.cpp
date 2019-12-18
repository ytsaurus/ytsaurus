#include "void_tvm_service.h"
#include "tvm_service.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TVoidTvmService
    : public ITvmService
{
public:
    TVoidTvmService()
    { }

    TFuture<TString> GetTicket(const TString& serviceId) override
    {
        return MakeFuture<TString>(TString());
    }
};

ITvmServicePtr CreateVoidTvmService()
{
    return New<TVoidTvmService>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
