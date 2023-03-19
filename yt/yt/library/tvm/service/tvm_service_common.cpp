#include "tvm_service.h"

#include <library/cpp/yt/memory/new.h>

#include <yt/yt/library/tvm/tvm_base.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TServiceTicketAuth
    : public IServiceTicketAuth
{
public:
    TServiceTicketAuth(
        ITvmServicePtr tvmService,
        TTvmId destServiceId)
        : TvmService_(std::move(tvmService))
        , DstServiceId_(destServiceId)
    { }

    TString IssueServiceTicket() override
    {
        return TvmService_->GetServiceTicket(DstServiceId_);
    }

private:
    ITvmServicePtr TvmService_;
    TTvmId DstServiceId_;
};

////////////////////////////////////////////////////////////////////////////////

IServiceTicketAuthPtr CreateServiceTicketAuth(
    ITvmServicePtr tvmService,
    TTvmId dstServiceId)
{
    YT_VERIFY(tvmService);

    return New<TServiceTicketAuth>(std::move(tvmService), dstServiceId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
