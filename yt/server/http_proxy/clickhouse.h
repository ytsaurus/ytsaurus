#include "public.h"

#include <yt/core/http/server.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TClickHouseHandler(TBootstrap* bootstrap);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    TBootstrap* const Bootstrap_;
    const TCoordinatorPtr Coordinator_;
    const TClickHouseConfigPtr Config_;
    const NHttp::IClientPtr HttpClient_;
    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
    IInvokerPtr ControlInvoker_;

    THashMap<TString, int> UserToRunningQueryCount_;

    //! Change internal user -> query count mapping value, which is used in profiling.
    /*!
     *  \note Invoker affinity: Control invoker
     */
    void AdjustQueryCount(const TString& user, int delta);

    void OnProfiling();
};

DEFINE_REFCOUNTED_TYPE(TClickHouseHandler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
