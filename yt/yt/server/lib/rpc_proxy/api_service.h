#pragma once

#include "public.h"

#include <yt/yt/client/signature/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/library/tracing/jaeger/public.h>

#include <yt/yt/core/rpc/service.h>

#include <yt/yt/library/profiling/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IApiService
    : public virtual NRpc::IService
{
    //! Thread affinity: any.
    virtual void OnDynamicConfigChanged(const TApiServiceDynamicConfigPtr& config) = 0;

    virtual NYTree::IYPathServicePtr CreateOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IApiService)

////////////////////////////////////////////////////////////////////////////////

//! Custom #stickyTransactionPool is useful for sharing transactions
//! between services (e.g.: ORM and RPC proxy).
IApiServicePtr CreateApiService(
    TApiServiceConfigPtr config,
    IInvokerPtr controlInvoker,
    IInvokerPtr workerInvoker,
    NApi::NNative::IConnectionPtr connection,
    NRpc::IAuthenticatorPtr authenticator,
    IProxyCoordinatorPtr proxyCoordinator,
    IAccessCheckerPtr accessChecker,
    ISecurityManagerPtr securityManager,
    NTracing::TSamplerPtr traceSampler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    NSignature::ISignatureValidatorPtr signatureValidator,
    NSignature::ISignatureGeneratorPtr signatureGenerator,
    INodeMemoryTrackerPtr memoryUsageTracker = {},
    NApi::IStickyTransactionPoolPtr stickyTransactionPool = {},
    IQueryCorpusReporterPtr queryCorpusReporter = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
