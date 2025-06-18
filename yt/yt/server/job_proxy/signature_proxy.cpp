#include "signature_proxy.h"

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NExecNode;
using namespace NJobTrackerClient;
using namespace NRpc;
using namespace NSignature;

////////////////////////////////////////////////////////////////////////////////

TProxySignatureGenerator::TProxySignatureGenerator(TSupervisorServiceProxy proxy, TJobId jobId)
    : Proxy_(std::move(proxy))
    , JobId_(std::move(jobId))
{ }

void TProxySignatureGenerator::DoSign(const TSignaturePtr& signature) const
{
    auto request = Proxy_.GenerateSignature();
    ToProto(request->mutable_job_id(), JobId_);
    request->set_payload(signature->Payload());
    auto response = WaitFor(request->Invoke())
        .ValueOrThrow();
    FromProto(signature.Get(), response->signature());
}

////////////////////////////////////////////////////////////////////////////////

TProxySignatureValidator::TProxySignatureValidator(TSupervisorServiceProxy proxy, TJobId jobId)
    : Proxy_(std::move(proxy))
    , JobId_(std::move(jobId))
{ }

TFuture<bool> TProxySignatureValidator::Validate(const TSignaturePtr& signature) const
{
    auto request = Proxy_.ValidateSignature();
    ToProto(request->mutable_job_id(), JobId_);
    ToProto(request->mutable_signature(), *signature);
    return request->Invoke()
        .Apply(BIND([] (const TSupervisorServiceProxy::TErrorOrRspValidateSignaturePtr& response) {
            return response.ValueOrThrow()->valid();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
