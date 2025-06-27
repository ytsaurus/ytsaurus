#pragma once

#include "public.h"

#include <yt/yt/server/lib/exec_node/supervisor_service_proxy.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/validator.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TProxySignatureGenerator
    : public NSignature::ISignatureGenerator
{
public:
    TProxySignatureGenerator(NExecNode::TSupervisorServiceProxy proxy, NJobTrackerClient::TJobId jobId);

private:
    const NExecNode::TSupervisorServiceProxy Proxy_;
    const NJobTrackerClient::TJobId JobId_;

    void DoSign(const NSignature::TSignaturePtr& signature) const final;
};

DEFINE_REFCOUNTED_TYPE(TProxySignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

class TProxySignatureValidator
    : public NSignature::ISignatureValidator
{
public:
    TProxySignatureValidator(NExecNode::TSupervisorServiceProxy proxy, NJobTrackerClient::TJobId jobId);

    TFuture<bool> Validate(const NSignature::TSignaturePtr& signature) const final;

private:
    const NExecNode::TSupervisorServiceProxy Proxy_;
    const NJobTrackerClient::TJobId JobId_;
};

DEFINE_REFCOUNTED_TYPE(TProxySignatureValidator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
