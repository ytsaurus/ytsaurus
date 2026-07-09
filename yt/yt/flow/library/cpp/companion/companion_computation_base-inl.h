#pragma once

#ifndef COMPANION_COMPUTATION_BASE_INL_H_
    #error "Direct inclusion of this file is not allowed, include companion_computation_base.h"
    // For the sake of sane code completion.
    #include "companion_computation_base.h"
#endif

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
TCompanionComputationBaseAdapter<TBase>::TCompanionComputationBaseAdapter(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TBase(std::move(context), std::move(dynamicContext))
    , CompanionClient_(
        this->GetContext()
            ->GetStaticResource("CompanionManager")
            ->template As<TCompanionManager>()
            ->CreateCompanionClient(this->GetContext()->StatusProfiler))
{ }

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::FetchAndValidateCompanionInfo()
{
    CompanionInfo_ = CompanionClient_->GetCompanionInfo();
    auto computationIt = CompanionInfo_->Computations.find(this->GetComputationId());
    if (computationIt == CompanionInfo_->Computations.end()) {
        THROW_ERROR_EXCEPTION("There is no corresponding computation in companion")
            << TErrorAttribute("computation_id", this->GetComputationId());
    }
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::PutJobInfoToCompanion()
{
    auto putJobRequest = New<TCompanionPutJobRequest>();
    putJobRequest->JobId = this->GetJobId();
    putJobRequest->ComputationId = this->GetComputationId();
    putJobRequest->ComputationSpec = this->GetSpec();
    putJobRequest->DynamicComputationSpec = this->GetDynamicSpec();
    putJobRequest->JobStreamSpecs = this->GetContext()->StreamSpecStorage->GetStreamSpecs();

    auto putJobResponse = CompanionClient_->PutJob(
        putJobRequest,
        this->GetContext()->ExternalMetricsReporter);
    if (putJobResponse->Status != ECompanionResponseStatus::Ok) {
        THROW_ERROR_EXCEPTION("Failed to put job to companion")
            << TErrorAttribute("job_id", putJobRequest->JobId)
            << TErrorAttribute("computation_id", this->GetComputationId());
    }
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::PutJobInfoToCompanionWithReconfigure()
{
    // Initial publish.
    PutJobInfoToCompanion();

    // Re-publish on dynamic spec changes.
    this->SubscribeOnReconfigure(BIND([this] {
        PutJobInfoToCompanion();
    }));
}

template <class TBase>
TCompanionResponsePtr TCompanionComputationBaseAdapter<TBase>::DoProcessWithCompanion(
    const TCompanionProcessRequestPtr& request)
{
    auto response = CompanionClient_->DoProcessWithCompanionSync(
        request,
        this->GetContext()->ExternalMetricsReporter);

    // Resend request with job info if job not found at companion.
    if (response->Status == ECompanionResponseStatus::JobNotFound) {
        request->SendJobInfo = true;
        response = CompanionClient_->DoProcessWithCompanionSync(
            request,
            this->GetContext()->ExternalMetricsReporter);
    }

    if (response->Status != ECompanionResponseStatus::Ok) {
        THROW_ERROR_EXCEPTION("Failed to process with companion")
            << TErrorAttribute("job_id", this->GetJobId())
            << TErrorAttribute("computation_id", this->GetComputationId())
            << TErrorAttribute("status", response->Status);
    }

    return response;
}

template <class TBase>
template <class TRequestType>
TIntrusivePtr<TRequestType> TCompanionComputationBaseAdapter<TBase>::CreateCompanionRequest()
{
    auto request = New<TRequestType>();
    request->JobId = this->GetJobId();
    request->ComputationId = this->GetComputationId();
    request->ComputationSpec = this->GetSpec();
    request->DynamicComputationSpec = this->GetDynamicSpec();
    request->JobStreamSpecs = this->GetContext()->StreamSpecStorage->GetStreamSpecs();
    return request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
