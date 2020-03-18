#pragma once
#ifndef REQUEST_SESSION_INL_H_
#error "Direct inclusion of this file is not allowed, include request_session.h"
// For the sake of sane code completion.
#include "request_session.h"
#endif
#undef REQUEST_SESSION_INL_H_

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/public.h>

#include <yt/core/ytree/attributes.h>

#include <yt/core/logging/log.h>

#include <util/random/shuffle.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TResponse>
TRequestSession<TResponse>::TRequestSession(
    int requiredSuccessCount,
    TServerAddressPoolPtr addressPool,
    const NLogging::TLogger& logger)
    : RequiredSuccessCount_(requiredSuccessCount)
    , Promise_(NewPromise<TResponse>())
    , AddressPool_(std::move(addressPool))
    , Logger(logger)
{ }

template <class TResponse>
TFuture<TResponse> TRequestSession<TResponse>::Run()
{
    UpAddresses_ = AddressPool_->GetUpAddresses();
    ProbationAddresses_ = AddressPool_->GetProbationAddresses();

    YT_LOG_DEBUG("Request session started (UpAddresses: %v, ProbationAddresses: %v)",
        UpAddresses_,
        ProbationAddresses_);

    Shuffle(UpAddresses_.begin(), UpAddresses_.end());
    Shuffle(ProbationAddresses_.begin(), ProbationAddresses_.end());

    if (!ProbationAddresses_.empty()) {
        TryMakeNextRequest(true);
    }

    for (int i = 0; i < RequiredSuccessCount_; ++i) {
        TryMakeNextRequest(false);
    }

    return Promise_;
}

template <class TResponse>
void TRequestSession<TResponse>::AddError(const TError& error)
{
    TGuard guard(ErrorsLock_);
    Errors_.push_back(error);
}

template <class TResponse>
TError TRequestSession<TResponse>::CreateError()
{
    TGuard errorsGuard(ErrorsLock_);
    return TError("There are not enough healthy servers known") << Errors_;
}

template <class TResponse>
void TRequestSession<TResponse>::TryMakeNextRequest(bool forceProbation)
{
    TString address;

    {
        TGuard addressesGuard(AddressesLock_);
        if (CurrentUpAddressIndex_ < UpAddresses_.size() && !forceProbation) {
            address = UpAddresses_[CurrentUpAddressIndex_++];
            YT_LOG_DEBUG("Sending request to up address (Address: %v)", address);
        } else if (CurrentProbationAddressIndex_ < ProbationAddresses_.size()) {
            address = ProbationAddresses_[CurrentProbationAddressIndex_++];
            YT_LOG_DEBUG("Sending request to probation address (Address: %v)", address);
        } else {
            addressesGuard.Release();

            auto error = CreateError();
            Promise_.TrySet(error);
            return;
        }
    }

    MakeRequest(address).Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<void>& errorOrValue) {
        if (!errorOrValue.IsOK()) {
            AddError(errorOrValue);
            TryMakeNextRequest(false);
            AddressPool_->BanAddress(address);
        } else {
            AddressPool_->UnbanAddress(address);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define REQUEST_SESSION_INL_H_
#include "request_session-inl.h"
#undef REQUEST_SESSION_INL_H_
