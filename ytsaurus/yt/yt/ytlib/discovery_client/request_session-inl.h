#ifndef REQUEST_SESSION_INL_H_
#error "Direct inclusion of this file is not allowed, include request_session.h"
// For the sake of sane code completion.
#include "request_session.h"
#endif
#undef REQUEST_SESSION_INL_H_

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/logging/log.h>

#include <util/random/shuffle.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

template <class TResponse>
TRequestSession<TResponse>::TRequestSession(
    std::optional<int> requiredSuccessCount,
    TServerAddressPoolPtr addressPool,
    const NLogging::TLogger& logger)
    : OptionalRequiredSuccessCount_(requiredSuccessCount)
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
        HasExtraProbationRequest_ = true;
        TryMakeNextRequest(true);
    }

    for (int i = 0; i < GetRequiredSuccessCount(); ++i) {
        TryMakeNextRequest(false);
    }

    return Promise_;
}

template <class TResponse>
void TRequestSession<TResponse>::AddError(const TError& error)
{
    auto guard = Guard(ErrorsLock_);
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
        if (CurrentUpAddressIndex_ < std::ssize(UpAddresses_) && !forceProbation) {
            address = UpAddresses_[CurrentUpAddressIndex_++];
            YT_LOG_DEBUG("Sending request to up address (Address: %v)", address);
        } else if (CurrentProbationAddressIndex_ < std::ssize(ProbationAddresses_)) {
            address = ProbationAddresses_[CurrentProbationAddressIndex_++];
            YT_LOG_DEBUG("Sending request to probation address (Address: %v)", address);
        } else if (HasExtraProbationRequest_) {
            HasExtraProbationRequest_ = false;
            return;
        } else {
            addressesGuard.Release();

            auto error = CreateError();
            Promise_.TrySet(error);
            return;
        }
    }

    // TODO(max42): switch to Subscribe.
    YT_UNUSED_FUTURE(MakeRequest(address).Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
        if (!error.IsOK()) {
            AddError(error);
            TryMakeNextRequest(false);
            AddressPool_->BanAddress(address);
        } else {
            AddressPool_->UnbanAddress(address);
        }
    })));
}

template <class TResponse>
int TRequestSession<TResponse>::GetRequiredSuccessCount() const
{
    if (OptionalRequiredSuccessCount_) {
        return *OptionalRequiredSuccessCount_;
    }

    return std::max(1, (AddressPool_->GetAddressCount() + 1) / 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
