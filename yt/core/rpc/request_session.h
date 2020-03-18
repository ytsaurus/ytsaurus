#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/public.h>

#include <yt/core/logging/log.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServerAddressPool
    : public TRefCounted
{
public:
    TServerAddressPool(
        TDuration banTimeout,
        const NLogging::TLogger& Logger,
        const std::vector<TString>& addresses);

    std::vector<TString> GetUpAddresses();
    std::vector<TString> GetProbationAddresses();

    void BanAddress(const TString& address);
    void UnbanAddress(const TString& address);

private:
    const TDuration BanTimeout_;
    const NLogging::TLogger Logger;

    TSpinLock Lock_;
    THashSet<TString> UpAddresses_;
    THashSet<TString> ProbationAddresses_;
    THashSet<TString> DownAddresses_;

    void OnBanTimeoutExpired(const TString& address);
};

DEFINE_REFCOUNTED_TYPE(TServerAddressPool)

////////////////////////////////////////////////////////////////////////////////

template <class TResponse>
class TRequestSession
    : public TRefCounted
{
public:
    TRequestSession(
        int requiredSuccessCount,
        TServerAddressPoolPtr addressPool,
        const NLogging::TLogger& logger);

    TFuture<TResponse> Run();

protected:
    const int RequiredSuccessCount_;
    const TPromise<TResponse> Promise_;

    virtual TFuture<void> MakeRequest(const TString& address) = 0;

private:
    const NRpc::TServerAddressPoolPtr AddressPool_;
    const NLogging::TLogger Logger;

    TSpinLock AddressesLock_;

    std::vector<TString> UpAddresses_;
    int CurrentUpAddressIndex_ = 0;

    std::vector<TString> ProbationAddresses_;
    int CurrentProbationAddressIndex_ = 0;

    TSpinLock ErrorsLock_;
    std::vector<TError> Errors_;

    void AddError(const TError& error);
    TError CreateError();
    void TryMakeNextRequest(bool forceProbation);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define REQUEST_SESSION_INL_H_
#include "request_session-inl.h"
#undef REQUEST_SESSION_INL_H_
