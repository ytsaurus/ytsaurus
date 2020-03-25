#include "request_session.h"

#include <yt/core/misc/public.h>

#include <yt/core/concurrency/delayed_executor.h>

namespace NYT::NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TServerAddressPool::TServerAddressPool(
    TDuration banTimeout,
    const NLogging::TLogger& logger,
    const std::vector<TString>& addresses)
    : BanTimeout_(banTimeout)
    , Logger(logger)
    , UpAddresses_(addresses.begin(), addresses.end())
{ }

std::vector<TString> TServerAddressPool::GetUpAddresses()
{
    TGuard guard(Lock_);
    return {UpAddresses_.begin(), UpAddresses_.end()};
}

std::vector<TString> TServerAddressPool::GetProbationAddresses()
{
    TGuard guard(Lock_);
    return {ProbationAddresses_.begin(), ProbationAddresses_.end()};
}

void TServerAddressPool::BanAddress(const TString& address)
{
    {
        TGuard guard(Lock_);
        auto it = UpAddresses_.find(address);
        if (it == UpAddresses_.end()) {
            YT_LOG_WARNING("Cannot ban server: server is already banned (Address: %v)", address);
            return;
        }
        UpAddresses_.erase(it);
        DownAddresses_.insert(address);
    }

    TDelayedExecutor::Submit(
        BIND(&TServerAddressPool::OnBanTimeoutExpired, MakeWeak(this), address),
        BanTimeout_);

    YT_LOG_DEBUG("Server banned (Address: %v)", address);
}

void TServerAddressPool::UnbanAddress(const TString& address)
{
    TGuard guard(Lock_);

    auto it = ProbationAddresses_.find(address);
    if (it == ProbationAddresses_.end()) {
        YT_LOG_DEBUG("Cannot unban server: server is not in probation list (Address: %v)", address);
        return;
    }
    YT_LOG_DEBUG("Server unbanned (Address: %v)", address);
    ProbationAddresses_.erase(it);
    UpAddresses_.insert(address);
}

void TServerAddressPool::OnBanTimeoutExpired(const TString& address)
{
    TGuard guard(Lock_);

    auto it = DownAddresses_.find(address);
    if (it == DownAddresses_.end()) {
        return;
    }

    YT_LOG_DEBUG("Server moved to probation list (Address: %v)", address);
    DownAddresses_.erase(it);
    ProbationAddresses_.insert(address);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
