#include "request_session.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TServerAddressPool::TServerAddressPool(
    TDuration banTimeout,
    const NLogging::TLogger& logger,
    const std::vector<TString>& addresses)
    : Logger(logger)
    , BanTimeout_(banTimeout)
    , UpAddresses_(addresses.begin(), addresses.end())
{ }

std::vector<TString> TServerAddressPool::GetUpAddresses()
{
    auto guard = Guard(Lock_);
    return {UpAddresses_.begin(), UpAddresses_.end()};
}

std::vector<TString> TServerAddressPool::GetProbationAddresses()
{
    auto guard = Guard(Lock_);
    return {ProbationAddresses_.begin(), ProbationAddresses_.end()};
}

void TServerAddressPool::BanAddress(const TString& address)
{
    {
        auto guard = Guard(Lock_);
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
    auto guard = Guard(Lock_);

    auto it = ProbationAddresses_.find(address);
    if (it == ProbationAddresses_.end()) {
        return;
    }
    YT_LOG_DEBUG("Server unbanned (Address: %v)", address);
    ProbationAddresses_.erase(it);
    UpAddresses_.insert(address);
}

void TServerAddressPool::SetBanTimeout(TDuration banTimeout)
{
    BanTimeout_ = std::move(banTimeout);
}

void TServerAddressPool::SetAddresses(const std::vector<TString>& addresses)
{
    auto guard = Guard(Lock_);

    YT_LOG_INFO("Address list changed (Addresses: %v)", addresses);

    ProbationAddresses_.clear();
    DownAddresses_.clear();
    UpAddresses_ = {addresses.begin(), addresses.end()};
}

void TServerAddressPool::OnBanTimeoutExpired(const TString& address)
{
    auto guard = Guard(Lock_);

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
