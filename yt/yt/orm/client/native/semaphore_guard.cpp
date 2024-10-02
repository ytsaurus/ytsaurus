#include "semaphore_guard.h"

#include "client.h"
#include "helpers.h"

#include <yt/yt/orm/client/objects/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/retry/retry.h>

namespace NYT::NOrm::NClient::NNative {

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSemaphoreSetGuard::TSemaphoreSetGuard(
    IClientPtr client,
    TString semaphoreSetId,
    TSemaphoreGuardOptions options)
    : Client_(std::move(client))
    , SemaphoreSetId_(std::move(semaphoreSetId))
    , LeaseUuid_(options.AcquireOptions().LeaseUuid
        ? *options.AcquireOptions().LeaseUuid
        : NObjects::GenerateUuid())
    , Options_(std::move(options))
{
    if (Options_.Invoker()) {
        Invoker_ = CreateSerializedInvoker(Options_.Invoker());
    } else {
        ActionQueue_ = New<TActionQueue>("SemPing");
        Invoker_ = ActionQueue_->GetInvoker();
    }
    Acquire();
}

TSemaphoreSetGuard::~TSemaphoreSetGuard()
{
    try {
        Release();
    } catch (...) {
        // ¯\_(ツ)_/¯
    }
}

TString TSemaphoreSetGuard::GetLeaseUuid() const
{
    return LeaseUuid_;
}

void TSemaphoreSetGuard::Acquire()
{
    DoWithRetry<TErrorException>(
        [&, this] {
            WaitFor(Client_->UpdateObject(
                SemaphoreSetId_,
                NObjects::TObjectTypeValues::SemaphoreSet,
                std::vector<TUpdate>{TSetUpdate{
                    "/control/acquire",
                    BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                        NYTree::BuildYsonFluently(consumer)
                            .BeginMap()
                                .Item("lease_uuid").Value(GetLeaseUuid())
                                .Item("duration").Value(Options_.AcquireOptions().LeaseDuration)
                                .Item("budget").Value(Options_.AcquireOptions().LeaseBudget)
                            .EndMap();
                    }))}}))
                .ValueOrThrow();
        },
        Options_.AcquireOptions().RetryOptions);
    IsAcquired_.store(true);

    if (auto pingOptions = Options_.PingOptions()) {
        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Invoker_,
            BIND([&self = *this, retryOptions = pingOptions->RetryOptions] {
                try {
                    DoWithRetry<TErrorException>(
                        [&self] {
                            WaitFor(self.Client_->UpdateObject(
                                self.SemaphoreSetId_,
                                NObjects::TObjectTypeValues::SemaphoreSet,
                                std::vector<TUpdate>{TSetUpdate{
                                    "/control/ping",
                                    BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                                        NYTree::BuildYsonFluently(consumer)
                                            .BeginMap()
                                                .Item("lease_uuid").Value(self.GetLeaseUuid())
                                                .Item("duration").Value(self.Options_.AcquireOptions().LeaseDuration)
                                            .EndMap();
                                    }))}}))
                                .ValueOrThrow();
                        },
                        retryOptions);
                } catch (const TErrorException&) {
                    self.IsAcquired_.store(false);
                }
            }),
            pingOptions->Period);
        PeriodicExecutor_->Start();
    }
}

void TSemaphoreSetGuard::Release()
{
    if (IsReleased_) {
        return;
    }
    IsReleased_ = true;
    if (Options_.PingOptions()) {
        WaitForUnique(PeriodicExecutor_->Stop())
            .ThrowOnError();
    }

    DoWithRetry<TErrorException>(
        [&, this] {
            WaitFor(Client_->UpdateObject(
                SemaphoreSetId_,
                NObjects::TObjectTypeValues::SemaphoreSet,
                std::vector<TUpdate>{TSetUpdate{
                    "/control/release",
                    BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
                        NYTree::BuildYsonFluently(consumer)
                            .BeginMap()
                                .Item("lease_uuid").Value(GetLeaseUuid())
                            .EndMap();
                    }))}}))
                .ValueOrThrow();
        },
        Options_.ReleaseOptions().RetryOptions,
        /*throwOnLast*/ true);
    IsAcquired_.store(false);
}

bool TSemaphoreSetGuard::IsAcquired() const
{
    return IsAcquired_.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
