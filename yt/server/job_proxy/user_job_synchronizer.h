#pragma once

#include "public.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/bus/public.h>

#include <yt/core/logging/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

//! Represents the "client side", where satellite can send notifications.
struct IUserJobSynchronizerClient
    : public virtual TRefCounted
{
    virtual void NotifyJobSatellitePrepared(const TErrorOr<i64>& rssOrError) = 0;
    virtual void NotifyUserJobFinished(const TError& error) = 0;
    virtual void NotifyExecutorPrepared() = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobSynchronizerClient)

////////////////////////////////////////////////////////////////////////////////

//! Represents the "server side", where we can wait for client.
struct IUserJobSynchronizer
    :  public virtual TRefCounted
{
    virtual void Wait() = 0;
    virtual TError GetUserProcessStatus() const = 0;
    virtual void CancelWait() = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobSynchronizer)

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizer
    : public IUserJobSynchronizerClient
    , public IUserJobSynchronizer
{
public:
    virtual void NotifyJobSatellitePrepared(const TErrorOr<i64>& rssOrError) override;
    virtual void NotifyExecutorPrepared() override;
    virtual void NotifyUserJobFinished(const TError& error) override;
    virtual void Wait() override;
    virtual TError GetUserProcessStatus() const override;
    virtual void CancelWait() override;
    i64 GetJobSatelliteRssUsage() const;

private:
    TPromise<i64> JobSatellitePreparedPromise_ = NewPromise<i64>();
    TPromise<void> UserJobFinishedPromise_ = NewPromise<void>();
    TPromise<void> ExecutorPreparedPromise_ = NewPromise<void>();
    i64 JobSatelliteRssUsage_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TUserJobSynchronizer)

////////////////////////////////////////////////////////////////////////////////

IUserJobSynchronizerClientPtr CreateUserJobSynchronizerClient(NBus::TTcpBusClientConfigPtr config);
NRpc::IServicePtr CreateUserJobSynchronizerService(
    const NLogging::TLogger& logger,
    IUserJobSynchronizerClientPtr jobControl,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
