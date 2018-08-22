#pragma once

#include "public.h"

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/ref_counted.h>

#include <yt/core/net/address.h>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionStatistics
{
    TDuration IdleDuration;
    TDuration BusyDuration;
};

////////////////////////////////////////////////////////////////////////////////

struct IConnectionReader
    : public NConcurrency::IAsyncInputStream
{
    virtual TFuture<void> CloseRead() = 0;

    virtual TFuture<void> Abort() = 0;

    virtual int GetHandle() const = 0;

    virtual i64 GetReadByteCount() const = 0;

    virtual void SetReadDeadline(TInstant deadline) = 0;

    virtual TConnectionStatistics GetReadStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnectionReader)

////////////////////////////////////////////////////////////////////////////////

struct IConnectionWriter
    : public NConcurrency::IAsyncOutputStream
{
    virtual TFuture<void> WriteV(const TSharedRefArray& data) = 0;

    virtual TFuture<void> CloseWrite() = 0;

    virtual TFuture<void> Abort() = 0;

    virtual int GetHandle() const = 0;

    virtual i64 GetWriteByteCount() const = 0;

    virtual void SetWriteDeadline(TInstant deadline) = 0;

    virtual TConnectionStatistics GetWriteStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnectionWriter)

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public IConnectionReader
    , public IConnectionWriter
{
    virtual const TNetworkAddress& LocalAddress() const = 0;
    virtual const TNetworkAddress& RemoteAddress() const = 0;

    // Returns true if connection is not is failed state and has no
    // active IO operations.
    virtual bool IsIdle() const = 0;

    virtual bool SetNoDelay() = 0;
    virtual bool SetKeepAlive() = 0;

    virtual TFuture<void> Abort() override = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

std::pair<IConnectionPtr, IConnectionPtr> CreateConnectionPair(const NConcurrency::IPollerPtr& poller);

//! File descriptor must be in nonblocking mode.
IConnectionPtr CreateConnectionFromFD(
    int fd,
    const TNetworkAddress& localAddress,
    const TNetworkAddress& remoteAddress,
    const NConcurrency::IPollerPtr& poller);

IConnectionReaderPtr CreateInputConnectionFromPath(
    const TString& pipePath,
    const NConcurrency::IPollerPtr& poller,
    const TIntrusivePtr<TRefCounted>& pipeHolder);

IConnectionWriterPtr CreateOutputConnectionFromPath(
    const TString& pipePath,
    const NConcurrency::IPollerPtr& poller,
    const TIntrusivePtr<TRefCounted>& pipeHolder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
