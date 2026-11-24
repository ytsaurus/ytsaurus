#pragma once

#include "attributes.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/concurrency/nonblocking_queue.h>
#include <yt/yt/core/logging/log.h>

#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb-cpp-sdk/client/topic/deferred_publications.h>

#include <map>
#include <memory>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYdbMessageToWrite
{
    i64 SeqNo;
    std::string Data;
    TYdbMessageMeta MessageMeta;
    std::optional<NYdb::NTopic::TDeferredPublication> DeferredPublication;
};

struct IYdbWriter
    : public TRefCounted
{
    virtual TFuture<void> Write(TYdbMessageToWrite message) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYdbWriter);

class TRetryableYdbWriter
    : public IYdbWriter
{
public:
    using TMessageToWrite = TYdbMessageToWrite;

    TRetryableYdbWriter(
        std::shared_ptr<NYdb::NTopic::TTopicClient> topicClient,
        NYdb::NTopic::TWriteSessionSettings settings,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler);

    TFuture<void> Write(TMessageToWrite message) override;

    void Run();

private:
    const std::shared_ptr<NYdb::NTopic::TTopicClient> TopicClient_;
    const NYdb::NTopic::TWriteSessionSettings Settings_;
    const NLogging::TLogger Logger;
    const IStatusErrorStatePtr ErrorState_;

    NConcurrency::TNonblockingQueue<TMessageToWrite> Queue_;
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    THashMap<i64, TPromise<void>> Promises_;

private:
    NYdb::NTopic::TWriteMessage MakeWriteMessage(const TMessageToWrite& message) const;

    void CompleteWrite(
        i64 seqNo,
        TError error,
        std::map<i64, TMessageToWrite>* inflight);

    void FailDeferredInflightWrites(
        const TError& error,
        std::map<i64, TMessageToWrite>* inflight);

protected:
    void ProcessEvent(
        std::optional<NYdb::NTopic::TWriteSessionEvent::TEvent> event,
        std::optional<NYdb::NTopic::TContinuationToken>* token,
        std::map<i64, TMessageToWrite>* inflight);
};

DEFINE_REFCOUNTED_TYPE(TRetryableYdbWriter);

////////////////////////////////////////////////////////////////////////////////

IYdbWriterPtr CreateYdbWriter(
    IResourcePtr resource,
    std::shared_ptr<NYdb::NTopic::TTopicClient> topicClient,
    NYdb::NTopic::TWriteSessionSettings settings,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    IStatusProfilerPtr statusProfiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
