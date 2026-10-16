#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

struct IInputMessages
    : public TRefCounted
{
    struct TMessage
    {
        TKey Key;
        TMessageId MessageId;
        TSystemTimestamp SystemTimestamp;
    };

    virtual TFuture<std::vector<bool>> Contains(
        const TComputationId& computationId,
        const std::vector<TMessage>& messages) = 0;

    virtual void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        const std::vector<TMessage>& messages) = 0;
};

DEFINE_REFCOUNTED_TYPE(IInputMessages);

////////////////////////////////////////////////////////////////////////////////

class TInputMessages
    : public IInputMessages
{
public:
    explicit TInputMessages(TContextPtr context);

    TFuture<std::vector<bool>> Contains(
        const TComputationId& computationId,
        const std::vector<TMessage>& messages) override;

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        const std::vector<TMessage>& messages) override;

private:
    struct TMetrics
    {
        NProfiling::TProfiler Profiler;
        NProfiling::TCounter LookupRows = Profiler.Counter("/lookup_rows");
        NProfiling::TCounter LookupBytes = Profiler.Counter("/lookup_bytes");
        NProfiling::TEventTimer LookupTime = Profiler.Timer("/lookup_time");
        NProfiling::TCounter WriteRows = Profiler.Counter("/write_rows");
        NProfiling::TCounter WriteBytes = Profiler.Counter("/write_bytes");
    };

    const TContextPtr Context_;
    const NYPath::TYPath TablePath_;
    const NLogging::TLogger Logger;
    const std::string Tag_;
    const TMetrics Metrics_;
};

DEFINE_REFCOUNTED_TYPE(TInputMessages);

////////////////////////////////////////////////////////////////////////////////

// Compact variant of TInputMessages. Uses a single string key column:
// deduplication_message_key = <computation_id><zero-byte><key[0] hash, 8 bytes, big-endian><CityHash128(message_id), 16 bytes>
// instead of three separate columns.
class TCompactInputMessages
    : public IInputMessages
{
public:
    explicit TCompactInputMessages(TContextPtr context);

    TFuture<std::vector<bool>> Contains(
        const TComputationId& computationId,
        const std::vector<TMessage>& messages) override;

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        const std::vector<TMessage>& messages) override;

private:
    struct TMetrics
    {
        NProfiling::TProfiler Profiler;
        NProfiling::TCounter LookupRows = Profiler.Counter("/lookup_rows");
        NProfiling::TCounter LookupBytes = Profiler.Counter("/lookup_bytes");
        NProfiling::TEventTimer LookupTime = Profiler.Timer("/lookup_time");
        NProfiling::TCounter WriteRows = Profiler.Counter("/write_rows");
        NProfiling::TCounter WriteBytes = Profiler.Counter("/write_bytes");
    };

    static void BuildDeduplicationKey(
        const TComputationId& computationId,
        const TMessage& message,
        TString& buffer);

    const TContextPtr Context_;
    const NYPath::TYPath TablePath_;
    const NLogging::TLogger Logger;
    const std::string Tag_;
    const TMetrics Metrics_;
};

DEFINE_REFCOUNTED_TYPE(TCompactInputMessages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
