#pragma once

#include "io_engine.h"

#include <library/cpp/yt/misc/enum_indexed_array.h>

#include <optional>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

class TFixedBinsHistogramBase
{
public:
    using TBins = std::vector<i64>;
    using TCounters = std::vector<i64>;

    explicit TFixedBinsHistogramBase(TBins bins);

    const TBins& GetBins() const;
    const TCounters& GetCounters() const;
    void RecordValue(i64 value, i64 count = 1);

protected:
    void Add(const TFixedBinsHistogramBase& other);

private:
    TBins BinValues_;
    TCounters Counters_;
};

struct THistogramSummary
{
    i64 TotalCount = 0;

    //! Quantiles.
    i64 P90 = 0;
    i64 P99 = 0;
    i64 P99_9 = 0;
    i64 P99_99 = 0;
    i64 Max = 0;
};

THistogramSummary ComputeHistogramSummary(const TFixedBinsHistogramBase& hist);

////////////////////////////////////////////////////////////////////////////////

class TRequestSizeHistogram
    : public TFixedBinsHistogramBase
{
public:
    TRequestSizeHistogram();

    TRequestSizeHistogram& operator+=(const TRequestSizeHistogram& other);
};

struct TRequestSizes
{
    // Request size distribution by workload category.
    TEnumIndexedArray<EWorkloadCategory, TRequestSizeHistogram> Reads;
    TEnumIndexedArray<EWorkloadCategory, TRequestSizeHistogram> Writes;

    // Modeling period duration
    TDuration Duration;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TEnumIndexedArray<EWorkloadCategory, TRequestSizeHistogram>& statsByCategory,
    TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

class TRequestLatencyHistogram
    : public TFixedBinsHistogramBase
{
public:
    TRequestLatencyHistogram();

    TRequestLatencyHistogram& operator+=(const TRequestLatencyHistogram& other);
};

struct TRequestLatencies
{
    TEnumIndexedArray<EWorkloadCategory, TRequestLatencyHistogram> Reads;
    TEnumIndexedArray<EWorkloadCategory, TRequestLatencyHistogram> Writes;

    // Measuring period duration.
    TDuration Duration;
};

////////////////////////////////////////////////////////////////////////////////

struct IIOEngineWorkloadModel
    : public IIOEngine
{
    virtual std::optional<TRequestSizes> GetRequestSizes() = 0;
    virtual std::optional<TRequestLatencies> GetRequestLatencies() = 0;
};

DEFINE_REFCOUNTED_TYPE(IIOEngineWorkloadModel)

IIOEngineWorkloadModelPtr CreateIOModelInterceptor(
    TString locationId,
    IIOEnginePtr underlying,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
