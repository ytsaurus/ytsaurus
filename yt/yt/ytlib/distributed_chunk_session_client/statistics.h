#pragma once

#include "public.h"

#include <library/cpp/yt/string/format.h>

#include <util/generic/strbuf.h>

#include <util/system/types.h>

#include <iosfwd>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedChunkSessionWriteStatistics
{
    i64 DataWeight = 0;
    i64 UncompressedDataSize = 0;
    i64 RowCount = 0;

    bool operator==(const TDistributedChunkSessionWriteStatistics&) const = default;
};

struct TDistributedChunkSessionProgress
{
    i64 DataWeight = 0;
    i64 CompressedDataSize = 0;
    i64 UncompressedDataSize = 0;
    i64 RecordCount = 0;
    i64 RowCount = 0;

    bool operator==(const TDistributedChunkSessionProgress&) const = default;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TDistributedChunkSessionProgress& progress,
    TStringBuf spec);

void PrintTo(
    const TDistributedChunkSessionProgress& progress,
    std::ostream* os);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TWriteRecordStatistics* protoStatistics,
    const TDistributedChunkSessionWriteStatistics& statistics);

void FromProto(
    TDistributedChunkSessionWriteStatistics* statistics,
    const NProto::TWriteRecordStatistics& protoStatistics);

void ToProto(
    NProto::TSessionProgress* protoProgress,
    const TDistributedChunkSessionProgress& progress);

void FromProto(
    TDistributedChunkSessionProgress* progress,
    const NProto::TSessionProgress& protoProgress);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
