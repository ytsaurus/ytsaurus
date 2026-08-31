#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/error/error.h>

#include <library/cpp/yt/yson/public.h>

#include <library/cpp/yt/string/format.h>

#include <library/cpp/yt/misc/strong_typedef.h>

#include <util/generic/strbuf.h>

#include <util/system/types.h>

#include <iosfwd>
#include <optional>
#include <utility>
#include <variant>

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

    PHOENIX_DECLARE_TYPE(TDistributedChunkSessionProgress, 0x4be2fca1);
};

struct TSessionSealSummary
{
    //! Exact terminal journal record count learned after master sealing.
    i64 RecordCount = 0;
    //! Physical changelog size reported by master. Includes journal framing and
    //! padding, so it must not be read as a logical compressed size.
    i64 PhysicalCompressedDataSize = 0;

    bool operator==(const TSessionSealSummary&) const = default;
};

////////////////////////////////////////////////////////////////////////////////

bool IsNonnegative(const TDistributedChunkSessionProgress& progress);

bool IsNonnegative(const TSessionSealSummary& summary);

void VerifyNonnegative(const TDistributedChunkSessionProgress& progress);

//! Every record carries at least one row, and a row weighs at least one unit of every
//! measure, so no component ever drops below the record count. Splitting and
//! extrapolation preserve this, which lets uncompressed data size be used directly as a
//! strictly positive job-size weight.
void VerifyAtLeastOneUnitPerRecord(const TDistributedChunkSessionProgress& progress);

bool IsComponentwiseLessOrEqual(
    const TDistributedChunkSessionProgress& lhs,
    const TDistributedChunkSessionProgress& rhs);

////////////////////////////////////////////////////////////////////////////////

TDistributedChunkSessionProgress operator+(
    const TDistributedChunkSessionProgress& lhs,
    const TDistributedChunkSessionProgress& rhs);

TDistributedChunkSessionProgress& operator+=(
    TDistributedChunkSessionProgress& lhs,
    const TDistributedChunkSessionProgress& rhs);

TDistributedChunkSessionProgress operator-(
    const TDistributedChunkSessionProgress& lhs,
    const TDistributedChunkSessionProgress& rhs);

//! Requires nonnegative |progress| with |progress.RecordCount| > 0,
//! |prefixRecordCount| in [0, |progress.RecordCount|], and nonempty records.
//! Both halves keep nonempty records and sum to |progress|.
std::pair<TDistributedChunkSessionProgress, TDistributedChunkSessionProgress> Split(
    const TDistributedChunkSessionProgress& progress,
    i64 prefixRecordCount);

//! Requires nonnegative |sample| with |sample.RecordCount| > 0 and nonempty records,
//! |recordCount| nonnegative, and |compressedDataSize| at least |recordCount|.
//! Compressed data size is taken verbatim rather than extrapolated, so that the seal
//! total is reproduced exactly; the remaining components keep nonempty records.
TDistributedChunkSessionProgress Extrapolate(
    const TDistributedChunkSessionProgress& sample,
    i64 recordCount,
    i64 compressedDataSize);

////////////////////////////////////////////////////////////////////////////////

//! Cumulative quorum-confirmed progress while the session is open.
YT_DEFINE_STRONG_TYPEDEF(TSessionInFlightProgress, TDistributedChunkSessionProgress);

//! Raised once a session closes cleanly; empty when the sequencer reported no progress.
// COMPAT(apollo1321): Only a pre-26.2 sequencer leaves this empty.
YT_DEFINE_STRONG_TYPEDEF(
    TSessionFinalProgress,
    std::optional<TDistributedChunkSessionProgress>);

//! Graceful close failed, so no final progress will follow.
YT_DEFINE_STRONG_TYPEDEF(TSessionCloseFailed, TError);

//! Progress a controller reports for its own session. Exactly one of the terminal
//! alternatives is raised once the session ends.
using TControllerSessionProgress = std::variant<
    TSessionInFlightProgress,
    TSessionFinalProgress,
    TSessionCloseFailed>;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSessionSealSummary& summary,
    TStringBuf spec);

void FormatValue(
    TStringBuilderBase* builder,
    const TDistributedChunkSessionProgress& progress,
    TStringBuf spec);

void PrintTo(
    const TDistributedChunkSessionProgress& progress,
    std::ostream* os);

void PrintTo(
    const TSessionSealSummary& summary,
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

void Serialize(
    const TDistributedChunkSessionProgress& progress,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
