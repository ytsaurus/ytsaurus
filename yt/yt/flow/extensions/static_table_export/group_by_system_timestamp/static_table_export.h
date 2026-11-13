#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/sink.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/core/compression/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TStaticTableExportParameters
    : public ISink::TParameters
{
    NYPath::TRichYPath OutputDirectory;
    TDuration WindowPeriod;
    TDuration ClosureDelta;
    NCompression::ECodec CompressionCodec{};
    NTableClient::EOptimizeFor OptimizeFor{};
    //! The band of unpublished windows writers may append to, counted from the last published one.
    int MaxUnpublishedWindowCount{};
    TResourceId WriterResourceId;
    //! Offset from UTC of the civil time in table names, like "+03:00".
    std::string TableNameUtcOffset;
    i64 TableNameUtcOffsetSeconds = 0;

    REGISTER_YSON_STRUCT(TStaticTableExportParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStaticTableExportParameters
    : public ISink::TDynamicParameters
{
    TDuration PublicationPeriod;
    TDuration RetryBackoff;
    std::optional<TDuration> TableTtl;

    REGISTER_YSON_STRUCT(TDynamicStaticTableExportParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicStaticTableExportParameters);

////////////////////////////////////////////////////////////////////////////////

struct TStaticTableExportWriterParameters
    : public NYTree::TYsonStruct
{
    //! Raw source payloads held from delivery until acknowledgement.
    i64 MaxRawDataWeight{};
    //! Parsed rows held from parsing until the native write accepts them.
    i64 MaxParsedDataWeight{};
    //! Raw size of admitted records not yet written; admission waits above it.
    //! A closing stream writes this backlog within #CommitTimeout, so it only needs to keep parsing busy.
    i64 MaxUnwrittenDataWeight{};
    TDuration IdleFlushPeriod;
    TDuration MaxStreamAge;
    TDuration TransactionTimeout;
    TDuration CommitTimeout;
    TDuration IOTimeout;
    TDuration ProgressRefreshPeriod;
    TDuration RetryBackoff;
    NTableClient::TTableWriterConfigPtr TableWriterConfig;
    //! Maximum number of table writers that append concurrently in the transaction of one stream.
    int TableWriterCount{};

    REGISTER_YSON_STRUCT(TStaticTableExportWriterParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportWriterParameters);

////////////////////////////////////////////////////////////////////////////////

//! Start of the window that contains |timestamp|, in seconds.
ui64 GetStaticTableExportWindow(TSystemTimestamp timestamp, TDuration period);

//! The window by system timestamp the next message of a partition goes to, or null when it is beyond the band.
std::optional<ui64> AssignStaticTableExportWindow(
    TSystemTimestamp writeTimestamp,
    std::optional<ui64> highestWindow,
    ui64 closedThrough,
    const TStaticTableExportParametersPtr& parameters);

//! Whether every message that can still target |window| has an event time past its closure.
bool IsStaticTableExportWindowClosable(
    ui64 window,
    TSystemTimestamp eventWatermark,
    const TStaticTableExportParametersPtr& parameters);

//! Validates that watermark alignment binds before the window band does.
void ValidateStaticTableExportBand(const TStaticTableExportParametersPtr& parameters, TDuration driftBound);

//! The name of the table of the window starting at |windowStart|, in the civil time of the table name offset.
std::string GetStaticTableExportTableName(ui64 windowStart, i64 utcOffsetSeconds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
