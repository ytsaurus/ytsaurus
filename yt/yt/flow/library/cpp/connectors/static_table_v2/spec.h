#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/re2/public.h>

namespace NYT::NFlow::NStaticTableConnectorV2 {

////////////////////////////////////////////////////////////////////////////////

struct TTableTimestampLocatorSpec
    : public virtual NYTree::TYsonStruct
{
    std::string Attribute;
    ETimestampFormat Format{};

    REGISTER_YSON_STRUCT(TTableTimestampLocatorSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableTimestampLocatorSpec);

struct TTableSourceParameters
    : public TOrderedSourceBase::TParameters
{
    // `Tables` and `TablesPath` are alternative ways of determining which tables to read.

    // Rich paths with cluster and columns to read.
    std::optional<std::vector<NYPath::TRichYPath>> Tables;

    // Rich path to a directory with tables. Rich path contains cluster and columns to read.
    std::optional<NYPath::TRichYPath> TablesPath;

    // Read only tables whose name matches this regexp.
    NRe2::TRe2Ptr TableNameFilter;

    // New tables must be created with increasing (EventTimestamp, SystemTimestamp).
    // By default it is extracted from table name with iso8601 parser.
    TTableTimestampLocatorSpecPtr EventTimestampLocator;
    // The moment when new table become available for reading. Used for computing system time lag.
    // By default it is extracted from table creation_time attribute.
    TTableTimestampLocatorSpecPtr SystemTimestampLocator;

    // Ignore symlinks, so they do not process and count.
    bool IgnoreSymlinks = false;

    // Skip non-table nodes (e.g. map nodes) instead of throwing an error.
    bool SkipNonTableNodes = false;

    // Event time watermark is delayed until LatestNotProcessedTable.CreateTimestamp - WatermarkDelay
    // if there are some not processed tables.
    // Or until now - IdleWatermarkDelay if there is no not processed tables.
    TDuration WatermarkDelay;

    // Multi-cluster only: how long the active cluster must stay unavailable before
    // the current table fails over to another cluster.
    TDuration FailoverDelay;

    REGISTER_YSON_STRUCT(TTableSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableSourceParameters);

////////////////////////////////////////////////////////////////////////////////

// TODO: fix name. "dynamicTable" is so-so.
struct TDynamicTableSourceParameters
    : public TOrderedSourceBase::TDynamicParameters
{
    // All the table needs to be read in this time.
    TDuration DesiredTableProcessTime;

    // Limit total speed of processing for all partitions.
    double MaxRowsPerSecond{};
    double MaxBytesPerSecond{};

    // Min timestamp in seconds of table to process.
    std::optional<ui64> MinEventTimestamp;

    // TLDR: Set it to NOW to restart processing of all visible tables.
    // Ignore current processing progress if previous start/restart was earlier than RestartInstant.
    TInstant RestartInstant;

    // Do not override parameters below if you are not sure.

    NYTree::TSize MaxPartitionCount;

    // Period of throttler of partition reader.
    TDuration ThrottlerPeriod;

    // Compute partition size so that partition is fully read in this time.
    TDuration DesiredPartitionProcessTime;

    // Speed limits for one partition. Controller uses them for limiting partition size.
    double DesiredPartitionRowsPerSecond{};
    double DesiredPartitionBytesPerSecond{};

    // If the source returns empty response during this period, table reader is recreated.
    TDuration ReadTimeout;

    REGISTER_YSON_STRUCT(TDynamicTableSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableSourcePartitionSpec
    : public TOrderedSourceBase::TDynamicPartitionSpec
{
    // Rich path with cluster, lower/upper limits and columns to read.
    // Lower limit can be increased to upper during partition processing. It means forcing partition to become completed.
    NYPath::TRichYPath Table;

    TSystemTimestamp EventTimestamp;
    TSystemTimestamp SystemTimestamp;

    // Desired speed of reading. Can be updated by controller during work.
    double RowsPerSecond{};

    REGISTER_YSON_STRUCT(TDynamicTableSourcePartitionSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableSourcePartitionSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnectorV2
