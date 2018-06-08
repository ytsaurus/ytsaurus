#pragma once

#include "public.h"

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/hydra/config.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/transaction_client/config.h>

#include <yt/core/rpc/retrying_channel.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

constexpr int ApiVersion3 = 3;
constexpr int ApiVersion4 = 4;

class TDriverConfig
    : public NYTree::TYsonSerializable
{
public:
    NApi::TFileReaderConfigPtr FileReader;
    NApi::TFileWriterConfigPtr FileWriter;
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NApi::TJournalReaderConfigPtr JournalReader;
    NApi::TJournalWriterConfigPtr JournalWriter;
    int ApiVersion;

    i64 ReadBufferRowCount;
    i64 ReadBufferSize;
    i64 WriteBufferSize;

    TSlruCacheConfigPtr ClientCache;

    TDriverConfig()
    {
        RegisterParameter("file_reader", FileReader)
            .DefaultNew();
        RegisterParameter("file_writer", FileWriter)
            .DefaultNew();
        RegisterParameter("table_reader", TableReader)
            .DefaultNew();
        RegisterParameter("table_writer", TableWriter)
            .DefaultNew();
        RegisterParameter("journal_reader", JournalReader)
            .DefaultNew();
        RegisterParameter("journal_writer", JournalWriter)
            .DefaultNew();

        RegisterParameter("read_buffer_row_count", ReadBufferRowCount)
            .Default((i64) 10000);
        RegisterParameter("read_buffer_size", ReadBufferSize)
            .Default((i64) 1 * 1024 * 1024);
        RegisterParameter("write_buffer_size", WriteBufferSize)
            .Default((i64) 1 * 1024 * 1024);

        RegisterParameter("client_cache", ClientCache)
            .DefaultNew(1);

        RegisterParameter("api_version", ApiVersion)
            .Default(ApiVersion3);
    }
};

DEFINE_REFCOUNTED_TYPE(TDriverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

