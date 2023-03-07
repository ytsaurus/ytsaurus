#pragma once

#include "public.h"

#include <yt/client/api/config.h>

#include <yt/client/chunk_client/config.h>

#include <yt/client/table_client/config.h>

#include <yt/client/transaction_client/config.h>

#include <yt/core/rpc/retrying_channel.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NDriver {

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
    NChunkClient::TFetcherConfigPtr Fetcher;
    int ApiVersion;

    i64 ReadBufferRowCount;
    i64 ReadBufferSize;
    i64 WriteBufferSize;
    bool EnablePingRetries;

    TSlruCacheConfigPtr ClientCache;

    std::optional<TString> Token;

    std::optional<bool> RewriteOperationPath;
    bool ForceTracing;

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
        RegisterParameter("fetcher", Fetcher)
            .DefaultNew();

        RegisterParameter("read_buffer_row_count", ReadBufferRowCount)
            .Default((i64) 10000);
        RegisterParameter("read_buffer_size", ReadBufferSize)
            .Default((i64) 1 * 1024 * 1024);
        RegisterParameter("write_buffer_size", WriteBufferSize)
            .Default((i64) 1 * 1024 * 1024);

        RegisterParameter("enable_ping_retries", EnablePingRetries)
            .Default(false);

        RegisterParameter("client_cache", ClientCache)
            .DefaultNew(1024 * 1024);

        RegisterParameter("api_version", ApiVersion)
            .Default(ApiVersion3)
            .GreaterThanOrEqual(ApiVersion3)
            .LessThanOrEqual(ApiVersion4);

        RegisterParameter("token", Token)
            .Optional();

        RegisterParameter("rewrite_operation_path", RewriteOperationPath)
            .Optional();

        RegisterPostprocessor([&] {
            if (ApiVersion != ApiVersion3 && ApiVersion != ApiVersion4) {
                THROW_ERROR_EXCEPTION("Unsupported API version %v",
                    ApiVersion);
            }
        });

        RegisterParameter("force_tracing", ForceTracing)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TDriverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver

