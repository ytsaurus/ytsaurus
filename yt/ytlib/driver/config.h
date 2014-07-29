#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/rpc/retrying_channel.h>

#include <ytlib/hydra/config.h>

#include <ytlib/transaction_client/config.h>

#include <ytlib/table_client/config.h>

#include <ytlib/new_table_client/config.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/hive/config.h>

#include <ytlib/api/config.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TDriverConfig
    : public NApi::TConnectionConfig
{
public:
    NApi::TFileReaderConfigPtr FileReader;
    NApi::TFileWriterConfigPtr FileWriter;
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NVersionedTableClient::TChunkWriterConfigPtr NewTableWriter; // TODO(babenko): merge with the above
    NApi::TJournalReaderConfigPtr JournalReader;
    NApi::TJournalWriterConfigPtr JournalWriter;
    bool ReadFromFollowers;
    i64 ReadBufferSize;
    i64 WriteBufferSize;
    int HeavyPoolSize;

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
        RegisterParameter("new_table_writer", NewTableWriter)
            .DefaultNew();
        RegisterParameter("read_from_followers", ReadFromFollowers)
            .Describe("Enable read-only requests to followers")
            .Default(false);
        RegisterParameter("read_buffer_size", ReadBufferSize)
            .Default((i64) 1 * 1024 * 1024);
        RegisterParameter("write_buffer_size", WriteBufferSize)
            .Default((i64) 1 * 1024 * 1024);
        RegisterParameter("heavy_pool_size", HeavyPoolSize)
            .Describe("Number of threads handling heavy requests")
            .Default(4);
    }
};

DEFINE_REFCOUNTED_TYPE(TDriverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

