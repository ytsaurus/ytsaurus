#include "stdafx.h"
#include "config.h"

#include <ytlib/transaction_client/config.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/hive/config.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/new_table_client/config.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TMasterConnectionConfig::TMasterConnectionConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TConnectionConfig::TConnectionConfig()
{
    RegisterParameter("master", Master);
    RegisterParameter("timestamp_provider", TimestampProvider)
        .Default(nullptr);
    RegisterParameter("master_cache", MasterCache)
        .Default(nullptr);
    RegisterParameter("cell_directory", CellDirectory)
        .DefaultNew();
    RegisterParameter("scheduler", Scheduler)
        .DefaultNew();
    RegisterParameter("transaction_manager", TransactionManager)
        .DefaultNew();
    RegisterParameter("compressed_block_cache", CompressedBlockCache)
        .DefaultNew();
    RegisterParameter("uncompressed_block_cache", UncompressedBlockCache)
        .DefaultNew();
    RegisterParameter("table_mount_cache", TableMountCache)
        .DefaultNew();

    RegisterParameter("query_timeout", QueryTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("write_request_codec", WriteRequestCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("lookup_request_codec", LookupRequestCodec)
        .Default(NCompression::ECodec::Lz4);

    RegisterParameter("max_rows_per_read", MaxRowsPerRead)
        .GreaterThan(0)
        .Default(1000);
    RegisterParameter("max_rows_per_write", MaxRowsPerWrite)
        .GreaterThan(0)
        .Default(1000);

    RegisterParameter("max_rows_per_transaction", MaxRowsPerTransaction)
        .GreaterThan(0)
        .Default(100000);

    RegisterParameter("default_input_row_limit", DefaultInputRowLimit)
        .GreaterThan(0)
        .Default(100000000);
    RegisterParameter("default_output_row_limit", DefaultOutputRowLimit)
        .GreaterThan(0)
        .Default(1000000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

