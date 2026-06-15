#include "config.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>

#include <yt/yt/client/api/config.h>

namespace NYT::NPushBasedShuffleClient {

using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

void TShuffleWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("memory_budget", &TThis::MemoryBudget)
        .GreaterThanOrEqual(1_MB)
        .Default(64_MB);
    registrar.Parameter("builders_budget_fraction", &TThis::BuildersBudgetFraction)
        .InRange(0.01, 0.99)
        .Default(0.1);
    registrar.Parameter("codec", &TThis::Codec)
        .Default(ECodec::None);
    registrar.Parameter("max_send_attempts", &TThis::MaxSendAttempts)
        .GreaterThan(0)
        .Default(3);
    registrar.Parameter("writer_config", &TThis::WriterConfig)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("chunk_session_reader_config", &TThis::ChunkSessionReaderConfig)
        .DefaultNew();
    registrar.Parameter("codec", &TThis::Codec)
        .Default(ECodec::None);
    registrar.Parameter("row_buffer_start_chunk_size", &TThis::RowBufferStartChunkSize)
        .Default(64_KB)
        .GreaterThan(0);
    registrar.Parameter("max_bytes_per_read", &TThis::MaxBytesPerRead)
        .Default(64_MB)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

void TPushShuffleConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("writer_config", &TThis::WriterConfig)
        .DefaultNew();
    registrar.Parameter("reader_config", &TThis::ReaderConfig)
        .DefaultNew();
    registrar.Parameter("journal_writer_config", &TThis::JournalWriterConfig)
        .DefaultNew();
    registrar.Parameter("session_pool_config", &TThis::SessionPoolConfig)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
