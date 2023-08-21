#include "config.h"

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

void TJournalHunkChunkWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_record_size", &TThis::MaxRecordSize)
        .Default(16_KB);
    registrar.Parameter("max_record_hunk_count", &TThis::MaxRecordHunkCount)
        .Default(100'000);
}

////////////////////////////////////////////////////////////////////////////////

void TJournalHunkChunkWriterOptions::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
