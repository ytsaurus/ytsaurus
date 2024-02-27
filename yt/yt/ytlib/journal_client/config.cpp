#include "config.h"

#include "helpers.h"

#include <yt/yt/library/erasure/impl/codec.h>

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

void TJournalHunkChunkWriterOptions::Register(TRegistrar registrar)
{
    registrar.Postprocessor([&] (TJournalHunkChunkWriterOptions* options) {
        if (options->ErasureCodec != NErasure::ECodec::None &&
            !NErasure::GetCodec(options->ErasureCodec)->IsBytewise())
        {
            THROW_ERROR_EXCEPTION("Only bytewise erasure codecs can be used, actual: %Qlv",
                options->ErasureCodec);
        }

        NJournalClient::ValidateJournalAttributes(
            options->ErasureCodec,
            options->ReplicationFactor,
            options->ReadQuorum,
            options->WriteQuorum);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
