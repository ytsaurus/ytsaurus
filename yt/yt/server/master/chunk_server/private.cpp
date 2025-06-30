#include "private.h"

namespace NYT::NChunkServer {

using namespace NChunkClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ChunkServerLogger;

///////////////////////////////////////////////////////////////////////////////

TStringBuf SerializeChunkFormatAsTableChunkFormat(EChunkFormat chunkFormat)
{
    switch (chunkFormat) {
        case EChunkFormat::FileDefault:
            return TStringBuf("old");
        case EChunkFormat::TableUnversionedSchemaful:
            return TStringBuf("unversioned_schemaful");
        case EChunkFormat::TableUnversionedSchemalessHorizontal:
            return TStringBuf("unversioned_schemaless_horizontal");
        case EChunkFormat::TableUnversionedColumnar:
            return TStringBuf("unversioned_columnar");
        case EChunkFormat::TableVersionedSimple:
            return TStringBuf("versioned_simple");
        case EChunkFormat::TableVersionedColumnar:
            return TStringBuf("versioned_columnar");
        case EChunkFormat::TableVersionedIndexed:
            return TStringBuf("versioned_indexed");
        case EChunkFormat::TableVersionedSlim:
            return TStringBuf("versioned_slim");
        case EChunkFormat::JournalDefault:
            return TStringBuf("journal_default");
        case EChunkFormat::HunkDefault:
            return TStringBuf("hunk_default");
        case EChunkFormat::Unknown:
            return TStringBuf("unknown");
        default:
            YT_LOG_ALERT("Unexpected chunk format encountered while formatting as table chunk format (ChunkFormat: %v)",
                chunkFormat);
            return TStringBuf("unexpected");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
