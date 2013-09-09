#include "stdafx.h"
#include "parser.h"

#include <core/yson/consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

const size_t ParseChunkSize = 1 << 16;

void Parse(TInputStream* input, IParser* parser)
{
    char chunk[ParseChunkSize];
    while (true) {
        // Read a chunk.
        size_t bytesRead = input->Read(chunk, ParseChunkSize);
        if (bytesRead == 0) {
            break;
        }
        // Parse the chunk.
        parser->Read(TStringBuf(chunk, bytesRead));
    }
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
