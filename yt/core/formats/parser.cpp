#include "stdafx.h"
#include "parser.h"

#include <core/yson/consumer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

static const size_t ParseBufferSize = 1 << 16;

void Parse(TInputStream* input, IParser* parser)
{
    char buffer[ParseBufferSize];
    while (true) {
        size_t bytesRead = input->Read(buffer, ParseBufferSize);
        if (bytesRead == 0) {
            break;
        }
        parser->Read(TStringBuf(buffer, bytesRead));
    }
    parser->Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
