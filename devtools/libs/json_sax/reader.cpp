#include "reader.h"
#include "parser.h"

namespace NYa::NJson {
    bool ReadJson(IInputStream& stream, TJsonCallbacks* h, size_t initBufferSize) {
        return TParserCtx(*h, initBufferSize).Parse(stream);
    }
}
