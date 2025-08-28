#pragma once

#include <library/cpp/json/common/defs.h>

#include <util/stream/input.h>

namespace NYa::NJson {
    bool ReadJson(IInputStream& stream, ::NJson::TJsonCallbacks* h, size_t initBufferSize = (1 << 16));
}
