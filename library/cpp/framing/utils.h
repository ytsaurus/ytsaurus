#pragma once

#include "format.h"
#include <util/system/types.h>

namespace NFraming {
    namespace NPrivate {
        inline constexpr size_t PROTOSEQ_FRAME_LEN_BYTES = sizeof(ui32);

        size_t GetProtoseqFrameSize(size_t dataSize);

        size_t GetLenvalFrameSize(size_t dataSize);

        size_t GetLightProtoseqFrameSize(size_t dataSize);
    }

    //@brief return size of whole frame depends on format
    inline size_t GetFrameSize(EFormat format, size_t dataSize) {
        switch (format) {
            case EFormat::Auto:
            case EFormat::Protoseq:
                return NPrivate::GetProtoseqFrameSize(dataSize);
            case EFormat::Lenval:
                return NPrivate::GetLenvalFrameSize(dataSize);
            case EFormat::LightProtoseq:
                return NPrivate::GetLightProtoseqFrameSize(dataSize);
        }
    }
}
