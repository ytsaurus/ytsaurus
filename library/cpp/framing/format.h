#pragma once

namespace NFraming {
    enum class EFormat {
        Auto = 0,          // It is `protoseq` codec in packer and auto detect `lenval` or `protoseq` codec in unpacker. 
                           // Does not support detecting `LightProtoseq` codec.
        Protoseq = 1,      // Each frame followed by special sequence of bytes, which allows
                           // to restore part of chunks even message was partially corrupted.
        Lenval = 2,        // Lightweight version of codec, which has small size, but do not safe from partial corrupts (Recommended).
        LightProtoseq = 3, // Like `protoseq` codec, but without sync frame.
    };
}
