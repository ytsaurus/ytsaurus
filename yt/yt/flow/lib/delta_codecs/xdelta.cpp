#include "xdelta.h"

#include <library/cpp/yt/error/error.h>
#include <library/cpp/yt/memory/blob.h>

#include <contrib/libs/xdelta3/include/xdelta3.h>

#include <util/system/byteorder.h>

namespace NYT::NFlow::NDeltaCodecs {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

int ProcessMemory(int is_encode,
    int (*func)(xd3_stream*),
    const uint8_t* input, usize_t input_size,
    const uint8_t* source, usize_t source_size,
    uint8_t* output, usize_t* output_size,
    usize_t output_size_max, usize_t iopt_size,
    const char** error_message)
{
    xd3_stream stream;
    xd3_config config;
    xd3_source src;
    int ret;

    memset(&stream, 0, sizeof(stream));
    xd3_init_config(&config, XD3_NOCOMPRESS);

    if (is_encode) {
        config.winsize = input_size;
        config.iopt_size = iopt_size;
        config.sprevsz = xd3_pow2_roundup(config.winsize);
    }

    if ((ret = xd3_config_stream(&stream, &config)) == 0) {
        if (source != nullptr || source_size == 0) {
            memset(&src, 0, sizeof(src));

            src.blksize = source_size;
            src.onblk = source_size;
            src.curblk = source;
            src.curblkno = 0;
            src.max_winsize = source_size;

            if ((ret = xd3_set_source_and_size(&stream, &src, source_size)) == 0) {
                ret = xd3_process_stream(is_encode,
                    &stream,
                    func,
                    1,
                    input,
                    input_size,
                    output,
                    output_size,
                    output_size_max);
                if (ret) {
                    *error_message = stream.msg;
                }
            } else {
                *error_message = stream.msg;
            }
        }
    } else {
        *error_message = stream.msg;
    }
    xd3_free_stream(&stream);
    return ret;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TSharedRef TXDeltaCodec::ApplyPatch(const TSharedRef& base, const TSharedRef& patch) const
{
    if (patch.ToStringBuf().empty()) {
        return base;
    }

    TRecordSize size = 0;
    Y_ABORT_UNLESS(patch.size() >= sizeof(size));
    memcpy(&size, patch.data(), sizeof(size));
    size = InetToHost(size);

    if (size == 0) {
        return TSharedRef::MakeEmpty();
    }

    TBlob buffer;
    buffer.Resize(size);
    const char* errorMessage = nullptr;
    size_t decodedSize = 0;
    int ret = ProcessMemory(false,
        &xd3_decode_input,
        reinterpret_cast<const uint8_t*>(patch.data()) + sizeof(size),
        patch.size() - sizeof(size),
        reinterpret_cast<const uint8_t*>(base.data()),
        base.size(),
        reinterpret_cast<uint8_t*>(buffer.Begin()),
        &decodedSize,
        size,
        100,
        &errorMessage);
    if (ret != 0) {
        THROW_ERROR_EXCEPTION("Failed to decode by XDelta (Code: %v(%v), Message: %v)",
            ret,
            xd3_strerror(ret),
            errorMessage ? errorMessage : "");
    }
    if (decodedSize != size) {
        THROW_ERROR_EXCEPTION("Decoded size mismatch: %v != %v",
            decodedSize,
            size);
    }
    return TSharedRef::FromBlob(std::move(buffer));
}

std::optional<TSharedRef> TXDeltaCodec::TryComputePatch(const TSharedRef& base, const TSharedRef& value) const
{
    if (base.ToStringBuf() == value.ToStringBuf()) {
        return TSharedRef::MakeEmpty();
    }
    TBlob buffer;
    TRecordSize patchSize = std::max(value.size(), base.size());
    buffer.Resize(sizeof(patchSize) + (patchSize < 50 ? 200 : patchSize) * 1.5); // NOTE: for small data N * 1.5 does not work e.g. data 10 & 10 -> patch 31
    TRecordSize size = HostToInet(static_cast<TRecordSize>(value.size()));
    memcpy(buffer.Begin(), &size, sizeof(size));

    const char* errorMessage = nullptr;
    usize_t outSize = 0;
    int ret = ProcessMemory(
        true,
        &xd3_encode_input,
        reinterpret_cast<const uint8_t*>(value.data()),
        value.size(),
        reinterpret_cast<const uint8_t*>(base.data()),
        base.size(),
        reinterpret_cast<uint8_t*>(buffer.Begin()) + sizeof(size),
        &outSize,
        buffer.Size() - sizeof(size),
        100,
        &errorMessage);

    if (ret != 0) {
        THROW_ERROR_EXCEPTION("Failed to encode by XDelta (Code: %v(%v), Message: %v)",
            ret,
            xd3_strerror(ret),
            errorMessage ? errorMessage : "");
    }

    buffer.Resize(outSize + sizeof(size));
    Y_ABORT_UNLESS(buffer.Size() >= sizeof(outSize), "Final size is not correct. Integer overflows is possible");

    return TSharedRef::FromBlob(std::move(buffer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDeltaCodecs
