#include "compression_helpers.h"

#include <library/cpp/streams/zstd/zstd.h>
#include <library/cpp/yt/logging/logger.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TString DoCompress(const TString& data, int quality = 9)
{
    TString compressed;
    TStringOutput output(compressed);
    TZstdCompress compressStream(&output, quality);
    compressStream.Write(data.data(), data.size());
    compressStream.Finish();
    output.Finish();
    return compressed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

const TString DefaultCompressedValue = DoCompress(TString("{}"));

////////////////////////////////////////////////////////////////////////////////

TString Compress(const TString& data, std::optional<ui64> maxCompressedStringSize, int quality)
{
    auto compressedValue = DoCompress(data, quality);
    return maxCompressedStringSize.has_value() && compressedValue.size() > maxCompressedStringSize.value() ? DefaultCompressedValue : compressedValue;
}

TString Decompress(const TString& data)
{
    TStringInput input(data);
    TZstdDecompress decompressStream(&input);
    auto res = decompressStream.ReadAll();
    return res;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
