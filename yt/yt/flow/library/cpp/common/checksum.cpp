#include "checksum.h"

#include <library/cpp/yt/string/format.h>

#include <util/digest/city.h>

#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/execpath.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString BinaryChecksumOverrideEnvVar = "YT_FLOW_BINARY_CHECKSUM_OVERRIDE";

std::string Hash128ToStr(const uint128& hash)
{
    return Format("%v_%v", hash.first, hash.second);
}

std::string CalculateBinaryChecksum()
{
    constexpr size_t ChunkSize = 64 * 1024;

    const std::string& binaryPath = ::GetPersistentExecPath();
    auto file = ::TFileInput(TString(binaryPath));

    char buf[ChunkSize];
    size_t read = file.Read(buf, ChunkSize);
    uint128 hash = CityHash128(buf, read);
    while ((read = file.Read(buf, ChunkSize)) > 0) {
        hash = CityHash128WithSeed(buf, read, hash);
    }
    return Hash128ToStr(hash);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

const std::string& GetBinaryChecksum()
{
    static const std::string BinaryChecksum = [] {
        // Hashing the binary reads all of it — gigabytes in a debug or sanitizer build. Tests
        // that do not check the checksum itself force a value rather than pay for that read.
        if (auto forced = GetEnv(BinaryChecksumOverrideEnvVar); !forced.empty()) {
            return std::string(forced);
        }
        return CalculateBinaryChecksum();
    }();
    return BinaryChecksum;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
