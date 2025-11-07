#include <yt/yt/tools/trigrep/lib/zstd_reader.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/logging/appendable_compressed_file.h>
#include <yt/yt/core/logging/stream_output.h>
#include <yt/yt/core/logging/zstd_log_codec.h>

#include <util/stream/file.h>

namespace NYT::NTrigrep {
namespace {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomLogFileName()
{
    return GenerateRandomFileName("log");
}

void WriteZstdLog(
    const TString& fileName,
    TStringBuf payload,
    const TAppendableCompressedFileOptions& options)
{
    auto codec = CreateZstdLogCodec();
    TFile file(fileName, CreateAlways);
    auto output = CreateAppendableCompressedFile(
        std::move(file),
        std::move(codec),
        GetSyncInvoker(),
        options);
    output->Write(payload);
    output->Flush();
}

std::vector<TString> ReadZstdLogFrames(const TString& fileName)
{
    std::vector<TString> frames;
    auto reader = CreateSequentialZstdReader(fileName);
    while (auto frameReader = reader->TryBeginNextFrame()) {
        auto frame = frameReader->ReadAll();
        if (frame.empty()) {
            break;
        }
        frames.push_back(frame);
    }
    return frames;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TZstdReaderTest, AllowBreakLines)
{
    auto fileName = GenerateRandomLogFileName();
    auto payload = TString("first\nsecond");
    WriteZstdLog(fileName, payload, {.TryNotBreakLines = false});
    auto frames = ReadZstdLogFrames(fileName);
    auto expectedFrames = std::vector{payload};
    EXPECT_EQ(frames, expectedFrames);
}

TEST(TZstdReaderTest, TryNotBreakLines)
{
    auto fileName = GenerateRandomLogFileName();
    auto payload = TString("first\nsecond\nthird");
    WriteZstdLog(fileName, payload, {.TryNotBreakLines = true});
    auto frames = ReadZstdLogFrames(fileName);
    auto expectedFrames = std::vector{TString("first\nsecond\n"), TString("third")};
    EXPECT_EQ(frames, expectedFrames);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTrigrep
