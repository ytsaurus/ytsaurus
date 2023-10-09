#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/server/lib/hydra/checkpointable_stream.h>

#include <util/stream/mem.h>
#include <util/stream/str.h>

#include <array>

namespace NYT::NHydra {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TCheckpointableStreamTest, Simple)
{
    TStringStream stringOutput;
    auto output = CreateCheckpointableOutputStream(&stringOutput);

    output->Write("abc");
    output->Write("111");
    output->Write("ololo");

    TStringInput stringInput(stringOutput.Str());
    auto input = CreateCheckpointableInputStream(&stringInput);

    std::array<char, 10> buffer;

    EXPECT_EQ(0, input->GetOffset());
    EXPECT_EQ(2u, input->Load(buffer.data(), 2));
    EXPECT_EQ(2, input->GetOffset());
    EXPECT_EQ("ab", TStringBuf(buffer.data(), 2));

    EXPECT_EQ(2u, input->Load(buffer.data(), 2));
    EXPECT_EQ(4, input->GetOffset());
    EXPECT_EQ("c1", TStringBuf(buffer.data(), 2));

    EXPECT_EQ(7u, input->Load(buffer.data(), 10));
    EXPECT_EQ(11, input->GetOffset());
    EXPECT_EQ("11ololo", TStringBuf(buffer.data(), 7));
}

TEST(TCheckpointableStreamTest, Checkpoints)
{
    TStringStream stringOutput;
    auto output = CreateCheckpointableOutputStream(&stringOutput);

    output->Write("abc");
    output->Write("111");
    output->MakeCheckpoint();
    output->Write("u");
    output->MakeCheckpoint();
    output->Write("ololo");

    TStringInput stringInput(stringOutput.Str());
    auto input = CreateCheckpointableInputStream(&stringInput);

    std::array<char, 10> buffer;

    EXPECT_EQ(0, input->GetOffset());
    EXPECT_EQ(2u, input->Load(buffer.data(), 2));
    EXPECT_EQ(2, input->GetOffset());
    EXPECT_EQ("ab", TStringBuf(buffer.data(), 2));

    input->SkipToCheckpoint();

    EXPECT_EQ(6, input->GetOffset());
    EXPECT_EQ(1u, input->Load(buffer.data(), 1));
    EXPECT_EQ(7, input->GetOffset());
    EXPECT_EQ("u", TStringBuf(buffer.data(), 1));

    input->SkipToCheckpoint();

    EXPECT_EQ(7, input->GetOffset());
    EXPECT_EQ(2u, input->Load(buffer.data(), 2));
    EXPECT_EQ(9, input->GetOffset());
    EXPECT_EQ("ol", TStringBuf(buffer.data(), 2));

    EXPECT_EQ(2u, input->Load(buffer.data(), 2));
    EXPECT_EQ(11, input->GetOffset());
    EXPECT_EQ("ol", TStringBuf(buffer.data(), 2));

    input->SkipToCheckpoint();

    EXPECT_EQ(12, input->GetOffset());
    EXPECT_EQ(0u, input->Load(buffer.data(), 10));
}

TEST(TCheckpointableStreamTest, Buffered)
{
    std::vector<char> blob;
    for (int i = 0; i < 1000; ++i) {
        blob.push_back(std::rand() % 128);
    }

    TStringStream stringOutput;
    auto checkpointableOutput = CreateBufferedCheckpointableOutputStream(&stringOutput, 256);
    for (int i = 1; i <= 100; ++i) {
        for (int j = 0; j < i * 17; ++j) {
            checkpointableOutput->Write((i + j) % 128);
        }
        checkpointableOutput->Write(blob.data(), blob.size());
        checkpointableOutput->MakeCheckpoint();
    }
    checkpointableOutput->Flush();

    TStringInput stringInput(stringOutput.Str());
    auto checkpointableInput = CreateCheckpointableInputStream(&stringInput);

    std::vector<char> buffer;
    for (int i = 1; i <= 100; ++i) {
        if (i % 2 == 0) {
            buffer.resize(i * 17);
            EXPECT_EQ(checkpointableInput->Load(buffer.data(), buffer.size()), buffer.size());
            for (int j = 0; j < i * 17; ++j) {
                EXPECT_EQ((i + j) % 128, buffer[j]);
            }

            std::vector<char> blobCopy(blob.size());
            EXPECT_EQ(checkpointableInput->Load(blobCopy.data(), blobCopy.size()), blobCopy.size());
            EXPECT_EQ(blob, blobCopy);
        }
        checkpointableInput->SkipToCheckpoint();
    }
}

TEST(TCheckpointableStreamTest, BufferedAsync)
{
    TString str;
    TStringOutput stringOutput(str);
    auto asyncOutput = CreateAsyncAdapter(&stringOutput);
    auto checkpointableOutput = CreateBufferedCheckpointableSyncAdapter(asyncOutput, EWaitForStrategy::Get, 10);

    auto write = [&] (const TString& str) {
        const char* srcPtr = str.data();
        size_t srcLen = str.length();
        void* dstPtr = nullptr;
        size_t dstLen = 0;
        while (srcLen > 0) {
            dstLen = checkpointableOutput->Next(&dstPtr);
            size_t toCopy = Min(dstLen, srcLen);
            ::memcpy(dstPtr, srcPtr, toCopy);
            srcLen -= toCopy;
            dstLen -= toCopy;
            srcPtr += toCopy;
            dstPtr = static_cast<char*>(dstPtr) + toCopy;
        }
        checkpointableOutput->Undo(dstLen);
    };

    checkpointableOutput->MakeCheckpoint();
    const auto part1 = TString("01234567890123456789");
    write(part1);

    checkpointableOutput->MakeCheckpoint();
    const auto part2 = TString("abcdefghijklmnop");
    write(part2);

    checkpointableOutput->MakeCheckpoint();
    const auto part3 = TString("adsjkfjasdlkfjasldf");
    write(part3);

    checkpointableOutput->Flush();

    TStringInput stringInput(str);
    auto checkpointableInput = CreateCheckpointableInputStream(&stringInput);

    auto read = [&] (size_t len) {
        TString result;
        result.resize(len);
        YT_VERIFY(checkpointableInput->Load(result.begin(), len) == len);
        return result;
    };

    auto isEos = [&] {
        char ch;
        return checkpointableInput->Load(&ch, sizeof(ch)) == 0;
    };

    EXPECT_EQ(read(part1.length()), part1);
    EXPECT_EQ(read(3), part2.substr(0, 3));
    checkpointableInput->SkipToCheckpoint();
    EXPECT_EQ(read(part3.length()), part3);
    EXPECT_TRUE(isEos());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
