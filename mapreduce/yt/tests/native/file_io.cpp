#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/http/abortable_http_response.h>

#include <util/random/fast.h>

namespace NYT {
namespace NNativeTest {

////////////////////////////////////////////////////////////////////////////////

static Stroka GenerateRandomData(size_t size, ui64 seed = 42) {
    TReallyFastRng32 rng(seed);

    Stroka result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = rng.GenRand64();
        result += TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    result.resize(size);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TFileIo
    : public NTest::TTest
{
public:
    void SetUp() override
    {
        TTest::SetUp();
        FileData_ = GenerateRandomData(10 * 1024 * 1024);
        Client_ = CreateClient(ServerName());
        Client()->Create("//testing", NYT::ENodeType::NT_MAP, TCreateOptions().IgnoreExisting(true));
        Client()->Remove("//testing/*", TRemoveOptions().Force(true).Recursive(true));

        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryInterval = TDuration();

        {
            // we write our data to several chunks
            Client()->Create("//testing/file", ENodeType::NT_FILE);
            auto writer = Client()->CreateFileWriter(TRichYPath("//testing/file").Append(true));
            writer->Write(GetFileData());
            writer->Finish();
        }
    }

    void TearDown() override
    {
        TTest::TearDown();
    }

    IClientPtr Client() {
        return Client_;
    }

    TStringBuf GetFileData() const {
        return FileData_;
    }

private:
    IClientPtr Client_;
    Stroka FileData_;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TFileIo, Read)
{

    auto reader = Client()->CreateFileReader("//testing/file");

    Stroka result;
    const auto readNextPart = [&] {
        char buffer[1024];
        size_t read = reader->Read(buffer, sizeof(buffer));
        result += TStringBuf(buffer, read);
        return read;
    };

    auto read = readNextPart();
    UNIT_ASSERT(read > 0);

    UNIT_ASSERT(TAbortableHttpResponse::AbortAll("/read_file") > 0);

    while (readNextPart()) {
    }

    UNIT_ASSERT(result == GetFileData());
}

YT_TEST(TFileIo, ReadRange)
{
    constexpr size_t offset = 42;
    constexpr size_t length = 1024 * 1024;

    UNIT_ASSERT(offset + length < GetFileData().Size());
    auto reader = Client()->CreateFileReader(
        "//testing/file",
        TFileReaderOptions().Offset(offset).Length(length));

    Stroka result;
    const auto readNextPart = [&] {
        char buffer[1024];
        size_t read = reader->Read(buffer, sizeof(buffer));
        result += TStringBuf(buffer, read);
        return read;
    };

    auto read = readNextPart();
    UNIT_ASSERT(read > 0);

    UNIT_ASSERT(TAbortableHttpResponse::AbortAll("/read_file") > 0);

    while (readNextPart()) {
    }

    UNIT_ASSERT(result == GetFileData().SubStr(offset, length));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNativeTest
} // namespace NYT
