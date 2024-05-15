#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////

class TTestReaderFixture
    : public TTestFixture
{
public:
    static constexpr size_t FileSize = 10 * 1024 * 1024;

    TTestReaderFixture()
        : FileData_(GenerateRandomData(FileSize))
    {
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryInterval = TDuration();

        GetClient()->Create(GetWorkingDir() + "/file", ENodeType::NT_FILE);
        auto writer = GetClient()->CreateFileWriter(TRichYPath(GetWorkingDir() + "/file").Append(true));
        writer->Write(GetFileData());
        writer->Finish();
    }

    TStringBuf GetFileData() const
    {
        return FileData_;
    }

private:
    TString FileData_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(FileIo, Read)
{
    TTestReaderFixture testReaderFixture;

    auto client = testReaderFixture.GetClient();
    auto reader = client->CreateFileReader(testReaderFixture.GetWorkingDir() + "/file");
    TString result;
    const auto readNextPart = [&] {
        char buffer[1024];
        size_t read = reader->Read(buffer, sizeof(buffer));
        result += TStringBuf(buffer, read);
        return read;
    };

    auto read = readNextPart();
    EXPECT_TRUE(read > 0);

    EXPECT_TRUE(TAbortableHttpResponse::AbortAll("/read_file") > 0);

    while (readNextPart()) {
    }

    EXPECT_EQ(result, testReaderFixture.GetFileData());
}

TEST(FileIo, ReadRange)
{
    TTestReaderFixture testReaderFixture;
    auto client = testReaderFixture.GetClient();
    const auto fileData = testReaderFixture.GetFileData();

    constexpr size_t offset = 42;
    constexpr size_t length = 1024 * 1024;

    EXPECT_TRUE(offset + length < fileData.size());
    auto reader = client->CreateFileReader(
        testReaderFixture.GetWorkingDir() + "/file",
        TFileReaderOptions().Offset(offset).Length(length));

    TString result;
    const auto readNextPart = [&] {
        char buffer[1024];
        size_t read = reader->Read(buffer, sizeof(buffer));
        result += TStringBuf(buffer, read);
        return read;
    };

    auto read = readNextPart();
    EXPECT_TRUE(read > 0);

    EXPECT_TRUE(TAbortableHttpResponse::AbortAll("/read_file") > 0);

    while (readNextPart()) {
    }

    EXPECT_EQ(result, fileData.SubStr(offset, length));
}

void TryWriteFile(TFileWriterOptions options)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto writer = client->CreateFileWriter(workingDir + "/file", options);
    writer->Write(GenerateRandomData(10 * 1024 * 1024));
    writer->Finish();
}

TEST(FileIo, InvalidWriterOptionsFail)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TryWriteFile(TFileWriterOptions().WriterOptions(
            TWriterOptions()
                .UploadReplicationFactor(0))),
        NYT::TErrorResponse,
        "/upload_replication_factor");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        TryWriteFile(TFileWriterOptions().WriterOptions(
            TWriterOptions()
                .MinUploadReplicationFactor(0))),
        NYT::TErrorResponse,
        "/min_upload_replication_factor");
}
