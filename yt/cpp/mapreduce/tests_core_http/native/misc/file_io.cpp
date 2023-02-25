#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <library/cpp/testing/unittest/registar.h>

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

Y_UNIT_TEST_SUITE(FileIo)
{
    Y_UNIT_TEST(Read)
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
        UNIT_ASSERT(read > 0);

        UNIT_ASSERT(TAbortableHttpResponse::AbortAll("/read_file") > 0);

        while (readNextPart()) {
        }

        UNIT_ASSERT_VALUES_EQUAL(result, testReaderFixture.GetFileData());
    }

    Y_UNIT_TEST(ReadRange)
    {
        TTestReaderFixture testReaderFixture;
        auto client = testReaderFixture.GetClient();
        const auto fileData = testReaderFixture.GetFileData();

        constexpr size_t offset = 42;
        constexpr size_t length = 1024 * 1024;

        UNIT_ASSERT(offset + length < fileData.size());
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
        UNIT_ASSERT(read > 0);

        UNIT_ASSERT(TAbortableHttpResponse::AbortAll("/read_file") > 0);

        while (readNextPart()) {
        }

        UNIT_ASSERT_VALUES_EQUAL(result, fileData.SubStr(offset, length));
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

    Y_UNIT_TEST(InvalidWriterOptionsFail)
    {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TryWriteFile(TFileWriterOptions().WriterOptions(
                TWriterOptions()
                    .UploadReplicationFactor(0))),
            NYT::TErrorResponse,
            "/upload_replication_factor");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TryWriteFile(TFileWriterOptions().WriterOptions(
                TWriterOptions()
                    .MinUploadReplicationFactor(0))),
            NYT::TErrorResponse,
            "/min_upload_replication_factor");
    }
}
