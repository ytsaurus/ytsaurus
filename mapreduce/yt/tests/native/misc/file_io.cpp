#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/http/abortable_http_response.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////

class TTestReaderFixture {
public:
    TTestReaderFixture()
    {
        TConfig::Get()->UseAbortableResponse = true;
        TConfig::Get()->RetryInterval = TDuration();

        FileData_ = GenerateRandomData(10 * 1024 * 1024);
        Client_ = CreateTestClient();

        Client_->Create("//testing/file", ENodeType::NT_FILE);
        auto writer = Client_->CreateFileWriter(TRichYPath("//testing/file").Append(true));
        writer->Write(GetFileData());
        writer->Finish();
    }

    TStringBuf GetFileData() const {
        return FileData_;
    }

    IClientPtr GetClient() const {
        return Client_;
    }

private:
    TString FileData_;
    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(FileIo)
{
    Y_UNIT_TEST(Read)
    {
        TTestReaderFixture testReaderFixture;

        auto client = testReaderFixture.GetClient();
        auto reader = client->CreateFileReader("//testing/file");
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

        UNIT_ASSERT(offset + length < fileData.Size());
        auto reader = client->CreateFileReader(
            "//testing/file",
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

}
