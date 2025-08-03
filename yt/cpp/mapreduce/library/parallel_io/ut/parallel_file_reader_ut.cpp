#include <yt/cpp/mapreduce/library/parallel_io/parallel_file_reader.h>
#include <yt/cpp/mapreduce/library/parallel_io/resource_limiter.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>
#include <yt/cpp/mapreduce/library/mock_client/yt_mock.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/string/vector.h>
#include <util/system/tempfile.h>

using namespace NYT;
using namespace NYT::NTesting;
using ::TBlob;

namespace {
    using ::TBlob;

    struct TFile
    {
        TYPath Path;
        TString Content;
    };

    TFile GetTestFile(TTestFixture& fixture, IClientPtr& client, size_t fileSize)
    {
        auto workingDir = fixture.GetWorkingDir();
        TStringStream output;
        output << GenerateRandomData(fileSize);
        output.Flush();
        auto writer = client->CreateFileWriter(workingDir + "/file");
        writer->Write(output.Data(), output.Size());
        writer->Finish();
        return TFile{workingDir + "/file", output.Str()};
    }
} // namespace


TEST(TParallelFileReaderTest, ReadEmptyFileSingleThread)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 0);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().ThreadCount(1));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFileReaderTest, ReadAllSingleThread)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 100_KB);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().ThreadCount(1));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFileReaderTest, ReadAllWithoutTransaction)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 100_KB);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().ThreadCount(1).CreateTransaction(false));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFileReaderTest, ReadEmptyFileMultiThread)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 0);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().BatchSize(12_KB));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFileReaderTest, ReadFileWithLength)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 10_KB);

    auto options = TFileReaderOptions().Length(1_KB);
    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().ThreadCount(1).ReaderOptions(options));

    auto result = reader->ReadAll();

    auto fileContent = TStringBuf(file.Content.begin(), file.Content.begin() + 1_KB);

    EXPECT_EQ(fileContent, result);
}

TEST(TParallelFileReaderTest, ReadFileWithExceededLength)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 15_KB);

    auto options = TFileReaderOptions().Length(100_KB);
    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().ThreadCount(1).BatchSize(10_KB).ReaderOptions(options));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFileReaderTest, ReadAllMultiThread)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 100_KB);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().BatchSize(12_KB));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFileReaderTest, ReadAllPieceMultiThread)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 100_KB);

    TFileReaderOptions baseOptions;
    baseOptions.Length(30_KB);
    baseOptions.Offset(20_KB);
    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().BatchSize(12_KB).ReaderOptions(baseOptions));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content.substr(20_KB, 30_KB), result);
}

TEST(TParallelFileReaderTest, ReadAllBigBatch)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 21_KB);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().BatchSize(97_KB));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFileReaderTest, ReadAllStrictLimiter)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 200_KB);

    auto ramLimiter = ::MakeIntrusive<TResourceLimiter>(11_KB);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().BatchSize(7_KB).RamLimiter(ramLimiter));

    auto result = reader->ReadAll();

    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFileReaderTest, ReadNextBatch)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 1000_KB);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().BatchSize(7_KB));

    size_t offset = 0;
    while (auto blob = reader->ReadNextBatch()) {
        EXPECT_EQ(file.Content.substr(offset, blob->Size()), blob->AsStringBuf());
        offset += blob->Size();
    }
    EXPECT_TRUE(offset == file.Content.size());
}

TEST(TParallelFileReaderTest, ReadToPlace)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 1017_KB);

    auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().BatchSize(7_KB + 17));

    TString data(5_KB + 7, '0');
    size_t offset = 0;
    while (size_t readSize = reader->Read(&(data[0]), data.size())) {
        EXPECT_EQ(file.Content.substr(offset, readSize), data.substr(0, readSize));
        offset += readSize;
    }

    EXPECT_TRUE(offset == file.Content.size());
}

TEST(TParallelFileReaderTest, SaveFile)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 1017_KB);

    SaveFileParallel(client, file.Path, "localPath");
    TFileInput localFile("localPath");

    EXPECT_TRUE(file.Content == localFile.ReadAll());
}

TEST(TParallelFileReaderTest, BadMemoryLimit)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(Y_UNUSED(::MakeIntrusive<TResourceLimiter>(0u)), yexception, "0");

    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TRichYPath path = workingDir + "/file";

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        (CreateParallelFileReader(client, path, TParallelFileReaderOptions().BatchSize(11).RamLimiter(::MakeIntrusive<TResourceLimiter>(10u)))),
        yexception,
        "");
}

TEST(TParallelFileReaderTest, AbadonedReader)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFile(fixture, client, 1000_KB);

    {
        auto reader = CreateParallelFileReader(client, file.Path, TParallelFileReaderOptions().BatchSize(7_KB));
        reader->ReadNextBatch();
        reader->ReadNextBatch();
        reader->ReadNextBatch();
    }
}

TEST(TParallelFileReaderTest, BadFilePath)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto reader = CreateParallelFileReader(client, "nonExistentFile", TParallelFileReaderOptions().BatchSize(7_KB));
    EXPECT_THROW(reader->ReadNextBatch(), NYT::TErrorResponse);
}

TEST(TParallelFileReaderTest, ExceptionFromReadJob)
{
    constexpr size_t NumThreads = 2;

    class TLockMock
        : public ILock
    {
    public:
        const TLockId& GetId() const override
        {
            Y_ABORT("TLockMock");
        }

        /// Get cypress node id of locked object.
        TNodeId GetLockedNodeId() const override
        {
            TNodeId id; // default init is 0
            return id;
        }

        const ::NThreading::TFuture<void>& GetAcquiredFuture() const override
        {
            Y_ABORT("TLockMock");
        }
    };

    class TTransactionMockCreateFileReader
        : public TTransactionMock
    {
    public:
        ILockPtr Lock(const TYPath&, ELockMode, const TLockOptions&) override
        {
            return ::MakeIntrusive<TLockMock>();
        }

        void Unlock(const TYPath&, const TUnlockOptions&) override
        {
        }

        TNode Get(const TYPath&, const TGetOptions&) override
        {
            return 10000;
        }

        IFileReaderPtr CreateFileReader(const TRichYPath&, const TFileReaderOptions&) override
        {
            if (numThreads.fetch_sub(1) != 0) {
                ythrow yexception() << "Exception inside ReaderJob!";
            }
            Y_ABORT("RETHROW_EXCEPTIONS_GUARD must save caught exception and throw it!");
        }

    private:
        std::atomic<int> numThreads = NumThreads;
    };

    class TClientMockStartTransaction
        : public TClientMock
    {
    public:
        ITransactionPtr StartTransaction(const TStartTransactionOptions&) override
        {
            return ::MakeIntrusive<TTransactionMockCreateFileReader>();
        }
    };

    auto client = ::MakeIntrusive<TClientMockStartTransaction>();

    auto reader = CreateParallelFileReader(client, "nonExistentFile", TParallelFileReaderOptions().BatchSize(10).ThreadCount(NumThreads));
    EXPECT_THROW(reader->ReadNextBatch(), yexception);
}
