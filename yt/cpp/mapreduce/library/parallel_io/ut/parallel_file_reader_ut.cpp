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

////////////////////////////////////////////////////////////////////////////////

namespace {
    // Every part past the first is appended in a separate session, so several parts make the file multichunk.
    TFile GetTestFileWithChunks(const TTestFixture& fixture, const IClientPtr& client, const TVector<size_t>& chunkSizes)
    {
        auto path = fixture.GetWorkingDir() + "/partitioned_file";
        TString content;
        for (auto chunkSize : chunkSizes) {
            auto part = GenerateRandomData(chunkSize, /*seed*/ content.size() + 1);
            auto writer = client->CreateFileWriter(TRichYPath(path).Append(!content.empty()));
            writer->Write(part);
            writer->Finish();
            content += part;
        }
        return TFile{path, content};
    }

    TVector<TFilePartition> GetPartitions(const IClientPtr& client, const TFile& file, size_t partitionSize)
    {
        TVector<TFileReadRange> ranges;
        for (size_t begin = 0; begin < file.Content.size(); begin += partitionSize) {
            auto end = std::min(begin + partitionSize, file.Content.size());
            ranges.push_back(TFileReadRange().Begin(begin).End(end));
        }
        return client->GetFilePartitions(file.Path, ranges).Partitions;
    }

    class TFailingFileReader
        : public IFileReader
    {
    protected:
        size_t DoRead(void* /*buf*/, size_t /*len*/) override
        {
            ythrow yexception() << "Partition stream failure";
        }
    };

    class TFailingPartitionClientMock
        : public TClientMock
    {
    public:
        IFileReaderPtr CreateFilePartitionReader(const TString& /*cookie*/, const TFilePartitionReaderOptions& /*options*/) override
        {
            ++ReaderCount_;
            return ::MakeIntrusive<TFailingFileReader>();
        }

        int GetReaderCount() const
        {
            return ReaderCount_;
        }

    private:
        std::atomic<int> ReaderCount_ = 0;
    };
} // namespace

TEST(TParallelFilePartitionReaderTest, ReadAllSingleThread)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFileWithChunks(fixture, client, {10_KB, 10_KB, 10_KB});
    auto partitions = GetPartitions(client, file, 7_KB);

    auto reader = CreateParallelFilePartitionReader(client, partitions, TParallelFilePartitionReaderOptions().ThreadCount(1));

    EXPECT_EQ(file.Content, reader->ReadAll());
}

TEST(TParallelFilePartitionReaderTest, ReadNextBatchMultiThread)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFileWithChunks(fixture, client, {10_KB, 10_KB, 10_KB});
    auto partitions = GetPartitions(client, file, 7_KB);

    auto reader = CreateParallelFilePartitionReader(client, partitions);

    size_t offset = 0;
    size_t index = 0;
    while (auto blob = reader->ReadNextBatch()) {
        ASSERT_LT(index, partitions.size());
        EXPECT_EQ(static_cast<i64>(blob->Size()), partitions[index].Length);
        EXPECT_EQ(file.Content.substr(offset, blob->Size()), blob->AsStringBuf());
        offset += blob->Size();
        ++index;
    }
    EXPECT_EQ(index, partitions.size());
    EXPECT_EQ(offset, file.Content.size());
}

TEST(TParallelFilePartitionReaderTest, ReadInGivenOrder)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFileWithChunks(fixture, client, {10_KB, 10_KB});
    auto partitions = GetPartitions(client, file, 7_KB);
    std::reverse(partitions.begin(), partitions.end());

    TString expected;
    for (size_t index = partitions.size(); index > 0; --index) {
        expected += file.Content.substr((index - 1) * 7_KB, 7_KB);
    }

    auto reader = CreateParallelFilePartitionReader(client, partitions);

    EXPECT_EQ(expected, reader->ReadAll());
}

TEST(TParallelFilePartitionReaderTest, SkipEmptyPartitions)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFileWithChunks(fixture, client, {10_KB});

    TVector<TFileReadRange> ranges = {
        TFileReadRange().Begin(0).End(0),
        TFileReadRange().Begin(0).End(5_KB),
        TFileReadRange().Begin(5_KB).End(5_KB),
        TFileReadRange().Begin(5_KB),
    };
    auto partitions = client->GetFilePartitions(file.Path, ranges).Partitions;

    auto reader = CreateParallelFilePartitionReader(client, partitions);

    size_t batchCount = 0;
    TString result;
    while (auto blob = reader->ReadNextBatch()) {
        EXPECT_EQ(5_KB, blob->Size());
        result += blob->AsStringBuf();
        ++batchCount;
    }
    EXPECT_EQ(2u, batchCount);
    EXPECT_EQ(file.Content, result);
}

TEST(TParallelFilePartitionReaderTest, ReadNoPartitions)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();

    auto reader = CreateParallelFilePartitionReader(client, {});

    EXPECT_FALSE(reader->ReadNextBatch());
    EXPECT_EQ("", reader->ReadAll());
}

TEST(TParallelFilePartitionReaderTest, ReadAllStrictLimiter)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto file = GetTestFileWithChunks(fixture, client, {10_KB, 10_KB, 10_KB});
    auto partitions = GetPartitions(client, file, 7_KB);

    auto ramLimiter = ::MakeIntrusive<TResourceLimiter>(7_KB + 1);
    auto reader = CreateParallelFilePartitionReader(client, partitions, TParallelFilePartitionReaderOptions().RamLimiter(ramLimiter));

    EXPECT_EQ(file.Content, reader->ReadAll());
}

TEST(TParallelFilePartitionReaderTest, BadMemoryLimit)
{
    auto client = ::MakeIntrusive<TClientMock>();
    TVector<TFilePartition> partitions = {TFilePartition{.Cookie = "cookie", .Length = 11}};

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        (CreateParallelFilePartitionReader(client, partitions, TParallelFilePartitionReaderOptions().RamLimiter(::MakeIntrusive<TResourceLimiter>(10u)))),
        yexception,
        "");
}

TEST(TParallelFilePartitionReaderTest, ExceptionFromReadJob)
{
    auto client = ::MakeIntrusive<TFailingPartitionClientMock>();
    TVector<TFilePartition> partitions = {TFilePartition{.Cookie = "cookie", .Length = 10}};

    auto reader = CreateParallelFilePartitionReader(client, partitions, TParallelFilePartitionReaderOptions().ThreadCount(1));

    EXPECT_THROW(reader->ReadNextBatch(), yexception);
    EXPECT_EQ(1, client->GetReaderCount());
}
