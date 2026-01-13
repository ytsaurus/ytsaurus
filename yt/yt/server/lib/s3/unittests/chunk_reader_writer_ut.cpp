#include <yt/yt/server/lib/s3/config.h>
#include <yt/yt/server/lib/s3/chunk_reader.h>
#include <yt/yt/server/lib/s3/chunk_writer.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/s3/client.h>

#include <util/system/env.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NObjectClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TS3TestCase
{
    int BatchCount = 1;
    int BlockCountInChunk = 10;
    int RequestCountInReadBatch = 10;
    int BlockInRequest = 2;
    bool ReadSequentially = true;
};

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomString(size_t size, TRandomGenerator* generator)
{
    TString result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = generator->Generate<ui64>();
        result += TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    result.resize(size);
    return result;
}

std::vector<TBlock> CreateBlocks(int count, TRandomGenerator* generator)
{
    std::vector<TBlock> blocks;
    blocks.reserve(count);

    for (int index = 0; index < count; index++) {
        int size = 10 + generator->Generate<uint>() % 11;
        blocks.push_back(TBlock(TSharedRef::FromString(GenerateRandomString(size, generator))));
    }

    return blocks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

class TS3ReaderWriterTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TS3TestCase>
{
protected:
    const TString RootBucket_ = "ytsaurus";

    NS3::IClientPtr S3Client_;

    IChunkReaderPtr S3ChunkReader_;
    IChunkWriterPtr S3ChunkWriter_;
    TChunkId ChunkId_;
    TS3MediumDescriptorPtr MediumDescriptor_;

    TRandomGenerator Generator_ = TRandomGenerator(42);
    std::vector<TBlock> GeneratedBlocks_;

protected:
    void SetUpS3Client()
    {
        // The following environment variables are expected for the test to work.
        auto endpointUrl = GetEnv("AWS_ENDPOINT_URL");
        auto region = GetEnv("AWS_REGION");
        auto accessKeyId = GetEnv("AWS_ACCESS_KEY_ID");
        auto secretAccessKey = GetEnv("AWS_SECRET_ACCESS_KEY");
        if (endpointUrl.empty() || region.empty() || accessKeyId.empty() || secretAccessKey.empty()) {
            GTEST_FAIL() << "S3 environment is not configured; check if the local_s3_recipe is included";
        }

        auto clientConfig = New<NS3::TS3ClientConfig>();
        clientConfig->Url = endpointUrl;
        clientConfig->Region = region;

        auto s3CredentialProvider = NS3::CreateStaticCredentialProvider(accessKeyId, secretAccessKey);
        auto poller = CreateThreadPoolPoller(1, "S3TestPoller");
        S3Client_ = CreateClient(
            std::move(clientConfig),
            std::move(s3CredentialProvider),
            /*sslContextConfig*/ nullptr,
            poller,
            poller->GetInvoker());

        WaitFor(S3Client_->Start())
            .ThrowOnError();
    }

    void SetUpS3Reader()
    {
        YT_VERIFY(S3Client_);
        YT_VERIFY(MediumDescriptor_);

        auto config = New<TS3ReaderConfig>();
        S3ChunkReader_ = CreateS3RegularChunkReader(S3Client_, MediumDescriptor_, std::move(config), ChunkId_);
    }

    void SetUpS3Writer()
    {
        YT_VERIFY(S3Client_);
        YT_VERIFY(MediumDescriptor_);

        auto writerConfig = New<TS3WriterConfig>();
        S3ChunkWriter_ = CreateS3RegularChunkWriter(
            S3Client_,
            MediumDescriptor_,
            std::move(writerConfig),
            TSessionId{ChunkId_, /*mediumIndex*/ 0});
    }

    void SetUp() override
    {
        auto testCase = GetParam();
        auto blockCountInChunk = testCase.BlockCountInChunk;
        GeneratedBlocks_ = CreateBlocks(blockCountInChunk, &Generator_);
        EXPECT_EQ(std::ssize(GeneratedBlocks_), blockCountInChunk);

        ChunkId_ = MakeRandomId(EObjectType::Chunk, TCellTag(0xf003));
        auto mediumConfig = New<TS3MediumConfig>();
        mediumConfig->Bucket = RootBucket_;
        MediumDescriptor_ = New<TS3MediumDescriptor>(
            /*name*/ "test_s3_medium",
            /*index*/ 0,
            /*priority*/ 0,
            std::move(mediumConfig));

        SetUpS3Client();
        CleanBuckets();
        SetUpS3Reader();
        SetUpS3Writer();

        WaitFor(S3Client_->PutBucket({
            .Bucket = RootBucket_,
        })).ThrowOnError();
    }

    void TearDown() override
    {
        CleanBuckets();
    }

    void CleanBuckets()
    {
        if (!S3Client_) {
            // It means that we have skipped this test suite.
            return;
        }

        // Clean all the objects and buckets up.
        auto listBucketsRsp = WaitFor(S3Client_->ListBuckets({}))
            .ValueOrThrow();
        for (const auto& bucket: listBucketsRsp.Buckets) {
            auto listObjectsResponse = WaitFor(S3Client_->ListObjects({
                .Bucket = bucket.Name,
            }))
                .ValueOrThrow();

            std::vector<TString> objectKeys;
            for (const auto& object: listObjectsResponse.Objects) {
                objectKeys.push_back(object.Key);
            }
            if (!objectKeys.empty()) {
                WaitFor(S3Client_->DeleteObjects({
                    .Bucket = bucket.Name,
                    .Objects = std::move(objectKeys),
                }))
                    .ValueOrThrow();
            }

            WaitFor(S3Client_->DeleteBucket({
                bucket.Name,
            }))
                .ValueOrThrow();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TS3ReaderWriterTest, BlobsLayoutOnWrite)
{
    IChunkWriter::TWriteBlocksOptions writeOptions;
    TWorkloadDescriptor workloadDescriptor;

    S3ChunkWriter_->Open()
        .Apply(BIND([&] {
            EXPECT_TRUE(S3ChunkWriter_->WriteBlocks(writeOptions, workloadDescriptor, GeneratedBlocks_));
            return S3ChunkWriter_->GetReadyEvent();
        }))
        .Apply(BIND([&] {
            auto deferredMeta = New<TDeferredChunkMeta>();
            deferredMeta->set_type(0);
            deferredMeta->set_format(0);
            *deferredMeta->mutable_extensions() = {};
            return S3ChunkWriter_->Close({}, {}, deferredMeta);
        }))
        .Wait(TDuration::Seconds(120));

    {
        auto response = WaitFor(S3Client_->ListBuckets({})).ValueOrThrow();
        EXPECT_EQ(std::ssize(response.Buckets), 1);
        EXPECT_EQ(response.Buckets.front().Name, RootBucket_);
    }
    {
        auto response = WaitFor(S3Client_->ListObjects({.Bucket = RootBucket_}))
            .ValueOrThrow();
        EXPECT_EQ(std::ssize(response.Objects), 2);
        EXPECT_EQ(response.Objects.front().Key, Format("chunk-data/%v", ChunkId_));
        EXPECT_EQ(response.Objects.back().Key, Format("chunk-data/%v.meta", ChunkId_));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TS3ReaderWriterTest, WriteAndReadBlocks)
{
    IChunkWriter::TWriteBlocksOptions writeOptions;
    TWorkloadDescriptor workloadDescriptor;

    NTracing::TTraceContextGuard g(NTracing::TTraceContext::NewRoot("a"));
    S3ChunkWriter_->Open()
        .Apply(BIND([&] {
            EXPECT_TRUE(S3ChunkWriter_->WriteBlocks(writeOptions, workloadDescriptor, GeneratedBlocks_));
            return S3ChunkWriter_->GetReadyEvent();
        }))
        .Apply(BIND([&] {
            auto deferredMeta = New<TDeferredChunkMeta>();
            deferredMeta->set_type(0);
            deferredMeta->set_format(0);
            *deferredMeta->mutable_extensions() = {};
            return S3ChunkWriter_->Close({}, {}, deferredMeta);
        }))
        .Wait(TDuration::Seconds(120));

    auto pool = NConcurrency::CreateThreadPool(16, "Worker");
    auto invoker = pool->GetInvoker();
    auto testCase = GetParam();

    i64 minBytesToRead = 0;
    i64 maxBytesToRead = 0;
    std::vector<bool> requestedBlockMask(testCase.BlockCountInChunk);
    IChunkReader::TReadBlocksOptions readBlockOptions;
    for (int batchIndex = 0; batchIndex < testCase.BatchCount; ++batchIndex) {
        std::vector<TFuture<std::vector<TBlock>>> readFutures;

        int requestSize = std::max(std::max(testCase.BlockCountInChunk / testCase.RequestCountInReadBatch, 1), testCase.BlockInRequest);

        for (int requestIndex = 0; requestIndex < testCase.RequestCountInReadBatch; ++requestIndex) {
            std::vector<int> blockIndicies;
            int blockInRequestCount = std::max<int>(Generator_.Generate<ui64>() % requestSize, 1);
            for (int blockIndex = 0; blockIndex < blockInRequestCount; blockIndex++) {
                if (testCase.ReadSequentially) {
                    blockIndicies.push_back((requestIndex + blockIndex) % testCase.BlockCountInChunk);
                } else {
                    blockIndicies.push_back(Generator_.Generate<ui64>() % testCase.BlockCountInChunk);
                }
            }

            // Blocks may be requested in any order. Shuffle block indicies.
            std::random_shuffle(blockIndicies.begin(), blockIndicies.end());

            auto estimatedBytesToRead = 0;
            for (auto blockIndex : blockIndicies) {
                maxBytesToRead += GeneratedBlocks_[blockIndex].Size();
                estimatedBytesToRead += GeneratedBlocks_[blockIndex].Size();
                if (!requestedBlockMask[blockIndex]) {
                    requestedBlockMask[blockIndex] = true;
                    minBytesToRead += GeneratedBlocks_[blockIndex].Size();
                }
            }

            readBlockOptions.EstimatedSize = estimatedBytesToRead;

            auto future = S3ChunkReader_->ReadBlocks(readBlockOptions, blockIndicies)
                .Apply(BIND([=, this] (const std::vector<TBlock>& returnedBlocks) {
                    EXPECT_EQ(blockIndicies.size(), returnedBlocks.size());
                    int index = 0;
                    for (const auto& block : returnedBlocks) {
                        EXPECT_EQ(GeneratedBlocks_[blockIndicies[index]].GetOrComputeChecksum(), block.GetOrComputeChecksum());
                        index++;
                    }
                    return returnedBlocks;
                }).AsyncVia(invoker));
            readFutures.push_back(std::move(future));
        }

        WaitFor(AllSucceeded(std::move(readFutures)))
            .ThrowOnError();

        auto statistics = readBlockOptions.ClientOptions.ChunkReaderStatistics;

        EXPECT_GE(statistics->DataBytesReadFromDisk, minBytesToRead);
        EXPECT_LE(statistics->DataBytesReadFromDisk, maxBytesToRead);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TS3ReaderWriterTest,
    TS3ReaderWriterTest,
    ::testing::Values(
        TS3TestCase{},
        TS3TestCase{
            .BatchCount = 16,
            .BlockCountInChunk = 1024,
            .RequestCountInReadBatch = 32,
            .BlockInRequest = 16,
            .ReadSequentially = true,
        },
        TS3TestCase{
            .BatchCount = 16,
            .BlockCountInChunk = 1024,
            .RequestCountInReadBatch = 32,
            .BlockInRequest = 16,
            .ReadSequentially = false,
        }
    ));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
