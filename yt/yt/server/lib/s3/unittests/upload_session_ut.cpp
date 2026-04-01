#include <yt/yt/server/lib/s3/upload_session.h>

#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/s3/client.h>

#include <util/system/env.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("TestLogger");

////////////////////////////////////////////////////////////////////////////////

class TUploadSessionTest
    : public ::testing::Test
{
protected:
    const TString RootBucket_ = "ytsaurus";

    NS3::IClientPtr S3Client_;

    TChunkId ChunkId_;
    TS3MediumDescriptorPtr MediumDescriptor_;

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

    void SetUp() override
    {
        ForbidContextSwitchInFutureHandler();

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
        YT_VERIFY(S3Client_);
    
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

TEST_F(TUploadSessionTest, SimpleUpload)
{
    auto pool = NConcurrency::CreateThreadPool(2, "Worker");
    auto invoker = pool->GetInvoker();
    auto objectPlacement = MediumDescriptor_->GetS3ObjectPlacementForChunk(ChunkId_);
    auto session = New<TS3SimpleUploadSession>(S3Client_, objectPlacement, invoker, Logger);
    auto data = TSharedRef::FromString("one meaningful string");
    auto result = WaitFor(session->Upload(data));
    EXPECT_TRUE(result.IsOK());

    auto getObjectResponse = WaitFor(S3Client_->GetObject({
        .Bucket = TString(objectPlacement.Bucket),
        .Key = TString(objectPlacement.Key),
    }))
        .ValueOrThrow();
    EXPECT_EQ(getObjectResponse.Data.ToStringBuf(), "one meaningful string");
}

TEST_F(TUploadSessionTest, ParallelMultiPartUpload)
{
    const int N = 100;
    auto pool = NConcurrency::CreateThreadPool(4, "Worker");
    auto invoker = pool->GetInvoker();
    auto objectPlacement = MediumDescriptor_->GetS3ObjectPlacementForChunk(ChunkId_);
    auto session = New<TS3MultiPartUploadSession>(
        S3Client_,
        objectPlacement,
        TS3MultiPartUploadSession::TOptions{.PartSize = 64_MB, .UploadWindowSize = 128_MB},
        invoker,
        Logger);
    WaitFor(session->Start())
        .ThrowOnError();

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < N; ++i) {
        futures.push_back(BIND([&session, i] {
            session->Add({TSharedRef::FromString(Format("%v", i))});
        }).AsyncVia(pool->GetInvoker()).Run());
    }

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();

    auto result = WaitFor(session->Complete());
    EXPECT_TRUE(result.IsOK());

    auto getObjectResponse = WaitFor(S3Client_->GetObject({
        .Bucket = TString(objectPlacement.Bucket),
        .Key = TString(objectPlacement.Key),
    }))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(getObjectResponse.Data.ToStringBuf()),  10 + 2 * 90);
}

TEST_F(TUploadSessionTest, AbortIncompleteMultiPartUpload)
{
    const int N = 100;
    auto pool = NConcurrency::CreateThreadPool(4, "Worker");
    auto invoker = pool->GetInvoker();
    auto objectPlacement = MediumDescriptor_->GetS3ObjectPlacementForChunk(ChunkId_);
    auto session = New<TS3MultiPartUploadSession>(
        S3Client_,
        objectPlacement,
        TS3MultiPartUploadSession::TOptions{.PartSize = 64_MB, .UploadWindowSize = 128_MB},
        invoker,
        Logger);
    WaitFor(session->Start())
        .ThrowOnError();

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < N; ++i) {
        futures.push_back(BIND([&session, i] {
            session->Add({TSharedRef::FromString(Format("%v", i))});
        }).AsyncVia(pool->GetInvoker()).Run());
    }

    futures.push_back(BIND([&session] {
        return session->Abort(TError("Some error"));
    }).AsyncVia(pool->GetInvoker()).Run());

    EXPECT_TRUE(WaitFor(AllSucceeded(std::move(futures))).IsOK());

    auto getObjectResponseOrError = WaitFor(S3Client_->GetObject({
        .Bucket = TString(objectPlacement.Bucket),
        .Key = TString(objectPlacement.Key),
    }));
    EXPECT_FALSE(getObjectResponseOrError.IsOK());
    EXPECT_TRUE(getObjectResponseOrError.GetMessage().contains("Not Found"));
}

TEST_F(TUploadSessionTest, AbortCompletedMultiPartUpload)
{
    const int N = 100;
    auto pool = NConcurrency::CreateThreadPool(4, "Worker");
    auto invoker = pool->GetInvoker();
    auto objectPlacement = MediumDescriptor_->GetS3ObjectPlacementForChunk(ChunkId_);
    auto session = New<TS3MultiPartUploadSession>(
        S3Client_,
        objectPlacement,
        TS3MultiPartUploadSession::TOptions{.PartSize = 64_MB, .UploadWindowSize = 128_MB},
        invoker,
        Logger);
    WaitFor(session->Start())
        .ThrowOnError();

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < N; ++i) {
        futures.push_back(BIND([&session, i] {
            session->Add({TSharedRef::FromString(Format("%v", i))});
        }).AsyncVia(pool->GetInvoker()).Run());
    }

    EXPECT_TRUE(WaitFor(AllSucceeded(std::move(futures))).IsOK());
    EXPECT_TRUE(WaitFor(session->Complete()).IsOK());
    auto getObjectResponse = WaitFor(S3Client_->GetObject({
        .Bucket = TString(objectPlacement.Bucket),
        .Key = TString(objectPlacement.Key),
    }))
        .ValueOrThrow();
    EXPECT_EQ(std::ssize(getObjectResponse.Data.ToStringBuf()),  10 + 2 * 90);

    EXPECT_TRUE(WaitFor(session->Abort(TError("Some error"))).IsOK());

    auto getObjectResponseOrError = WaitFor(S3Client_->GetObject({
        .Bucket = TString(objectPlacement.Bucket),
        .Key = TString(objectPlacement.Key),
    }));
    EXPECT_FALSE(getObjectResponseOrError.IsOK());
    EXPECT_TRUE(getObjectResponseOrError.GetMessage().contains("Not Found"));
}

TEST_F(TUploadSessionTest, MultipleAbortMultiPartUpload)
{
    const int N = 100;
    auto pool = NConcurrency::CreateThreadPool(4, "Worker");
    auto invoker = pool->GetInvoker();
    auto objectPlacement = MediumDescriptor_->GetS3ObjectPlacementForChunk(ChunkId_);
    auto session = New<TS3MultiPartUploadSession>(
        S3Client_,
        objectPlacement,
        TS3MultiPartUploadSession::TOptions{.PartSize = 64_MB, .UploadWindowSize = 128_MB},
        invoker,
        Logger);
    WaitFor(session->Start())
        .ThrowOnError();

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < N; ++i) {
        futures.push_back(session->Abort(TError("Some error")));
    }

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
