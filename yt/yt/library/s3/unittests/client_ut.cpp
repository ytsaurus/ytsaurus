#include <gtest/gtest.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/library/s3/client.h>

#include <util/system/env.h>

namespace NYT::NS3 {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TS3ClientTest
    : public ::testing::Test
{
protected:
    const TString Bucket1_ = "test-bucket1";

    ICredentialsProviderPtr S3CredentialProvider_;
    IThreadPoolPollerPtr Poller_;
    IClientPtr S3Client_;

private:
    void SetUp() override
    {
        auto clientConfig = New<NS3::TS3ClientConfig>();
        clientConfig->Url = GetEnv("AWS_ENDPOINT_URL");
        clientConfig->Region = GetEnv("AWS_REGION");

        S3CredentialProvider_ = CreateStaticCredentialProvider(
            GetEnv("AWS_ACCESS_KEY_ID"), GetEnv("AWS_SECRET_ACCESS_KEY"));
        Poller_ = CreateThreadPoolPoller(1, "S3TestPoller");
        S3Client_ = CreateClient(
            std::move(clientConfig),
            S3CredentialProvider_,
            Poller_,
            Poller_->GetInvoker());

        WaitFor(S3Client_->Start())
            .ThrowOnError();

        WaitFor(S3Client_->PutBucket({
            .Bucket = Bucket1_,
        })).ValueOrThrow();
    }

    void TearDown() override
    {
        // Clean all the buckets up.
        auto listBucketsRsp = WaitFor(S3Client_->ListBuckets({}))
            .ValueOrThrow();
        for (const auto& bucket: listBucketsRsp.Buckets) {
            S3Client_->DeleteBucket({
                bucket.Name,
            }).Wait();
        }
    }
};

TEST_F(TS3ClientTest, ListBuckets)
{
    auto listBucketsRsp = WaitFor(S3Client_->ListBuckets({}))
        .ValueOrThrow();
    ASSERT_EQ(listBucketsRsp.Buckets.size(), 1u);
    ASSERT_EQ(listBucketsRsp.Buckets[0].Name, Bucket1_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NS3
