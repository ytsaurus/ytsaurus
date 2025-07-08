#include <gtest/gtest.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/library/s3/client.h>

#include <util/system/env.h>

#include <algorithm>

namespace NYT::NS3 {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TS3ClientTest
    : public ::testing::Test
{
protected:
    const TString Bucket1_ = "test-bucket1";
    const TString Bucket2_ = "test-bucket2";

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
        // Clean all the objects and buckets up.
        auto listBucketsRsp = WaitFor(S3Client_->ListBuckets({}))
            .ValueOrThrow();
        for (const auto& bucket: listBucketsRsp.Buckets) {
            auto listObjectsResponse = WaitFor(S3Client_->ListObjects({
                .Bucket = bucket.Name,
            })).ValueOrThrow();

            std::vector<TString> objectKeys;
            for (const auto& object: listObjectsResponse.Objects) {
                objectKeys.push_back(object.Key);
            }
            S3Client_->DeleteObjects({
                .Bucket = bucket.Name,
                .Objects = std::move(objectKeys),
            }).Wait();

            S3Client_->DeleteBucket({
                bucket.Name,
            }).Wait();
        }
    }
};

TEST_F(TS3ClientTest, PutAndListBuckets)
{
    WaitFor(S3Client_->PutBucket({
        .Bucket = Bucket2_,
    })).ValueOrThrow();

    auto listBucketsRsp = WaitFor(S3Client_->ListBuckets({}))
        .ValueOrThrow();
    ASSERT_EQ(listBucketsRsp.Buckets.size(), 2u);

    // Check that the two buskets returned have the expected names.
    for (const auto& bucket: listBucketsRsp.Buckets) {
        ASSERT_TRUE(bucket.Name == Bucket1_ || bucket.Name == Bucket2_);
    }
}

TEST_F(TS3ClientTest, PutAndGetObjects)
{
    const TString object1Key = "foo1", object1Data = "bar1", object2Key = "foo2", object2Data = "bar2";
    WaitFor(S3Client_->PutObject({
        .Bucket = Bucket1_,
        .Key = object1Key,
        .Data = TSharedRef::FromString(object1Data),
    })).ValueOrThrow();
    WaitFor(S3Client_->PutObject({
        .Bucket = Bucket1_,
        .Key = object2Key,
        .Data = TSharedRef::FromString(object2Data),
    })).ValueOrThrow();

    auto getObjectResponse = WaitFor(S3Client_->GetObject({
        .Bucket = Bucket1_,
        .Key = object1Key,
    })).ValueOrThrow();
    ASSERT_EQ(TString(getObjectResponse.Data.ToStringBuf()), object1Data);

    auto listObjectsResponse = WaitFor(S3Client_->ListObjects({
        .Bucket = Bucket1_,
    })).ValueOrThrow();
    ASSERT_EQ(listObjectsResponse.Objects.size(), 2u);

    // Check that the two objects returned have the expected keys.
    for (const auto& object: listObjectsResponse.Objects) {
        ASSERT_TRUE(object.Key == object1Key || object.Key == object2Key);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NS3
