#include <gtest/gtest.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/library/s3/client.h>

#include <util/system/env.h>

#include <algorithm>

namespace NYT::NS3 {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

//! Test for the S3 client.
/*!
 *  As of now, this test expects an S3 environment to be launched independently, and its
 *  parameters provided in the environment variables mentioned in SetUp(). This will be
 *  enhanced when we're able to run a local S3 instance as part of the test suite.
 */
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

        S3CredentialProvider_ = CreateStaticCredentialProvider(accessKeyId, secretAccessKey);
        Poller_ = CreateThreadPoolPoller(1, "S3TestPoller");
        S3Client_ = CreateClient(
            std::move(clientConfig),
            S3CredentialProvider_,
            /*sslContextConfig*/ nullptr,
            Poller_,
            Poller_->GetInvoker());

        WaitFor(S3Client_->Start())
            .ThrowOnError();

        CleanBuckets();
        WaitFor(S3Client_->PutBucket({
            .Bucket = Bucket1_,
        }))
            .ValueOrThrow();
    }

    void TearDown() override
    {
        CleanBuckets();
    }

    void CleanBuckets()
    {
        if (S3Client_ == nullptr) {
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

TEST_F(TS3ClientTest, PutAndListBuckets)
{
    WaitFor(S3Client_->PutBucket({
        .Bucket = Bucket2_,
    }))
        .ValueOrThrow();

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
    const TString object1Key = "foo1";
    const TString object1Data = "bar1";
    const TString object2Key = "foo2";
    const TString object2Data = "bar2";
    WaitFor(S3Client_->PutObject({
        .Bucket = Bucket1_,
        .Key = object1Key,
        .Data = TSharedRef::FromString(object1Data),
    }))
        .ValueOrThrow();
    WaitFor(S3Client_->PutObject({
        .Bucket = Bucket1_,
        .Key = object2Key,
        .Data = TSharedRef::FromString(object2Data),
    }))
        .ValueOrThrow();

    auto getObjectResponse = WaitFor(S3Client_->GetObject({
        .Bucket = Bucket1_,
        .Key = object1Key,
    }))
        .ValueOrThrow();
    ASSERT_EQ(std::string(getObjectResponse.Data.ToStringBuf()), object1Data);

    auto listObjectsResponse = WaitFor(S3Client_->ListObjects({
        .Bucket = Bucket1_,
    }))
        .ValueOrThrow();
    ASSERT_EQ(listObjectsResponse.Objects.size(), 2u);

    // Check that the two objects returned have the expected keys.
    for (const auto& object: listObjectsResponse.Objects) {
        ASSERT_TRUE(object.Key == object1Key || object.Key == object2Key);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NS3
