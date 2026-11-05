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
    const std::string Bucket1_ = "test-bucket1";
    const std::string Bucket2_ = "test-bucket2";

    ICredentialsProviderPtr S3CredentialProvider_;
    IThreadPoolPollerPtr Poller_;
    IClientPtr S3Client_;

private:
    void SetUp() override
    {
        ForbidContextSwitchInFutureHandler();

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

            std::vector<std::string> objectKeys;
            for (const auto& object : listObjectsResponse.Objects) {
                objectKeys.push_back(object.Key);
            }
            if (!objectKeys.empty()) {
                auto deleteObjectsResponse = WaitFor(S3Client_->DeleteObjects({
                    .Bucket = bucket.Name,
                    .Objects = std::move(objectKeys),
                }))
                    .ValueOrThrow();
                ASSERT_TRUE(deleteObjectsResponse.Errors.empty());
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
    const std::string object1Key = "foo1";
    const std::string object1Data = "bar1";
    const std::string object2Key = "foo2";
    const std::string object2Data = "bar2";
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

TEST_F(TS3ClientTest, PutAndGetObjectsPreservesSpecialCharactersInKeys)
{
    const std::vector<std::string> objectKeys{
        "А.jpeg",
        "photo 1.jpeg",
        "a?b.jpeg",
        "100%.jpeg",
        "photo%201.jpeg",
        "nested/path.jpeg",
        "фото/А.jpeg",
    };

    for (const auto& key : objectKeys) {
        SCOPED_TRACE(key);
        WaitFor(S3Client_->PutObject({
            .Bucket = Bucket1_,
            .Key = key,
            .Data = TSharedRef::FromString(key),
        }))
            .ValueOrThrow();
    }

    for (const auto& key : objectKeys) {
        SCOPED_TRACE(key);
        auto response = WaitFor(S3Client_->GetObject({
            .Bucket = Bucket1_,
            .Key = key,
        }))
            .ValueOrThrow();
        EXPECT_EQ(std::string(response.Data.ToStringBuf()), key);
    }

    auto listResponse = WaitFor(S3Client_->ListObjects({
        .Bucket = Bucket1_,
    }))
        .ValueOrThrow();
    std::vector<std::string> listedKeys;
    for (const auto& object : listResponse.Objects) {
        listedKeys.push_back(object.Key);
    }
    auto expectedKeys = objectKeys;
    std::sort(listedKeys.begin(), listedKeys.end());
    std::sort(expectedKeys.begin(), expectedKeys.end());
    EXPECT_EQ(listedKeys, expectedKeys);
}

TEST_F(TS3ClientTest, ListObjectsPreservesSpecialCharactersInPrefix)
{
    const std::vector<std::string> prefixes{
        "ampersand&/",
        "question?/",
        "plus+/",
        "кириллица/",
        "nested/path/",
        "percent%20/",
        "space /",
    };

    for (const auto& prefix : prefixes) {
        SCOPED_TRACE(prefix);
        auto key = prefix + "object";
        WaitFor(S3Client_->PutObject({
            .Bucket = Bucket1_,
            .Key = key,
            .Data = TSharedRef::FromString(key),
        }))
            .ValueOrThrow();
    }

    for (const auto& prefix : prefixes) {
        SCOPED_TRACE(prefix);
        auto response = WaitFor(S3Client_->ListObjects({
            .Prefix = prefix,
            .Bucket = Bucket1_,
        }))
            .ValueOrThrow();
        ASSERT_EQ(response.Objects.size(), 1u);
        EXPECT_EQ(response.Objects.front().Key, prefix + "object");
    }
}

TEST_F(TS3ClientTest, DeleteObjectsPreservesSpecialCharactersInKeys)
{
    const std::vector<std::string> objectKeys{
        "a&b",
        "a&amp;b",
        "a<b",
        "a>b",
        "a\"b",
        "a'b",
        "a]]>b",
    };

    for (const auto& key : objectKeys) {
        SCOPED_TRACE(key);
        WaitFor(S3Client_->PutObject({
            .Bucket = Bucket1_,
            .Key = key,
            .Data = TSharedRef::FromString(key),
        }))
            .ValueOrThrow();
    }

    auto listResponse = WaitFor(S3Client_->ListObjects({
        .Bucket = Bucket1_,
    }))
        .ValueOrThrow();
    std::vector<std::string> listedKeys;
    for (const auto& object : listResponse.Objects) {
        listedKeys.push_back(object.Key);
    }
    auto expectedKeys = objectKeys;
    std::sort(listedKeys.begin(), listedKeys.end());
    std::sort(expectedKeys.begin(), expectedKeys.end());
    ASSERT_EQ(listedKeys, expectedKeys);

    auto deleteResponse = WaitFor(S3Client_->DeleteObjects({
        .Bucket = Bucket1_,
        .Objects = objectKeys,
    }))
        .ValueOrThrow();
    ASSERT_TRUE(deleteResponse.Errors.empty());

    auto remainingObjectsResponse = WaitFor(S3Client_->ListObjects({
        .Bucket = Bucket1_,
    }))
        .ValueOrThrow();
    EXPECT_TRUE(remainingObjectsResponse.Objects.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NS3
