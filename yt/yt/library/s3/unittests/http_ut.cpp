#include <gtest/gtest.h>

#include <yt/yt/library/s3/http.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NS3 {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const static TString S3TutorialAccessKeyId = "AKIAIOSFODNN7EXAMPLE";
const static TString S3TutorialSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

////////////////////////////////////////////////////////////////////////////////

TEST(TRequestPreparerTest, Test1)
{
    auto time = TInstant::ParseIso8601("20130524T000000Z");
    THttpRequest request{
        .Method = NHttp::EMethod::Get,
        .Protocol = "http",
        .Host = "examplebucket.s3.amazonaws.com",
        .Path = "/test.txt",
        .Region = "us-east-1",
        .Service = "s3",
        .Headers = {{"range", "bytes=0-9"}, {"x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
    };

    PrepareHttpRequest(
        &request,
        S3TutorialAccessKeyId,
        S3TutorialSecretAccessKey,
        time);

    EXPECT_EQ(
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
        request.Headers["Authorization"]);
}

TEST(TRequestPreparerTest, Test2)
{
    auto time = TInstant::ParseIso8601("20130524T000000Z");
    auto payload = TSharedRef::FromString("Welcome to Amazon S3.");
    THttpRequest request{
        .Method = NHttp::EMethod::Put,
        .Protocol = "http",
        .Host = "examplebucket.s3.amazonaws.com",
        .Path = "/test$file.text",
        .Region = "us-east-1",
        .Service = "s3",
        .Headers = {
            {"Date", "Fri, 24 May 2013 00:00:00 GMT"},
            {"x-amz-storage-class", "REDUCED_REDUNDANCY"},
            {"x-amz-content-sha256", "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"}},
        .Payload = payload,
    };

    PrepareHttpRequest(
        &request,
        S3TutorialAccessKeyId,
        S3TutorialSecretAccessKey,
        time);

    EXPECT_EQ(
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
        request.Headers["Authorization"]);
}

TEST(TRequestPreparerTest, Test3)
{
    auto time = TInstant::ParseIso8601("20130524T000000Z");
    THttpRequest request{
        .Method = NHttp::EMethod::Get,
        .Protocol = "http",
        .Host = "examplebucket.s3.amazonaws.com",
        .Region = "us-east-1",
        .Service = "s3",
        .Query = {{"lifecycle", ""}},
        .Headers = {
            {"x-amz-date", "20130524T000000Z"},
            {"x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
    };

    PrepareHttpRequest(
        &request,
        S3TutorialAccessKeyId,
        S3TutorialSecretAccessKey,
        time);

    EXPECT_EQ(
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
        request.Headers["Authorization"]);
}

TEST(TRequestPreparerTest, Test4)
{
    auto time = TInstant::ParseIso8601("20130524T000000Z");
    THttpRequest request{
        .Method = NHttp::EMethod::Get,
        .Protocol = "http",
        .Host = "examplebucket.s3.amazonaws.com",
        .Path = "/",
        .Region = "us-east-1",
        .Service = "s3",
        .Query = {{"max-keys", "2"}, {"prefix", "J"}},
        .Headers = {
            {"x-amz-date", "20130524T000000Z"},
            {"x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
    };

    PrepareHttpRequest(
        &request,
        S3TutorialAccessKeyId,
        S3TutorialSecretAccessKey,
        time);

    EXPECT_EQ(
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
        request.Headers["Authorization"]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NS3
