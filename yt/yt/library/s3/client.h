#pragma once

#include "public.h"

#include "config.h"
#include "http.h"

#include <yt/yt/core/actions/future.h>

// Forward declaration for Poco::XML::Node.
namespace Poco::XML {

class Node;

} // namespace Poco::XML

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBucketAcl,
    ((Private)           (0) ("private")           )
    ((PublicRead)        (1) ("public-read")       )
    ((PublicWrite)       (2) ("public-read-write") )
    ((AuthenticatedRead) (3) ("authenticated-read"))
);

struct TBucket
{
    TInstant CreationDate;
    std::string Name;

    void Deserialize(const Poco::XML::Node& node);
};

struct TObject
{
    std::string Key;
    TInstant LastModified;
    std::string ETag;
    i64 Size;

    void Deserialize(const Poco::XML::Node& node);
};

struct TOwner
{
    std::string DisplayName;
    std::string Id;

    void Deserialize(const Poco::XML::Node& node);
};

struct TDeleteError
{
    std::string Key;
    std::string Code;
    std::string Message;
};

////////////////////////////////////////////////////////////////////////////////

struct THeadBucketRequest
{
    std::string Bucket;

    void Serialize(THttpRequest* request) const;
};

struct THeadBucketResponse
{
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TListBucketsRequest
{
    void Serialize(THttpRequest* request) const;
};

struct TListBucketsResponse
{
    std::vector<TBucket> Buckets;

    TOwner Owner;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TListObjectsRequest
{
    std::string Prefix;
    std::string Bucket;
    std::optional<std::string> ContinuationToken;
    void Serialize(THttpRequest* request) const;
};

struct TListObjectsResponse
{
    std::optional<std::string> NextContinuationToken;
    std::vector<TObject> Objects;
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TPutBucketRequest
{
    std::string Bucket;
    EBucketAcl Acl = EBucketAcl::Private;

    void Serialize(THttpRequest* request) const;
};

struct TPutBucketResponse
{
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TPutObjectRequest
{
    std::string Bucket;
    std::string Key;

    TSharedRef Data;

    std::optional<std::string> ContentMd5;

    void Serialize(THttpRequest* request) const;
};

struct TPutObjectResponse
{
    std::string ETag;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadPartRequest
{
    std::string Bucket;
    std::string Key;

    std::string UploadId;
    int PartIndex;

    TSharedRef Data;

    std::optional<std::string> ContentMd5;

    void Serialize(THttpRequest* request) const;
};

struct TUploadPartResponse
{
    std::string ETag;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TGetObjectRequest
{
    std::string Bucket;
    std::string Key;
    std::optional<std::string> Range;

    void Serialize(THttpRequest* request) const;
};

struct TGetObjectResponse
{
    TSharedRef Data;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TGetObjectStreamRequest
{
    std::string Bucket;
    std::string Key;
    std::optional<std::string> Range;

    void Serialize(THttpRequest* request) const;
};

struct TGetObjectStreamResponse
{
    NHttp::IResponsePtr Stream;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TDeleteBucketRequest
{
    std::string Bucket;

    void Serialize(THttpRequest* request) const;
};

struct TDeleteBucketResponse
{
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TDeleteObjectRequest
{
    std::string Bucket;
    std::string Object;

    void Serialize(THttpRequest* request) const;
};

struct TDeleteObjectResponse
{
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TDeleteObjectsRequest
{
    std::string Bucket;
    std::vector<std::string> Objects;

    void Serialize(THttpRequest* request) const;
};

struct TDeleteObjectsResponse
{
    std::vector<TDeleteError> Errors;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateMultipartUploadRequest
{
    std::string Bucket;
    std::string Key;

    void Serialize(THttpRequest* request) const;
};

struct TCreateMultipartUploadResponse
{
    std::string Bucket;
    std::string Key;

    std::string UploadId;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortMultipartUploadRequest
{
    std::string Bucket;
    std::string Key;

    std::string UploadId;

    void Serialize(THttpRequest* request) const;
};

struct TAbortMultipartUploadResponse
{
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TCompleteMultipartUploadRequest
{
    std::string Bucket;
    std::string Key;

    std::string UploadId;

    struct TPart
    {
        int PartIndex;

        std::string ETag;
    };
    std::vector<TPart> Parts;

    void Serialize(THttpRequest* request) const;
};

struct TCompleteMultipartUploadResponse
{
    std::string ETag;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct THeadObjectRequest
{
    std::string Bucket;
    std::string Key;

    void Serialize(THttpRequest* request) const;
};

struct THeadObjectResponse
{
    TInstant LastModified;
    std::string ETag;
    i64 Size;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public TRefCounted
{
    //! Must be called before work with client.
    virtual TFuture<void> Start() = 0;

#define DEFINE_COMMAND(Command)                                                                    \
    virtual TFuture<T ## Command ## Response> Command(const T ## Command ## Request& request) = 0; \

    DEFINE_COMMAND(ListBuckets)
    DEFINE_COMMAND(ListObjects)
    DEFINE_COMMAND(PutBucket)
    DEFINE_COMMAND(PutObject)
    DEFINE_COMMAND(UploadPart)
    DEFINE_COMMAND(GetObject)
    DEFINE_COMMAND(GetObjectStream)
    DEFINE_COMMAND(DeleteBucket)
    DEFINE_COMMAND(DeleteObject)
    DEFINE_COMMAND(DeleteObjects)
    DEFINE_COMMAND(CreateMultipartUpload)
    DEFINE_COMMAND(AbortMultipartUpload)
    DEFINE_COMMAND(CompleteMultipartUpload)
    DEFINE_COMMAND(HeadObject)
    DEFINE_COMMAND(HeadBucket)
#undef DEFINE_COMMAND
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    TS3ClientConfigPtr config,
    ICredentialsProviderPtr credentialProvider,
    NCrypto::TSslContextConfigPtr sslContextConfig,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr executionInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
