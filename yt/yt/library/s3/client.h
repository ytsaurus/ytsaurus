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
    TString Name;

    void Deserialize(const Poco::XML::Node& node);
};

struct TObject
{
    TString Key;
    TInstant LastModified;
    TString ETag;
    i64 Size;

    void Deserialize(const Poco::XML::Node& node);
};

struct TOwner
{
    TString DisplayName;
    TString Id;

    void Deserialize(const Poco::XML::Node& node);
};

struct TDeleteError
{
    TString Key;
    TString Code;
    TString Message;
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
    TString Prefix;
    TString Bucket;
    std::optional<TString> ContinuationToken;
    void Serialize(THttpRequest* request) const;
};

struct TListObjectsResponse
{
    std::optional<TString> NextContinuationToken;
    std::vector<TObject> Objects;
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TPutBucketRequest
{
    TString Bucket;
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
    TString Bucket;
    TString Key;

    TSharedRef Data;

    std::optional<TString> ContentMd5;

    void Serialize(THttpRequest* request) const;
};

struct TPutObjectResponse
{
    TString ETag;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TUploadPartRequest
{
    TString Bucket;
    TString Key;

    TString UploadId;
    int PartIndex;

    TSharedRef Data;

    std::optional<TString> ContentMd5;

    void Serialize(THttpRequest* request) const;
};

struct TUploadPartResponse
{
    TString ETag;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TGetObjectRequest
{
    TString Bucket;
    TString Key;
    std::optional<TString> Range;

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
    TString Bucket;
    TString Key;
    std::optional<TString> Range;

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
    TString Bucket;

    void Serialize(THttpRequest* request) const;
};

struct TDeleteBucketResponse
{
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TDeleteObjectRequest
{
    TString Bucket;
    TString Object;

    void Serialize(THttpRequest* request) const;
};

struct TDeleteObjectResponse
{
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TDeleteObjectsRequest
{
    TString Bucket;
    std::vector<TString> Objects;

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
    TString Bucket;
    TString Key;

    void Serialize(THttpRequest* request) const;
};

struct TCreateMultipartUploadResponse
{
    TString Bucket;
    TString Key;

    TString UploadId;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortMultipartUploadRequest
{
    TString Bucket;
    TString Key;

    TString UploadId;

    void Serialize(THttpRequest* request) const;
};

struct TAbortMultipartUploadResponse
{
    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct TCompleteMultipartUploadRequest
{
    TString Bucket;
    TString Key;

    TString UploadId;

    struct TPart
    {
        int PartIndex;

        TString ETag;
    };
    std::vector<TPart> Parts;

    void Serialize(THttpRequest* request) const;
};

struct TCompleteMultipartUploadResponse
{
    TString ETag;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct THeadObjectRequest
{
    TString Bucket;
    TString Key;

    void Serialize(THttpRequest* request) const;
};

struct THeadObjectResponse
{
    TInstant LastModified;
    TString ETag;
    i64 Size;

    void Deserialize(const NHttp::IResponsePtr& response);
};

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public TRefCounted
{
    // TODO(achulkov2): [PForReview] Remove this method from public interface.
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
#undef DEFINE_COMMAND
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    TS3ClientConfigPtr config,
    ICredentialsProviderPtr credentialProvider,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr executionInvoker);

////////////////////////////////////////////////////////////////////////////////

//! Wraps client with retries according to provided backoff options and retry checker.
IClientPtr CreateRetryingClient(
    IClientPtr underlyingClient,
    TExponentialBackoffOptions backoffOptions,
    IInvokerPtr executionInvoker,
    TCallback<bool(const TError&)> retryChecker = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
