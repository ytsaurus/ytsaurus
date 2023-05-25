#pragma once

#include "public.h"

#include "config.h"
#include "http.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/xml/document/xml-document.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

struct TBucket
{
    TInstant CreationDate;
    TString Name;

    void Deserialize(NXml::TNode node);
};

struct TOwner
{
    TString DisplayName;
    TString Id;

    void Deserialize(NXml::TNode node);
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

struct TPutObjectRequest
{
    TString Bucket;
    TString Key;

    TSharedRef Data;

    void Serialize(THttpRequest* request) const;
};

struct TPutObjectResponse
{
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

    void Serialize(THttpRequest* request) const;
};

struct TGetObjectResponse
{
    TSharedRef Data;

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
    DEFINE_COMMAND(PutObject)
    DEFINE_COMMAND(UploadPart)
    DEFINE_COMMAND(GetObject)
    DEFINE_COMMAND(CreateMultipartUpload)
    DEFINE_COMMAND(AbortMultipartUpload)
    DEFINE_COMMAND(CompleteMultipartUpload)
#undef DEFINE_COMMAND
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    TS3ConnectionConfigPtr config,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr executionInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
