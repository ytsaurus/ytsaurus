#include "client.h"

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/net/address.h>

#include <library/cpp/string_utils/base64/base64.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

NXml::TDocument ParseXmlDocument(TSharedRef responseBody)
{
    TString responseString(responseBody.ToStringBuf());
    return NXml::TDocument(
        responseString,
        NXml::TDocument::Source::String);
}

////////////////////////////////////////////////////////////////////////////////

void TBucket::Deserialize(NXml::TNode node)
{
    CreationDate = TInstant::ParseIso8601(node.FirstChild("CreationDate").Value<TString>());
    Name = node.FirstChild("Name").Value<TString>();
}

void TObject::Deserialize(NXml::TNode node)
{
    Key = node.FirstChild("Key").Value<TString>();
    LastModified = TInstant::ParseIso8601(node.FirstChild("LastModified").Value<TString>());
    ETag = node.FirstChild("ETag").Value<TString>();
    Size = node.FirstChild("Size").Value<ui64>();
}

void TOwner::Deserialize(NXml::TNode node)
{
    DisplayName = node.FirstChild("DisplayName").Value<TString>();
    Id = node.FirstChild("ID").Value<TString>();
}

////////////////////////////////////////////////////////////////////////////////

void TListBucketsRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Get;
    request->Path = "/";
}

void TListBucketsResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    auto document = ParseXmlDocument(response->ReadAll());
    auto node = document.Root();
    for (
        auto bucket = node.FirstChild("Buckets").FirstChild();
        !bucket.IsNull();
        bucket = bucket.NextSibling())
    {
        Buckets.emplace_back().Deserialize(bucket);
    }
    Owner.Deserialize(node.FirstChild("Owner"));
}

////////////////////////////////////////////////////////////////////////////////

void TListObjectsRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Get;
    request->Path = Format("/%v", Bucket);
    request->Query["list-type"] = "2";
    request->Query["prefix"] = Prefix;
    if (ContinuationToken) {
        request->Query["continuation-token"] = *ContinuationToken;
    }
}

void TListObjectsResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    auto document = ParseXmlDocument(response->ReadAll());
    auto node = document.Root();
    for (
        auto object = node.FirstChild("Contents");
        !object.IsNull();
        object = object.NextSibling())
    {
        Objects.emplace_back().Deserialize(object);
    }
    if (auto nextToken = node.FirstChild("NextContinuationToken"); !nextToken.IsNull()) {
        NextContinuationToken = nextToken.Value<TString>();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPutBucketRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Put;
    request->Path = Format("/%v", Bucket);
    request->Headers["Content-Length"] = "0";
}

void TPutBucketResponse::Deserialize(const NHttp::IResponsePtr& /*response*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TPutObjectRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Put;
    request->Path = Format("/%v/%v", Bucket, Key);
    request->Payload = Data;
}

void TPutObjectResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    ETag = response->GetHeaders()->GetOrThrow("ETag");
}

////////////////////////////////////////////////////////////////////////////////

void TUploadPartRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Put;
    request->Path = Format("/%v/%v", Bucket, Key);
    request->Query["partNumber"] = ToString(PartIndex);
    request->Query["uploadId"] = UploadId;
    request->Payload = Data;
}

void TUploadPartResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    ETag = response->GetHeaders()->GetOrThrow("ETag");
}

////////////////////////////////////////////////////////////////////////////////

void TGetObjectRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Get;
    request->Path = Format("/%v/%v", Bucket, Key);
}

void TGetObjectResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    Data = response->ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

void TGetObjectStreamRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Get;
    request->Path = Format("/%v/%v", Bucket, Key);
}

void TGetObjectStreamResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    Stream = std::move(response);
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteObjectsRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Post;
    request->Path = "/";
    request->Query["delete"] = "";
    TStringStream bodyStream;
    bodyStream << R"(<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">)";
    for (const auto& key : Objects) {
        bodyStream << "<Object>";
        bodyStream << "<Key>" << key << "</Key>";
        bodyStream << "</Object>";
    }
    bodyStream << "</Delete>";
    request->Payload = TSharedRef::FromString(bodyStream.Str());

    auto contentHash = NCrypto::TSha256Hasher().Append(request->Payload.ToStringBuf()).GetDigest();
    request->Headers["x-amz-checksum-sha256"] = Base64Encode({contentHash.data(), contentHash.size()});
}

void TDeleteObjectsResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    auto document = ParseXmlDocument(response->ReadAll());
    auto node = document.Root();
    TString errors;
    for (auto error = node.FirstChild("Error");
        !error.IsNull();
        error = error.NextSibling())
    {
        errors += Format("%v: %v\n",
            error.FirstChild("Key").Value<TString>(),
            error.FirstChild("Message").Value<TString>());
    }

    if (errors) {
        THROW_ERROR_EXCEPTION(EErrorCode::S3ApiError, errors);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TCreateMultipartUploadRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Post;
    request->Path = Format("/%v/%v", Bucket, Key);
    request->Query["uploads"] = "";
}

void TCreateMultipartUploadResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    auto document = ParseXmlDocument(response->ReadAll());
    auto node = document.Root();

    Bucket = node.FirstChild("Bucket").Value<TString>();
    Key = node.FirstChild("Key").Value<TString>();
    UploadId = node.FirstChild("UploadId").Value<TString>();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortMultipartUploadRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Delete;
    request->Path = Format("/%v/%v", Bucket, Key);
    request->Query["uploadId"] = UploadId;
}

void TAbortMultipartUploadResponse::Deserialize(const NHttp::IResponsePtr& /*response*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TCompleteMultipartUploadRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Post;
    request->Path = Format("/%v/%v", Bucket, Key);
    request->Query["uploadId"] = UploadId;

    TStringStream bodyStream;
    bodyStream << "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
    for (const auto& part : Parts) {
        bodyStream << "<Part>";
        bodyStream << "<PartNumber>" << part.PartIndex << "</PartNumber>";
        bodyStream << "        <ETag>" << part.ETag << "</ETag>\n";
        bodyStream << "</Part>";
    }
    bodyStream << "</CompleteMultipartUpload>";
    request->Payload = TSharedRef::FromString(bodyStream.Str());
}

void TCompleteMultipartUploadResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    auto document = ParseXmlDocument(response->ReadAll());

    ETag = document.Root().FirstChild("ETag").Value<TString>();
}

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(
        TS3ClientConfigPtr config,
        IPollerPtr poller,
        IInvokerPtr executionInvoker)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , ExecutionInvoker_(std::move(executionInvoker))
    { }

    TFuture<void> Start() override
    {
        auto urlRef = NHttp::ParseUrl(Config_->Url);
        auto asyncAddress = TAddressResolver::Get()->Resolve(TString{urlRef.Host});
        return asyncAddress.Apply(BIND([=, this, this_ = MakeStrong(this)] (const TNetworkAddress& address) {
            BaseHttpRequest_ = THttpRequest{
                .Protocol = TString{urlRef.Protocol},
                .Host = TString{urlRef.Host},
                .Port = urlRef.Port,
                .Region = Config_->Region,
                .Service = "s3",
            };

            bool useTls = (urlRef.Protocol == "https");
            TNetworkAddress s3Address(
                address,
                urlRef.Port.value_or(useTls ? 443 : 80));

            Client_ = CreateHttpClient(
                Config_,
                s3Address,
                useTls,
                Poller_,
                ExecutionInvoker_);
            return Client_->Start();
        }));
    }

#define DEFINE_STRUCTURED_COMMAND(Command)                                                              \
    TFuture<T ## Command ## Response> Command(const T ## Command ## Request& request) override          \
    {                                                                                                   \
        return SendCommand<T ## Command ## Response>(request);                                          \
    }

    DEFINE_STRUCTURED_COMMAND(ListBuckets)
    DEFINE_STRUCTURED_COMMAND(ListObjects)
    DEFINE_STRUCTURED_COMMAND(PutBucket)
    DEFINE_STRUCTURED_COMMAND(PutObject)
    DEFINE_STRUCTURED_COMMAND(UploadPart)
    DEFINE_STRUCTURED_COMMAND(GetObject)
    DEFINE_STRUCTURED_COMMAND(GetObjectStream)
    DEFINE_STRUCTURED_COMMAND(DeleteObjects)
    DEFINE_STRUCTURED_COMMAND(CreateMultipartUpload)
    DEFINE_STRUCTURED_COMMAND(AbortMultipartUpload)
    DEFINE_STRUCTURED_COMMAND(CompleteMultipartUpload)
#undef DEFINE_STRUCTURED_COMMAND

private:
    static TError ErrorFromResponse(NHttp::IResponsePtr response)
    {
        auto statusCode = response->GetStatusCode();
        auto error = NYT::TError(
            "Got status code %v %v",
            ToUnderlying(statusCode),
            ToHttpString(statusCode));
        auto responseBody = response->ReadAll();
        try {
            const auto xml = ParseXmlDocument(responseBody);
            for (auto node = xml.Root().FirstChild(); !node.IsNull(); node = node.NextSibling()) {
                error <<= TErrorAttribute(node.Name(), node.Value<TString>());
            }
        } catch (const std::exception&) {
            error <<= TErrorAttribute("response_body", responseBody.ToStringBuf());
            auto headers = response->GetHeaders();
            for (TStringBuf header : {"x-amz-request-id", "x-amz-id-2"}) {
                if (auto* value = headers->Find(header)) {
                    error <<= TErrorAttribute(TString(header), *value);
                }
            }
        }
        return error;
    }

    template <class TCommandResponse, class TCommandRequest>
    TFuture<TCommandResponse> SendCommand(const TCommandRequest& request)
    {
        auto req = BaseHttpRequest_;
        request.Serialize(&req);

        return BIND([this, this_ = MakeStrong(this)] (THttpRequest req) {
            PrepareHttpRequest(
                &req,
                Config_->AccessKeyId,
                Config_->SecretAccessKey);

            return Client_->MakeRequest(std::move(req))
                .ApplyUnique(BIND([] (TErrorOr<NHttp::IResponsePtr>&& responseOrError) -> TErrorOr<TCommandResponse> {
                    if (!responseOrError.IsOK()) {
                        return TError("HTTP request failed") << std::move(responseOrError);
                    }
                    auto& response = responseOrError.Value();
                    // 3xx are not really errors but we don't handle redirects anyways
                    if (ToUnderlying(response->GetStatusCode()) >= 300) {
                        return ErrorFromResponse(response);
                    }
                    TCommandResponse rsp;
                    rsp.Deserialize(response);
                    return rsp;
                }));
        })
            .AsyncVia(ExecutionInvoker_)
            .Run(std::move(req));
    }

    TS3ClientConfigPtr Config_;
    TNetworkAddress S3Address_;

    IPollerPtr Poller_;
    IInvokerPtr ExecutionInvoker_;

    IHttpClientPtr Client_;

    THttpRequest BaseHttpRequest_;
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    TS3ClientConfigPtr config,
    IPollerPtr poller,
    IInvokerPtr executionInvoker)
{
    return New<TClient>(
        std::move(config),
        std::move(poller),
        std::move(executionInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
