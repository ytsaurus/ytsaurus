#include "client.h"

#include <yt/yt/core/net/address.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

NXml::TDocument ParseXmlDocument(const NHttp::IResponsePtr& response)
{
    auto responseBody = response->ReadAll();
    TString responseString(responseBody.Begin(), responseBody.End());
    if (ToUnderlying(response->GetStatusCode()) >= 400) {
        THROW_ERROR_EXCEPTION("S3 replied with error status code %v", response->GetStatusCode())
            << TErrorAttribute("body", responseString);
    }
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
    auto document = ParseXmlDocument(response);
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

void TPutObjectRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Put;
    request->Path = Format("/%v/%v", Bucket, Key);
    request->Payload = Data;
}

void TPutObjectResponse::Deserialize(const NHttp::IResponsePtr& /*response*/)
{ }

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

void TCreateMultipartUploadRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Post;
    request->Path = Format("/%v/%v", Bucket, Key);
    request->Query["uploads"] = "";
}

void TCreateMultipartUploadResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    auto document = ParseXmlDocument(response);
    auto node = document.Root();

    Bucket = node.FirstChild("Bucket").Value<TString>();
    Key = node.FirstChild("Key").Value<TString>();
    UploadId = node.FirstChild("UploadId").Value<TString>();
}

////////////////////////////////////////////////////////////////////////////////

void TAbortMultipartUploadRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Post;
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

void TCompleteMultipartUploadResponse::Deserialize(const NHttp::IResponsePtr& /*response*/)
{ }

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
        auto req = BaseHttpRequest_;                                                                    \
        request.Serialize(&req);                                                                        \
                                                                                                        \
        PrepareHttpRequest(                                                                             \
            &req,                                                                                       \
            Config_->AccessKeyId,                                                                       \
            Config_->SecretAccessKey);                                                                  \
                                                                                                        \
        auto response = WaitFor(Client_->MakeRequest(req))                                              \
            .ValueOrThrow();                                                                            \
        T ## Command ## Response rsp;                                                                   \
        rsp.Deserialize(response);                                                                      \
                                                                                                        \
        return MakeFuture<T ## Command ## Response>(rsp);                                               \
    }

    DEFINE_STRUCTURED_COMMAND(ListBuckets)
    DEFINE_STRUCTURED_COMMAND(PutObject)
    DEFINE_STRUCTURED_COMMAND(UploadPart)
    DEFINE_STRUCTURED_COMMAND(GetObject)
    DEFINE_STRUCTURED_COMMAND(CreateMultipartUpload)
    DEFINE_STRUCTURED_COMMAND(AbortMultipartUpload)
    DEFINE_STRUCTURED_COMMAND(CompleteMultipartUpload)
#undef DEFINE_STRUCTURED_COMMAND

private:
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
