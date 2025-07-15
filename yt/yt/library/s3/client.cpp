#include "client.h"

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/net/address.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <contrib/libs/poco/XML/include/Poco/XML/XML.h>
#include <contrib/libs/poco/XML/include/Poco/DOM/AutoPtr.h>
#include <contrib/libs/poco/XML/include/Poco/DOM/DOMParser.h>
#include <contrib/libs/poco/XML/include/Poco/DOM/Document.h>
#include <contrib/libs/poco/XML/include/Poco/DOM/Node.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NNet;

using TPocoXmlDocumentPtr = Poco::XML::AutoPtr<Poco::XML::Document>;

////////////////////////////////////////////////////////////////////////////////

Poco::XML::Node* GetRootNode(TPocoXmlDocumentPtr document)
{
    for (auto* child = document->firstChild(); child != nullptr; child = child->nextSibling()) {
        // Skip the comment nodes.
        if (child->nodeType() == Poco::XML::Node::ELEMENT_NODE) {
            return child;
        }
    }
    THROW_ERROR_EXCEPTION("The document does not have a non-comment root node");
}

struct TXmlNodeHolder
{
    Poco::XML::Node& operator*()
    {
        return *Node;
    }

    const Poco::XML::Node& operator*() const
    {
        return *Node;
    }

    Poco::XML::Node* operator->()
    {
        return Node;
    }

    const Poco::XML::Node* operator->() const
    {
        return Node;
    }

    //! Parsed document; this pointer also manages the lifetime.
    TPocoXmlDocumentPtr Document;

    //! Pointer to one of the nodes (by default a root node) of this document.
    Poco::XML::Node* Node;
};

TXmlNodeHolder ParseXmlDocument(TSharedRef responseBody)
{
    std::string responseString(responseBody.ToStringBuf());
    TPocoXmlDocumentPtr parsedDocument = Poco::XML::DOMParser{}.parseString(responseString);
    return {
        .Document = parsedDocument,
        .Node = GetRootNode(parsedDocument),
    };
}

Poco::XML::Node* GetChildByName(const Poco::XML::Node& node, const std::string& childName)
{
    for (auto* child = node.firstChild(); child != nullptr; child = child->nextSibling()) {
        if (child->nodeName() == childName) {
            return child;
        }
    }
    THROW_ERROR_EXCEPTION("Child with name %Qv not found in node %Qv", childName, node.nodeName());
}

Poco::XML::Node* TryGetChildByName(const Poco::XML::Node& node, const std::string& childName)
{
    try {
        return GetChildByName(node, childName);
    } catch (const TErrorException&) {
        return nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TBucket::Deserialize(const Poco::XML::Node& node)
{
    CreationDate = TInstant::ParseIso8601(GetChildByName(node, "CreationDate")->innerText());
    Name = GetChildByName(node, "Name")->innerText();
}

void TObject::Deserialize(const Poco::XML::Node& node)
{
    Key = GetChildByName(node, "Key")->innerText();
    LastModified = TInstant::ParseIso8601(GetChildByName(node, "LastModified")->innerText());
    ETag = GetChildByName(node, "ETag")->innerText();
    Size = FromString<ui64>(GetChildByName(node, "Size")->innerText());
}

void TOwner::Deserialize(const Poco::XML::Node& node)
{
    DisplayName = GetChildByName(node, "DisplayName")->innerText();
    Id = GetChildByName(node, "ID")->innerText();
}

////////////////////////////////////////////////////////////////////////////////

void TListBucketsRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Get;
    request->Path = "/";
}

void TListBucketsResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    auto parsedDocument = ParseXmlDocument(response->ReadAll());
    auto bucketsNode = GetChildByName(*parsedDocument, "Buckets");
    for (auto* child = bucketsNode->firstChild(); child != nullptr; child = child->nextSibling()) {
        Buckets.emplace_back().Deserialize(*child);
    }
    Owner.Deserialize(*GetChildByName(*parsedDocument, "Owner"));
}

////////////////////////////////////////////////////////////////////////////////

void TListObjectsRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Get;
    request->Path = Format("/%v", Bucket);
    request->Query["list-type"] = "2";
    if (!Prefix.empty()) {
        request->Query["prefix"] = Prefix;
    }
    if (ContinuationToken) {
        request->Query["continuation-token"] = *ContinuationToken;
    }
}

void TListObjectsResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    auto parsedDocument = ParseXmlDocument(response->ReadAll());
    for (auto* child = parsedDocument->firstChild(); child != nullptr; child = child->nextSibling()) {
        if (child->nodeName() != "Contents") {
            continue;
        }
        Objects.emplace_back().Deserialize(*child);
    }
    if (auto nextToken = TryGetChildByName(*parsedDocument, "NextContinuationToken")) {
        NextContinuationToken = nextToken->innerText();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPutBucketRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Put;
    request->Path = Format("/%v", Bucket);
    request->Headers["Content-Length"] = "0";
    request->Headers["X-Amz-Acl"] = ToString(Acl);
}

void TPutBucketResponse::Deserialize(const NHttp::IResponsePtr& /*response*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TPutObjectRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Put;
    request->Path = Format("/%v/%v", Bucket, Key);
    request->Payload = Data;
    if (ContentMd5) {
        request->Headers["content-md5"] = *ContentMd5;
    }
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
    if (ContentMd5) {
        request->Headers["content-md5"] = *ContentMd5;
    }
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
    if (Range) {
        request->Headers["range"] = *Range;
    }
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
    if (Range) {
        request->Headers["range"] = *Range;
    }
}

void TGetObjectStreamResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    Stream = std::move(response);
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteBucketRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Delete;
    request->Path = Format("/%v", Bucket);
}

void TDeleteBucketResponse::Deserialize(const NHttp::IResponsePtr& /*response*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDeleteObjectRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Delete;
    request->Path = Format("/%v/%v", Bucket, Object);
}

void TDeleteObjectResponse::Deserialize(const NHttp::IResponsePtr& /*response*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDeleteObjectsRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Post;
    request->Path = Format("/%v", Bucket);
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
    auto parsedDocument = ParseXmlDocument(response->ReadAll());
    for (auto* errorNode = TryGetChildByName(*parsedDocument, "Error");
        errorNode != nullptr;
        errorNode = errorNode->nextSibling())
    {
        Errors.emplace_back(TDeleteError{
            .Key = TString(GetChildByName(*errorNode, "Key")->innerText()),
            .Code = TString(GetChildByName(*errorNode, "Code")->innerText()),
            .Message = TString(GetChildByName(*errorNode, "Message")->innerText()),
        });
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
    auto parsedDocument = ParseXmlDocument(response->ReadAll());
    Bucket = GetChildByName(*parsedDocument, "Bucket")->innerText();
    Key = GetChildByName(*parsedDocument, "Key")->innerText();
    UploadId = GetChildByName(*parsedDocument, "UploadId")->innerText();
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
    auto parsedDocument = ParseXmlDocument(response->ReadAll());

    ETag = GetChildByName(*parsedDocument, "ETag")->innerText();
}

////////////////////////////////////////////////////////////////////////////////

void THeadObjectRequest::Serialize(THttpRequest* request) const
{
    request->Method = NHttp::EMethod::Head;
    request->Path = Format("/%v/%v", Bucket, Key);
}

void THeadObjectResponse::Deserialize(const NHttp::IResponsePtr& response)
{
    LastModified = TInstant::ParseRfc822(response->GetHeaders()->GetOrThrow("Last-Modified"));
    ETag = response->GetHeaders()->GetOrThrow("ETag");
    Size = FromString<ui64>(response->GetHeaders()->GetOrThrow("Content-Length"));
}

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(
        TS3ClientConfigPtr config,
        ICredentialsProviderPtr credentialProvider,
        IPollerPtr poller,
        IInvokerPtr executionInvoker)
        : Config_(std::move(config))
        , CredentialProvider_(std::move(credentialProvider))
        , Poller_(std::move(poller))
        , ExecutionInvoker_(std::move(executionInvoker))
    { }

    TFuture<void> Start() override
    {
        auto urlRef = NHttp::ParseUrl(Config_->Url);
        BaseHttpRequest_ = THttpRequest{
            .Protocol = TString{urlRef.Protocol},
            .Host = TString{urlRef.Host},
            .Port = urlRef.Port,
            .Region = Config_->Region,
            .Service = "s3",
        };

        // If working with S3 proxy, THttpRequest must be filled using Config_->Url. But the connection is established with Config_->ProxyUrl.
        if (Config_->ProxyUrl) {
            urlRef = NHttp::ParseUrl(*Config_->ProxyUrl);
        }

        auto asyncAddress = TAddressResolver::Get()->Resolve(TString{urlRef.Host});
        return asyncAddress.Apply(BIND([=, this, this_ = MakeStrong(this)] (const TNetworkAddress& address) {

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
    DEFINE_STRUCTURED_COMMAND(DeleteBucket)
    DEFINE_STRUCTURED_COMMAND(DeleteObject)
    DEFINE_STRUCTURED_COMMAND(DeleteObjects)
    DEFINE_STRUCTURED_COMMAND(CreateMultipartUpload)
    DEFINE_STRUCTURED_COMMAND(AbortMultipartUpload)
    DEFINE_STRUCTURED_COMMAND(CompleteMultipartUpload)
    DEFINE_STRUCTURED_COMMAND(HeadObject)
#undef DEFINE_STRUCTURED_COMMAND

private:
    static TError ErrorFromResponse(NHttp::IResponsePtr response)
    {
        auto statusCode = response->GetStatusCode();
        auto error = TError(
            "Got status code %v %v",
            ToUnderlying(statusCode),
            ToHttpString(statusCode));
        error <<= TErrorAttribute("http_code", statusCode);
        auto responseBody = response->ReadAll();
        try {
            auto parsedDocument = ParseXmlDocument(responseBody);
            for (auto* child = parsedDocument->firstChild(); child != nullptr; child = child->nextSibling()) {
                error <<= TErrorAttribute(child->nodeName(), child->innerText());
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
                CredentialProvider_);

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
    ICredentialsProviderPtr CredentialProvider_;
    TNetworkAddress S3Address_;

    IPollerPtr Poller_;
    IInvokerPtr ExecutionInvoker_;

    IHttpClientPtr Client_;

    THttpRequest BaseHttpRequest_;
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    TS3ClientConfigPtr config,
    ICredentialsProviderPtr credentialProvider,
    IPollerPtr poller,
    IInvokerPtr executionInvoker)
{
    return New<TClient>(
        std::move(config),
        std::move(credentialProvider),
        std::move(poller),
        std::move(executionInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
