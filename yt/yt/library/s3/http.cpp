#include "http.h"

#include "crypto_helpers.h"
#include "private.h"

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/crypto/tls.h>

#include <yt/yt/core/http/stream.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/dialer.h>

namespace NYT::NS3 {

using namespace NConcurrency;
using namespace NHttp;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

void PrepareHttpRequest(
    THttpRequest* request,
    ICredentialsProviderPtr credentialProvider,
    TInstant requestTime)
{
    TStringStream canonicalRequestStream;

    // HTTP method.
    {
        canonicalRequestStream << ToHttpString(request->Method);
        canonicalRequestStream << '\n';
    }

    // Canonical URI.
    {
        auto encodedPath = NCrypto::UriEncode(request->Path, /*isObjectPath*/ true);
        if (encodedPath.empty() || encodedPath[0] != '/') {
            canonicalRequestStream << '/';
        }
        canonicalRequestStream << encodedPath;
        canonicalRequestStream << '\n';
    }

    // Canonical query string.
    {
        std::vector<std::pair<TString, TString>> queryParts;
        queryParts.reserve(request->Query.size());

        for (const auto& [key, value] : request->Query) {
            queryParts.emplace_back(
                NCrypto::UriEncode(key, /*isObjectPath*/ false),
                NCrypto::UriEncode(value, /*isObjectPath*/ false));
        }

        Sort(queryParts);

        auto first = true;
        for (const auto& [key, value] : queryParts) {
            if (first) {
                first = false;
            } else {
                canonicalRequestStream << '&';
            }
            canonicalRequestStream << key << '=' << value;
        }

        canonicalRequestStream << '\n';
    }

    auto contentSha256 = [&] {
        if (auto payload = request->Payload) {
            return NCrypto::Sha256HashHex(payload);
        } else {
            const static auto EmptyPayloadHash = NCrypto::Sha256HashHex("");
            return EmptyPayloadHash;
        }
    }();

    TStringStream signedHeadersStream;

    // Canonical headers.
    {
        std::vector<std::pair<TString, TString>> headers;
        headers.reserve(request->Headers.size() + 2);

        constexpr auto HostHeaderName = "Host";
        constexpr auto ContentLengthHeaderName = "Content-Length";
        constexpr auto XAmzDateHeaderName = "x-amz-date";
        constexpr auto XAmzContentSha256 = "x-amz-content-sha256";

        bool hasHostHeader = false;
        bool hasContentLengthHeader = false;
        bool hasXAmzDateHeader = false;
        bool hasXAmzContentSha256Header = false;
        auto addHeader = [&] (const TString& key, const TString& value, bool newHeader = false) {
            auto lowerKey = NCrypto::Lowercase(key);
            if (lowerKey == NCrypto::Lowercase(HostHeaderName)) {
                hasHostHeader = true;
            } else if (lowerKey == NCrypto::Lowercase(ContentLengthHeaderName)) {
                hasContentLengthHeader = true;
            } else if (lowerKey == NCrypto::Lowercase(XAmzDateHeaderName)) {
                hasXAmzDateHeader = true;
            } else if (lowerKey == NCrypto::Lowercase(XAmzContentSha256)) {
                hasXAmzContentSha256Header = true;
            }

            headers.emplace_back(lowerKey, NCrypto::Trim(value));
            if (newHeader) {
                EmplaceOrCrash(request->Headers, key, value);
            }
        };
        for (const auto& [key, value] : request->Headers) {
            addHeader(key, value);
        }

        if (!hasHostHeader) {
            auto host = request->Host;
            if (auto port = request->Port) {
                host = Format("%v:%v", request->Host, *port);
            }
            addHeader(TString(HostHeaderName), host, /*newHeader*/ true);
        }
        if (!hasContentLengthHeader && request->Payload) {
            EmplaceOrCrash(
                request->Headers,
                TString(ContentLengthHeaderName),
                ToString(request->Payload.size()));
        }
        if (!hasXAmzDateHeader) {
            addHeader(
                TString(XAmzDateHeaderName),
                NCrypto::FormatTimeIso8601(requestTime),
                /*newHeader*/ true);
        }
        if (!hasXAmzContentSha256Header) {
            addHeader(
                TString(XAmzContentSha256),
                contentSha256,
                /*newHeader*/ true);
        }

        Sort(headers);

        auto first = true;
        for (const auto& [key, value] : headers) {
            canonicalRequestStream << key << ':' << value << '\n';

            if (first) {
                first = false;
            } else {
                signedHeadersStream << ';';
            }
            signedHeadersStream << key;
        }

        canonicalRequestStream << '\n';
    }

    auto signedHeaders = signedHeadersStream.Str();

    // Signed headers.
    {
        canonicalRequestStream << signedHeaders << '\n';
    }

    // Hashed payload.
    {
        canonicalRequestStream << contentSha256;
    }

    auto credentials = credentialProvider->GetCredentials();

    if (!credentials.SessionToken.empty()) {
        auto authorizationHeader = Format("AWS TVM2:%v:%v", credentials.AccessKeyId, credentials.SecretAccessKey);
        constexpr auto AuthorizationHeaderName = "Authorization";
        EmplaceOrCrash(request->Headers, AuthorizationHeaderName, authorizationHeader);

        constexpr auto ServiceTicketHeaderName = "X-Ya-Service-Ticket";
        EmplaceOrCrash(request->Headers, ServiceTicketHeaderName, std::move(credentials.SessionToken));
    } else if (!credentials.SecretAccessKey.empty() && !credentials.AccessKeyId.empty()) {
        auto canonicalRequest = canonicalRequestStream.Str();
        auto time = NCrypto::FormatTimeIso8601(requestTime);
        auto date = TStringBuf(time).substr(0, "YYYYMMDD"sv.size());
        auto scope = Format("%v/%v/%v/aws4_request",
            date,
            request->Region,
            request->Service);
        auto stringToSign = Format("AWS4-HMAC-SHA256\n%v\n%v\n%v",
            time,
            scope,
            NCrypto::Sha256HashHex(canonicalRequest));

        const static TString Aws4 = "AWS4";
        const static TString Aws4Request = "aws4_request";

        auto dateKey = NCrypto::HmacSha256(Aws4 + credentials.SecretAccessKey, date);
        auto dateRegionKey = NCrypto::HmacSha256(dateKey, request->Region);
        auto dateRegionServiceKey = NCrypto::HmacSha256(dateRegionKey, request->Service);
        auto signingKey = NCrypto::HmacSha256(dateRegionServiceKey, Aws4Request);
        auto signature = NCrypto::Hex(NCrypto::HmacSha256(signingKey, stringToSign));
        auto authorizationHeader = Format("AWS4-HMAC-SHA256 Credential=%v/%v,SignedHeaders=%v,Signature=%v",
            credentials.AccessKeyId,
            scope,
            signedHeaders,
            signature);
        constexpr auto AuthorizationHeaderName = "Authorization";
        EmplaceOrCrash(request->Headers, AuthorizationHeaderName, authorizationHeader);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

NYT::NCrypto::TSslContextPtr CreateSslContext()
{
    auto sslContext = New<NYT::NCrypto::TSslContext>();
    sslContext->Commit();
    return sslContext;
}

} // namespace

class THttpClient
    : public IHttpClient
{
public:
    THttpClient(
        NHttp::TClientConfigPtr config,
        TNetworkAddress address,
        bool useTls,
        IPollerPtr poller,
        IInvokerPtr invoker)
        : Config_(config)
        , Address_(std::move(address))
        , Dialer_(useTls
            ? CreateSslContext()->CreateDialer(
                config->Dialer,
                std::move(poller),
                S3Logger())
            : CreateDialer(
                config->Dialer,
                std::move(poller),
                S3Logger()))
        , Invoker_(std::move(invoker))
    { }

    TFuture<void> Start() override
    {
        return VoidFuture;
    }

    TFuture<NHttp::IResponsePtr> MakeRequest(THttpRequest request) override
    {
        return BIND(&THttpClient::DoMakeRequest, MakeStrong(this), request)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    const NHttp::TClientConfigPtr Config_;
    const TNetworkAddress Address_;

    const IDialerPtr Dialer_;

    const IInvokerPtr Invoker_;

    NHttp::IResponsePtr DoMakeRequest(THttpRequest request)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto connection = WaitFor(Dialer_->Dial(Address_))
            .ValueOrThrow();

        auto input = New<THttpInput>(
            connection,
            Address_,
            Invoker_,
            EMessageType::Response,
            Config_);

        auto headers = FormHeaders(request);
        std::vector<TSharedRef> writeRefs({
            headers,
            request.Payload,
        });
        WaitFor(connection->WriteV(TSharedRefArray(std::move(writeRefs), TSharedRefArray::TMoveParts{})))
            .ThrowOnError();

        return input;
    }

    TSharedRef FormHeaders(const THttpRequest& request)
    {
        TStringStream stream;
        stream << ToHttpString(request.Method) << " " << request.Path;
        {
            auto first = true;
            for (const auto& [key, value] : request.Query) {
                if (first) {
                    stream << "?";
                    first = false;
                } else {
                    stream << "&";
                }
                stream << key;
                stream << "=";
                stream << value;
            }
            stream << " HTTP/1.1\r\n";
        }

        for (const auto& [key, value] : request.Headers) {
            stream << key << ": " << value << "\r\n";
        }
        stream << "\r\n";

        return TSharedRef::FromString(stream.Str());
    }
};

////////////////////////////////////////////////////////////////////////////////

IHttpClientPtr CreateHttpClient(
    NHttp::TClientConfigPtr config,
    NNet::TNetworkAddress address,
    bool useTls,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr invoker)
{
    return New<THttpClient>(
        std::move(config),
        std::move(address),
        useTls,
        std::move(poller),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
