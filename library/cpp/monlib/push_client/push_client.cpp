#include "push_client.h"

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/monlib/encode/format.h>
#include <library/cpp/retry/retry.h>
#include <library/cpp/http/misc/http_headers.h>

#include <util/generic/strbuf.h>
#include <util/stream/null.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/string/builder.h>


using namespace NSolomon;

namespace {
    TString MakePath(const TSolomonPushConfig& config) {
        TStringBuilder sb;
        sb << config.Path << TStringBuf("?project=") << config.Project
            << TStringBuf("&service=") << config.Service
            << TStringBuf("&cluster=") << config.Cluster;

        return sb;
    }
} // namespace anonymous

TSolomonPushConfig::TSolomonPushConfig(TString endpoint, IAuthProviderPtr provider)
    : AuthProvider_{provider ? std::move(provider) : CreateFakeAuth()}
{
    TStringBuf server, path;
    SplitUrlToHostAndPath(endpoint, server, path);

    TStringBuf schema, host;
    ui16 port{0};
    GetSchemeHostAndPort(server, schema, host, port);

    if (port == 0 && schema == TStringBuf("https://")) {
        Port = 443;
    } else if (port) {
        Port = port;
    }

    Server = server;
    Path = path;
}

TSolomonPushJsonBuilder::TSolomonPushJsonBuilder(const TSolomonPushConfig& config, EFormat format)
    : TSolomonJsonBuilder{format}
    , Config_{config}
{
    Y_ENSURE(config.Project, "TSolomonPushConfig::Project field is required");
    Y_ENSURE(config.Cluster, "TSolomonPushConfig::Cluster field is required");
    Y_ENSURE(config.Service, "TSolomonPushConfig::Service field is required");
}

void TSolomonPushJsonBuilder::PushToSolomon() {
    Finish();

    TSimpleHttpClient::THeaders headers;
    switch (Format()) {
        case EFormat::LegacyJson:
            break;
        case EFormat::Json:
            headers[NHttpHeaders::CONTENT_TYPE] = NMonitoring::NFormatContenType::JSON;
            break;
        case EFormat::Spack:
            headers[NHttpHeaders::CONTENT_TYPE] = NMonitoring::NFormatContenType::SPACK;
            break;
    };

    Config_.AuthProvider().AddCredentials(headers);

    const auto retryOpts = TRetryOptions().WithCount(Config_.RetryCount).WithSleep(Config_.RetrySleep);
    TStringStream response;
    try {
        auto f = [&] {
            TSimpleHttpClient(Config_.Server, Config_.Port, Config_.TimeoutSocket, Config_.TimeoutConnection)
                .DoPost(MakePath(Config_), GetJsonValue(), &response, std::move(headers));
        };
        std::function<void(const THttpRequestException&)> onFail = [](const THttpRequestException& ex) {
            auto status = ex.GetStatusCode();
            if (status >= 400 && status < 500) {
                throw ex;
            }
        };
        DoWithRetry(f, onFail, retryOpts, true);
    } catch (yexception& e) {
        Cerr << "Failed to push data to solomon: " << e.what() << "\nServer response: " << response.Str()
             << Endl;
    }

    Reset();
}

const IAuthProvider& TSolomonPushConfig::AuthProvider() const {
    return *AuthProvider_;
}

void TSolomonPushJsonBuilder::Finish() {
    if (auto&& host = Config_.Host; !host.Empty()) {
        TSensorPath labels;
        labels.emplace_back(TStringBuf("host"), host);
        WriteCommonLabels(labels);
    }
}
