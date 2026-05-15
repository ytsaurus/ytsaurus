#include "yql_yt_vanilla_http_mon.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYql::NFmr {

namespace {

class TReplier : public TRequestReplier {
public:
    TReplier(IVanillaPeerTracker* tracker)
        : Tracker_(tracker)
    {}

    bool DoReply(const TReplyParams& params) override {
        TParsedHttpFull req(params.Input.FirstLine());
        TStringBuf path = req.Path;

        if (path == "/" || path.empty()) {
            params.Output << MakePeersListResponse();
        } else {
            path.SkipPrefix("/");
            ui64 cookie;
            if (!TryFromString(path, cookie)) {
                params.Output << THttpResponse(HTTP_NOT_FOUND);
                return true;
            }
            if (cookie == Tracker_->GetSelfIndex()) {
                THttpResponse response(HTTP_OK);
                response.SetContentType("text/html");
                response.SetContent(TStringBuilder() << "<h1>I am node #" << cookie << "</h1>\n");
                params.Output << response;
            } else {
                params.Output << THttpResponse(HTTP_NOT_FOUND);
            }
        }
        return true;
    }

private:
    IVanillaPeerTracker* Tracker_;

    THttpResponse MakePeersListResponse() {
        TStringBuilder html;
        html << "<html><body><ul>\n";
        ui64 count = Tracker_->GetPeerCount();
        for (ui64 i = 0; i < count; ++i) {
            TString ip = Tracker_->GetPeerAddress(i);
            html << "<li><a href=\"" << i << "/\">Node #" << i << ": " << ip << "</a></li>\n";
        }
        html << "</ul></body></html>\n";
        THttpResponse response(HTTP_OK);
        response.SetContentType("text/html");
        response.SetContent(html);
        return response;
    }
};

class TVanillaHttpMon : public THttpServer::ICallBack, public IRunnable {
public:
    TVanillaHttpMon(IVanillaPeerTracker* tracker, const TVanillaHttpMonSettings& settings)
        : Tracker_(tracker)
    {
        THttpServer::TOptions opts;
        opts.AddBindAddress(settings.Host, settings.Port);
        HttpServer_ = MakeHolder<THttpServer>(this, opts);
    }

    void Start() override {
        HttpServer_->Start();
    }

    void Stop() override {
        HttpServer_->Stop();
    }

    ~TVanillaHttpMon() override {
        Stop();
    }

    TClientRequest* CreateClient() override {
        return new TReplier(Tracker_);
    }

private:
    IVanillaPeerTracker* Tracker_;
    THolder<THttpServer> HttpServer_;
};

} // namespace

IRunnable::TPtr MakeVanillaHttpMon(
    IVanillaPeerTracker* tracker,
    const TVanillaHttpMonSettings& settings)
{
    return MakeHolder<TVanillaHttpMon>(tracker, settings);
}

} // namespace NYql::NFmr
