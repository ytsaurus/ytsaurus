#include "yql_yt_vanilla_http_mon.h"

#include <yql/essentials/utils/rand_guid.h>

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>

#include <library/cpp/json/json_writer.h>

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
                params.Output << MakeSelfResponse();
            } else {
                params.Output << THttpResponse(HTTP_NOT_FOUND);
            }
        }
        return true;
    }

private:
    IVanillaPeerTracker* Tracker_;
    TRandGuid RandGuid_;

    TString MakeRandomETag() {
        return "W/" + RandGuid_.GenGuid();
    }

    THttpResponse MakePeersListResponse() {
        TStringStream stream;
        NJson::TJsonWriter writer(&stream, false);
        writer.OpenMap();
        writer.WriteKey("nodes");
        writer.OpenArray();
        ui64 count = Tracker_->GetPeerCount();
        for (ui64 i = 0; i < count; ++i) {
            TString ip = Tracker_->GetPeerAddress(i);
            writer.OpenMap();
            writer.WriteKey("index");
            writer.Write(i);
            writer.WriteKey("ip");
            writer.Write(ip);
            writer.CloseMap();
        }

        writer.CloseArray();
        writer.CloseMap();
        writer.Flush();
        THttpResponse response(HTTP_OK);
        response.SetContentType("application/json");
        response.SetContent(stream.Str());
        response.AddHeader("ETag", MakeRandomETag());
        return response;
    }

    THttpResponse MakeSelfResponse() {
        TStringStream stream;
        NJson::TJsonWriter writer(&stream, false);
        writer.OpenMap();
        writer.WriteKey("self_index");
        writer.Write(Tracker_->GetSelfIndex());
        writer.CloseMap();
        writer.Flush();

        THttpResponse response(HTTP_OK);
        response.SetContentType("application/json");
        response.SetContent(stream.Str());
        response.AddHeader("ETag", MakeRandomETag());
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
