#include "yql_yt_fmr_worker_server.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/yql/providers/yt/fmr/worker/interface/yql_yt_fmr_worker.h>

namespace NYql::NFmr {

namespace {

using THandler = std::function<THttpResponse(THttpInput& input)>;

class TReplier: public TRequestReplier {
public:
    TReplier(const THandler& pingHandler)
        : PingHandler_(pingHandler)
    {
    }

    bool DoReply(const TReplyParams& params) override {
        TParsedHttpFull httpRequest(params.Input.FirstLine());
        TStringBuf queryPath = httpRequest.Path;
        queryPath.SkipPrefix("/");

        YQL_CLOG(TRACE, FastMapReduce) << "Received HTTP request: " << httpRequest.Method << " " << httpRequest.Path;
        Cerr << "Received HTTP request: " << httpRequest.Method << " " << httpRequest.Path << "\n";

        if (queryPath == "ping") {
            YQL_ENSURE(httpRequest.Method == "GET");
            auto response = PingHandler_(params.Input);
            YQL_CLOG(TRACE, FastMapReduce) << "Sending ping response with status: " << response.HttpCode();
            params.Output << response;
        } else {
            YQL_CLOG(WARN, FastMapReduce) << "Unknown endpoint requested: " << httpRequest.Path;
            params.Output << THttpResponse(HTTP_NOT_FOUND);
        }
        return true;
    }

private:
    THandler PingHandler_;
};

class TFmrWorkerServer: public THttpServer::ICallBack, public IRunnable {
public:
    TFmrWorkerServer(const TFmrWorkerServerSettings& settings, IFmrWorker::TPtr worker)
        : Port_(settings.Port), Host_(settings.Host), Worker_(worker)
    {
        YQL_CLOG(INFO, FastMapReduce) << "Creating FMR Worker Server with host: " << Host_ << ", port: " << Port_;
        THttpServer::TOptions opts;
        opts.AddBindAddress(Host_, Port_);
        HttpServer_ = MakeHolder<THttpServer>(this, opts.EnableKeepAlive(true).EnableCompression(true));

        PingHandler_ = std::bind(&TFmrWorkerServer::PingHandler, this, std::placeholders::_1);
    }

    void Start() override {
        HttpServer_->Start();
        YQL_CLOG(INFO, FastMapReduce) << "Worker server is listening on url " << "http://" + Host_ + ":" + ToString(Port_);
        Cerr << "Worker server is listening on url " << "http://" + Host_ + ":" + ToString(Port_) << "\n";
    }

    void Stop() override {
        YQL_CLOG(INFO, FastMapReduce) << "Worker server is stopping";
        HttpServer_->Stop();
        YQL_CLOG(INFO, FastMapReduce) << "Worker server stopped";
    }

    ~TFmrWorkerServer() override {
        Stop();
    }

    TClientRequest* CreateClient() override {
        return new TReplier(PingHandler_);
    }

private:
    THandler PingHandler_;
    THolder<THttpServer> HttpServer_;
    const ui16 Port_;
    const TString Host_;
    IFmrWorker::TPtr Worker_;

    THttpResponse PingHandler(THttpInput& /*input*/) {
        const auto& workerState = Worker_->GetWorkerState();

        if (workerState == EFmrWorkerRuntimeState::Running) {
            THttpResponse httpResponse(HTTP_OK);
            httpResponse.SetContent("Ok");
            return httpResponse;
        } else {
            THttpResponse httpResponse(HTTP_SERVICE_UNAVAILABLE);
            httpResponse.SetContent("Stopped");
            return httpResponse;
        }

    }
};

} // namespace

IFmrWorkerServer::TPtr MakeFmrWorkerServer(const TFmrWorkerServerSettings& settings, IFmrWorker::TPtr worker) {
    return MakeHolder<TFmrWorkerServer>(settings, worker);
}

} // namespace NYql::NFmr
