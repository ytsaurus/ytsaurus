#include "yql_yt_table_data_service_server.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <util/string/split.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_log_context.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

using THandler = std::function<THttpResponse(THttpInput& input)>;

enum class ETableDataServiceRequestHandler {
    Put,
    Get,
    Delete
};

class TReplier: public TRequestReplier {
public:
    TReplier(std::unordered_map<ETableDataServiceRequestHandler, THandler>& handlers)
        : Handlers_(handlers)
    {
    }

    bool DoReply(const TReplyParams& params) override {
        TParsedHttpFull httpRequest(params.Input.FirstLine());
        auto handlerName = GetHandlerName(httpRequest);
        if (!handlerName) {
            params.Output << THttpResponse(HTTP_NOT_FOUND);
        } else {
            YQL_ENSURE(Handlers_.contains(*handlerName));
            auto callbackFunc = Handlers_[*handlerName];
            params.Output << callbackFunc(params.Input);
        }
        return true;
    }

private:
    std::unordered_map<ETableDataServiceRequestHandler, THandler> Handlers_;

    TMaybe<ETableDataServiceRequestHandler> GetHandlerName(TParsedHttpFull httpRequest) {
        if (httpRequest.Method == "POST") {
            return ETableDataServiceRequestHandler::Put;
        } else if (httpRequest.Method == "GET") {
            return ETableDataServiceRequestHandler::Get;
        } else if (httpRequest.Method == "DELETE") {
            return ETableDataServiceRequestHandler::Delete;
        }
        return Nothing();
    }
};

class TTableDataServiceServer: public THttpServer::ICallBack, public IRunnable {
public:
    TTableDataServiceServer(const TTableDataServiceServerSettings& settings)
        :Host_(settings.Host), Port_(settings.Port), WorkerId_(settings.WorkerId), WorkersNum_(settings.WorkersNum)
    {
        YQL_ENSURE(WorkerId_ >= 0 && WorkerId_ < WorkersNum_);
        THttpServer::TOptions opts;
        opts.AddBindAddress(Host_, Port_);
        HttpServer_ = MakeHolder<THttpServer>(this, opts.EnableKeepAlive(true).EnableCompression(true));

        THandler putTableDataServiceHandler = std::bind(&TTableDataServiceServer::PutTableDataServiceHandler, this, std::placeholders::_1);
        THandler getTableDataServiceHandler = std::bind(&TTableDataServiceServer::GetTableDataServiceHandler, this, std::placeholders::_1);
        THandler deleteTableDataServiceHandler = std::bind(&TTableDataServiceServer::DeleteTableDataServiceHandler, this, std::placeholders::_1);

        Handlers_ = std::unordered_map<ETableDataServiceRequestHandler, THandler>{
            {ETableDataServiceRequestHandler::Put, putTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Get, getTableDataServiceHandler},
            {ETableDataServiceRequestHandler::Delete, deleteTableDataServiceHandler}
        };
    }

    void Start() override {
        HttpServer_->Start();
        Cerr << "Table data service server with id " << WorkerId_ << " is listnening on url " <<  "http://" + Host_ + ":" + ToString(Port_) << "\n";
    }

    void Stop() override {
        HttpServer_->Stop();
    }

    ~TTableDataServiceServer() override {
        Stop();
    }

    TClientRequest* CreateClient() override {
        return new TReplier(Handlers_);
    }

private:
    std::unordered_map<ETableDataServiceRequestHandler, THandler> Handlers_;
    std::unordered_map<TString, TString> Data_;
    THolder<THttpServer> HttpServer_;
    const TString Host_;
    const ui16 Port_;
    const ui64 WorkerId_;
    const ui64 WorkersNum_;

    TString GetTableServiceId(THttpInput& input) {
        TParsedHttpFull httpRequest(input.FirstLine());
        TStringBuf url = httpRequest.Request;
        std::vector<TString> splittedUrl;
        TString delim = "?id=";
        StringSplitter(url).SplitByString(delim).AddTo(&splittedUrl);
        YQL_ENSURE(splittedUrl.size() == 2);
        return splittedUrl[1];
    }

    THttpResponse PutTableDataServiceHandler(THttpInput& input) {
        auto context = GetLogContext(input);
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        TString YsonTableConent = input.ReadAll();
        auto tableDataServiceId = GetTableServiceId(input);
        YQL_ENSURE(std::hash<TString>()(tableDataServiceId) % WorkersNum_ == WorkerId_);
        Data_[tableDataServiceId] = YsonTableConent;
        YQL_CLOG(DEBUG, FastMapReduce) << "Putting content in table data service with id " << tableDataServiceId;
        return THttpResponse(HTTP_OK);
    }

    THttpResponse GetTableDataServiceHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        auto tableDataServiceId = GetTableServiceId(input);
        YQL_ENSURE(std::hash<TString>()(tableDataServiceId) % WorkersNum_ == WorkerId_);
        TString ysonTableContent{};
        if (Data_.contains(tableDataServiceId)) {
            ysonTableContent = Data_[tableDataServiceId];
        }
        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContent(ysonTableContent);
        YQL_CLOG(DEBUG, FastMapReduce) << "Getting content in table data service with id " << tableDataServiceId;
        return httpResponse;
    }

    THttpResponse DeleteTableDataServiceHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        auto tableDataServiceId = GetTableServiceId(input);
        YQL_ENSURE(std::hash<TString>()(tableDataServiceId) % WorkersNum_ == WorkerId_);
        if (Data_.contains(tableDataServiceId)) {
            Data_.erase(tableDataServiceId);
        }
        YQL_CLOG(DEBUG, FastMapReduce) << "Deleting content in table data service with id " << tableDataServiceId;
        return THttpResponse(HTTP_OK);
    }
};

} // namespace

IFmrServer::TPtr MakeTableDataServiceServer(const TTableDataServiceServerSettings& settings) {
    return MakeHolder<TTableDataServiceServer>(settings);
}

} // NYql::NFmr
