#include "yql_yt_coordinator_server.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <yql/essentials/utils/log/context.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers/yql_yt_coordinator_proto_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_log_context.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_tvm_helpers.h>

namespace NYql::NFmr {

namespace {

using THandler = std::function<THttpResponse(THttpInput& input)>;

enum class EOperationHandler {
    StartOperation,
    GetOperation,
    DeleteOperation,
    SendHeartbeatResponse,
    GetFmrTableInfo,
    ClearSession,
    DropTables,
    Ping,
    OpenSession,
    PingSession,
    ListSessions,
    PrepareOperation
};

class TReplier: public TRequestReplier {
public:
    TReplier(
        std::unordered_map<EOperationHandler, THandler>& handlers
        , IFmrTvmClient::TPtr tvmClient
        , const std::vector<TTvmId>& allowedSourceTvmIds
    )
        : Handlers_(handlers)
        , TvmClient_(tvmClient)
        , AllowedSourceTvmIds_(allowedSourceTvmIds)
    {
    }

    bool DoReply(const TReplyParams& params) override {
        TParsedHttpFull httpRequest(params.Input.FirstLine());
        auto handlerName = GetHandlerName(httpRequest);
        if (!handlerName) {
            params.Output << THttpResponse(HTTP_NOT_FOUND);
            return true;
        }

        try {
            YQL_ENSURE(Handlers_.contains(*handlerName));
            if (*handlerName == ::EOperationHandler::SendHeartbeatResponse) {
                // for now, just check tvm for worker, for other methods we need to get service tickets from yql gateway.
                CheckTvmServiceTicket(params.Input.Headers(), TvmClient_, AllowedSourceTvmIds_);
            }

            auto callbackFunc = Handlers_[*handlerName];
            params.Output << callbackFunc(params.Input);
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "Error while processing url path " << httpRequest.Path << " is: " << CurrentExceptionMessage();
            THttpResponse response = THttpResponse(HTTP_BAD_REQUEST);
            response.SetContent(CurrentExceptionMessage());
            params.Output << response;
        }
        return true;
    }

private:
    std::unordered_map<EOperationHandler, THandler> Handlers_;
    IFmrTvmClient::TPtr TvmClient_;
    const std::vector<TTvmId> AllowedSourceTvmIds_;

    TMaybe<EOperationHandler> GetHandlerName(TParsedHttpFull httpRequest) {
        TStringBuf queryPath;
        httpRequest.Path.SkipPrefix("/");
        queryPath = httpRequest.Path.NextTok('/');
        if (queryPath == "operation") {
            if (httpRequest.Method == "POST") {
                return EOperationHandler::StartOperation;
            } else if (httpRequest.Method == "GET") {
                return EOperationHandler::GetOperation;
            } else if (httpRequest.Method == "DELETE") {
                return EOperationHandler::DeleteOperation;
            }
            ythrow yexception() << "Unsupported http method";
        } else if (queryPath == "worker_heartbeat") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return EOperationHandler::SendHeartbeatResponse;
        } else if (queryPath == "fmr_table_info") {
            YQL_ENSURE(httpRequest.Method == "GET");
            return EOperationHandler::GetFmrTableInfo;
        } else if (queryPath == "clear_session") {
            return EOperationHandler::ClearSession;
        } else if (queryPath == "drop_tables") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return EOperationHandler::DropTables;
        } else if (queryPath == "ping") {
            return EOperationHandler::Ping;
        } else if (queryPath == "open_session") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return EOperationHandler::OpenSession;
        } else if (queryPath == "ping_session") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return EOperationHandler::PingSession;
        } else if (queryPath == "list_sessions") {
            YQL_ENSURE(httpRequest.Method == "GET");
            return EOperationHandler::ListSessions;
        } else if (queryPath == "prepare_operation") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return EOperationHandler::PrepareOperation;
        }
        return Nothing();
    }
};

class TFmrCoordinatorServer: public THttpServer::ICallBack, public IRunnable {
public:
    TFmrCoordinatorServer(IFmrCoordinator::TPtr coordinator, const TFmrCoordinatorServerSettings& settings, IFmrTvmClient::TPtr tvmClient)
        : Coordinator_(coordinator)
        , Port_(settings.Port)
        , Host_(settings.Host)
        , AllowedSourceTvmIds_(settings.AllowedSourceTvmIds)
        , TvmClient_(tvmClient)
    {
        THttpServer::TOptions opts;
        opts.AddBindAddress(Host_, Port_);
        HttpServer_ = MakeHolder<THttpServer>(this, opts.EnableKeepAlive(true).EnableCompression(true));

        THandler startOperationHandler = std::bind(&TFmrCoordinatorServer::StartOperationHandler, this, std::placeholders::_1);
        THandler getOperationHandler = std::bind(&TFmrCoordinatorServer::GetOperationHandler, this, std::placeholders::_1);
        THandler deleteOperationHandler = std::bind(&TFmrCoordinatorServer::DeleteOperationHandler, this, std::placeholders::_1);
        THandler sendHeartbeatResponseHandler = std::bind(&TFmrCoordinatorServer::SendHeartbeatResponseHandler, this, std::placeholders::_1);
        THandler getFmrTableInfoHandler = std::bind(&TFmrCoordinatorServer::GetFmrTableInfoHandler, this, std::placeholders::_1);
        THandler clearSessionHandler = std::bind(&TFmrCoordinatorServer::ClearSessionHandler, this, std::placeholders::_1);
        THandler dropTablesHandler = std::bind(&TFmrCoordinatorServer::DropTablesHandler, this, std::placeholders::_1);
        THandler pingHandler = std::bind(&TFmrCoordinatorServer::PingHandler, this, std::placeholders::_1);
        THandler openSessionHandler = std::bind(&TFmrCoordinatorServer::OpenSessionHandler, this, std::placeholders::_1);
        THandler pingSessionHandler = std::bind(&TFmrCoordinatorServer::PingSessionHandler, this, std::placeholders::_1);
        THandler listSessionsHandler = std::bind(&TFmrCoordinatorServer::ListSessionsHandler, this, std::placeholders::_1);
        THandler PrepareOperationHandler = std::bind(&TFmrCoordinatorServer::PrepareOperationHandler, this, std::placeholders::_1);

        Handlers_ = std::unordered_map<EOperationHandler, THandler>{
            {EOperationHandler::StartOperation, startOperationHandler},
            {EOperationHandler::GetOperation, getOperationHandler},
            {EOperationHandler::DeleteOperation, deleteOperationHandler},
            {EOperationHandler::SendHeartbeatResponse, sendHeartbeatResponseHandler},
            {EOperationHandler::GetFmrTableInfo, getFmrTableInfoHandler},
            {EOperationHandler::ClearSession, clearSessionHandler},
            {EOperationHandler::DropTables, dropTablesHandler},
            {EOperationHandler::Ping, pingHandler},
            {EOperationHandler::OpenSession, openSessionHandler},
            {EOperationHandler::PingSession, pingSessionHandler},
            {EOperationHandler::ListSessions, listSessionsHandler},
            {EOperationHandler::PrepareOperation, PrepareOperationHandler}
        };
    }

    void Start() override {
        HttpServer_->Start();
        Cerr << "Coordinator server is listnening on url " <<  "http://" + Host_ + ":" + ToString(Port_) << "\n";
    }

    void Stop() override {
        HttpServer_->Stop();
    }

    ~TFmrCoordinatorServer() override {
        Stop();
    }

    TClientRequest* CreateClient() override {
       return new TReplier(Handlers_, TvmClient_, AllowedSourceTvmIds_);
    }

private:
    std::unordered_map<EOperationHandler, THandler> Handlers_;
    IFmrCoordinator::TPtr Coordinator_;
    THolder<THttpServer> HttpServer_;
    const ui16 Port_;
    const TString Host_;
    const std::vector<TTvmId> AllowedSourceTvmIds_;
    IFmrTvmClient::TPtr TvmClient_;

    THttpResponse StartOperationHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        NProto::TStartOperationRequest protoStartOperationRequest;
        YQL_ENSURE(protoStartOperationRequest.ParseFromString(input.ReadAll()));
        auto startOperationResponse = Coordinator_->StartOperation(StartOperationRequestFromProto(protoStartOperationRequest)).GetValueSync();
        auto protoStartOperationResponse = StartOperationResponseToProto(startOperationResponse);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoStartOperationResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse GetOperationHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        TParsedHttpFull httpRequest(input.FirstLine());
        httpRequest.Path.SkipPrefix("/operation/");
        TGetOperationRequest getOperationRequest{.OperationId = ToString(httpRequest.Path)};

        auto getOperationResponse = Coordinator_->GetOperation(getOperationRequest).GetValueSync();
        auto protoGetOperationResponse = GetOperationResponseToProto(getOperationResponse);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoGetOperationResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse DeleteOperationHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        TParsedHttpFull httpRequest(input.FirstLine());
        httpRequest.Path.SkipPrefix("/operation/");
        TDeleteOperationRequest deleteOperationRequest{.OperationId = ToString(httpRequest.Path)};

        auto deleteOperationResponse = Coordinator_->DeleteOperation(deleteOperationRequest).GetValueSync();
        auto protoDeleteOperationResponse = DeleteOperationResponseToProto(deleteOperationResponse);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoDeleteOperationResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse SendHeartbeatResponseHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        NProto::THeartbeatRequest protoHeartbeatRequest;
        YQL_ENSURE(protoHeartbeatRequest.ParseFromString(input.ReadAll()));

        auto sendHeartbeatResponse = Coordinator_->SendHeartbeatResponse(HeartbeatRequestFromProto(protoHeartbeatRequest)).GetValueSync();
        auto protoSendHeartbeatResponse = HeartbeatResponseToProto(sendHeartbeatResponse);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoSendHeartbeatResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse GetFmrTableInfoHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        NProto::TGetFmrTableInfoRequest protoGetFmrTableInfoRequest;
        YQL_ENSURE(protoGetFmrTableInfoRequest.ParseFromString(input.ReadAll()));

        auto fmrTableInfoResponse = Coordinator_->GetFmrTableInfo(GetFmrTableInfoRequestFromProto(protoGetFmrTableInfoRequest)).GetValueSync();
        auto protoFmrTableInfoResponse = GetFmrTableInfoResponseToProto(fmrTableInfoResponse);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoFmrTableInfoResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse ClearSessionHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));

        NProto::TClearSessionRequest protoClearSessionRequest;
        YQL_ENSURE(protoClearSessionRequest.ParseFromString(input.ReadAll()));

        Coordinator_->ClearSession(ClearSessionRequestFromProto(protoClearSessionRequest)).GetValueSync();
        THttpResponse httpResponse(HTTP_OK);
        return httpResponse;
    }

    THttpResponse DropTablesHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));

        NProto::TDropTablesRequest protoRequest;
        YQL_ENSURE(protoRequest.ParseFromString(input.ReadAll()));

        auto response = Coordinator_->DropTables(
            DropTablesRequestFromProto(protoRequest)
        ).GetValueSync();

        NProto::TDropTablesResponse protoResponse = DropTablesResponseToProto(response);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse PingHandler(THttpInput& /*input*/) {
        THttpResponse httpResponse(HTTP_OK);
        return httpResponse;
    }

    THttpResponse OpenSessionHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        NProto::TOpenSessionRequest protoRequest;
        YQL_ENSURE(protoRequest.ParseFromString(input.ReadAll()));

        auto response = Coordinator_->OpenSession(OpenSessionRequestFromProto(protoRequest)).GetValueSync();
        auto protoResponse = OpenSessionResponseToProto(response);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse PingSessionHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        NProto::TPingSessionRequest protoRequest;
        YQL_ENSURE(protoRequest.ParseFromString(input.ReadAll()));

        auto response = Coordinator_->PingSession(PingSessionRequestFromProto(protoRequest)).GetValueSync();
        auto protoResponse = PingSessionResponseToProto(response);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse ListSessionsHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        NProto::TListSessionsRequest protoRequest;
        YQL_ENSURE(protoRequest.ParseFromString(input.ReadAll()));

        auto response = Coordinator_->ListSessions(ListSessionsRequestFromProto(protoRequest)).GetValueSync();
        auto protoResponse = ListSessionsResponseToProto(response);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoResponse.SerializeAsString());
        return httpResponse;
    }

    THttpResponse PrepareOperationHandler(THttpInput& input) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetLogContext(input));
        NProto::TPrepareOperationRequest protoRequest;
        YQL_ENSURE(protoRequest.ParseFromString(input.ReadAll()));

        auto response = Coordinator_->PrepareOperation(PrepareOperationRequestFromProto(protoRequest)).GetValueSync();
        auto protoResponse = PrepareOperationResponseToProto(response);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoResponse.SerializeAsString());
        return httpResponse;
    }
};

} // namespace

IFmrServer::TPtr MakeFmrCoordinatorServer(
    IFmrCoordinator::TPtr coordinator,
    const TFmrCoordinatorServerSettings& settings,
    IFmrTvmClient::TPtr tvmClient
) {
    return MakeHolder<TFmrCoordinatorServer>(coordinator, settings, tvmClient);
}

} // namespace NYql::NFmr
