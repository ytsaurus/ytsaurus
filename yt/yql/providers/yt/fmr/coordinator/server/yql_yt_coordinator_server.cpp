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
    PrepareOperation,
    WaitForOperations,
    WaitForTasks,
};

// Holds the result of a WaitForOperations long-poll for the second DoReply pass.
struct TWaitForOperationsPendingRequest: public TThrRefBase {
    using TPtr = TIntrusivePtr<TWaitForOperationsPendingRequest>;
    TMaybe<TWaitForOperationsResponse> Response;
    std::exception_ptr Error;
    TRequestReplier* Replier = nullptr;
};

// Holds the result of a WaitForTasks long-poll for the second DoReply pass.
struct TWaitForTasksPendingRequest: public TThrRefBase {
    using TPtr = TIntrusivePtr<TWaitForTasksPendingRequest>;
    TMaybe<TWaitForTasksResponse> Response;
    std::exception_ptr Error;
    TRequestReplier* Replier = nullptr;
};

class TReplier: public TRequestReplier {
public:
    TReplier(
        std::unordered_map<EOperationHandler, THandler>& handlers
        , IFmrTvmClient::TPtr tvmClient
        , const std::vector<TTvmId>& allowedSourceTvmIds
        , IFmrCoordinator::TPtr coordinator
    )
        : Handlers_(handlers)
        , TvmClient_(tvmClient)
        , AllowedSourceTvmIds_(allowedSourceTvmIds)
        , Coordinator_(coordinator)
    {
    }

    bool DoReply(const TReplyParams& params) override {
        // Second pass: long-poll result is ready.
        if (WaitOpPending_) {
            try {
                if (WaitOpPending_->Error) {
                    std::rethrow_exception(WaitOpPending_->Error);
                }
                YQL_ENSURE(WaitOpPending_->Response.Defined());
                auto protoResponse = WaitForOperationsResponseToProto(*WaitOpPending_->Response);
                THttpResponse httpResponse(HTTP_OK);
                httpResponse.SetContentType("application/x-protobuf");
                httpResponse.SetContent(protoResponse.SerializeAsString());
                params.Output << httpResponse;
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << "WaitForOperations reply error: " << CurrentExceptionMessage();
                THttpResponse response(HTTP_INTERNAL_SERVER_ERROR);
                response.SetContent(CurrentExceptionMessage());
                params.Output << response;
            }
            return true;
        }

        if (WaitTasksPending_) {
            try {
                if (WaitTasksPending_->Error) {
                    std::rethrow_exception(WaitTasksPending_->Error);
                }
                YQL_ENSURE(WaitTasksPending_->Response.Defined());
                auto protoResponse = WaitForTasksResponseToProto(*WaitTasksPending_->Response);
                THttpResponse httpResponse(HTTP_OK);
                httpResponse.SetContentType("application/x-protobuf");
                httpResponse.SetContent(protoResponse.SerializeAsString());
                params.Output << httpResponse;
            } catch (...) {
                YQL_CLOG(ERROR, FastMapReduce) << "WaitForTasks reply error: " << CurrentExceptionMessage();
                THttpResponse response(HTTP_INTERNAL_SERVER_ERROR);
                response.SetContent(CurrentExceptionMessage());
                params.Output << response;
            }
            return true;
        }

        TParsedHttpFull httpRequest(params.Input.FirstLine());
        auto handlerName = GetHandlerName(httpRequest);
        if (!handlerName) {
            params.Output << THttpResponse(HTTP_NOT_FOUND);
            return true;
        }

        if (*handlerName == EOperationHandler::WaitForOperations) {
            return HandleWaitForOperations(params);
        }

        if (*handlerName == EOperationHandler::WaitForTasks) {
            return HandleWaitForTasks(params);
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
            THttpResponse response = THttpResponse(HTTP_INTERNAL_SERVER_ERROR);
            response.SetContent(CurrentExceptionMessage());
            params.Output << response;
        }
        return true;
    }

private:
    std::unordered_map<EOperationHandler, THandler> Handlers_;
    IFmrTvmClient::TPtr TvmClient_;
    const std::vector<TTvmId> AllowedSourceTvmIds_;
    IFmrCoordinator::TPtr Coordinator_;
    TWaitForOperationsPendingRequest::TPtr WaitOpPending_;
    TWaitForTasksPendingRequest::TPtr WaitTasksPending_;

    bool HandleWaitForOperations(const TReplyParams& params) {
        NProto::TWaitForOperationsRequest protoRequest;
        try {
            YQL_ENSURE(protoRequest.ParseFromString(params.Input.ReadAll()));
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "WaitForOperations parse error: " << CurrentExceptionMessage();
            THttpResponse response(HTTP_BAD_REQUEST);
            response.SetContent(CurrentExceptionMessage());
            params.Output << response;
            return true;
        }

        WaitOpPending_ = MakeIntrusive<TWaitForOperationsPendingRequest>();
        WaitOpPending_->Replier = this;
        auto pending = WaitOpPending_;
        Coordinator_->WaitForOperations(WaitForOperationsRequestFromProto(protoRequest))
            .Subscribe([pending](const NThreading::TFuture<TWaitForOperationsResponse>& f) {
                try {
                    pending->Response = f.GetValue();
                } catch (...) {
                    pending->Error = std::current_exception();
                    YQL_CLOG(ERROR, FastMapReduce) << "WaitForOperations error: " << CurrentExceptionMessage();
                }
                static_cast<IObjectInQueue*>(pending->Replier)->Process(nullptr);
            });
        // Tell the HTTP framework not to destroy this object yet.
        return false;
    }

    bool HandleWaitForTasks(const TReplyParams& params) {
        NProto::TWaitForTasksRequest protoRequest;
        try {
            YQL_ENSURE(protoRequest.ParseFromString(params.Input.ReadAll()));
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "WaitForTasks parse error: " << CurrentExceptionMessage();
            THttpResponse response(HTTP_BAD_REQUEST);
            response.SetContent(CurrentExceptionMessage());
            params.Output << response;
            return true;
        }

        WaitTasksPending_ = MakeIntrusive<TWaitForTasksPendingRequest>();
        WaitTasksPending_->Replier = this;
        auto pending = WaitTasksPending_;
        Coordinator_->WaitForTasks(WaitForTasksRequestFromProto(protoRequest))
            .Subscribe([pending](const NThreading::TFuture<TWaitForTasksResponse>& f) {
                try {
                    pending->Response = f.GetValue();
                } catch (...) {
                    pending->Error = std::current_exception();
                    YQL_CLOG(ERROR, FastMapReduce) << "WaitForTasks error: " << CurrentExceptionMessage();
                }
                static_cast<IObjectInQueue*>(pending->Replier)->Process(nullptr);
            });
        // Tell the HTTP framework not to destroy this object yet.
        return false;
    }

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
        } else if (queryPath == "wait_for_operations") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return EOperationHandler::WaitForOperations;
        } else if (queryPath == "wait_for_tasks") {
            YQL_ENSURE(httpRequest.Method == "POST");
            return EOperationHandler::WaitForTasks;
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

        THandler startOperationHandler = std::bind_front(&TFmrCoordinatorServer::StartOperationHandler, this);
        THandler getOperationHandler = std::bind_front(&TFmrCoordinatorServer::GetOperationHandler, this);
        THandler deleteOperationHandler = std::bind_front(&TFmrCoordinatorServer::DeleteOperationHandler, this);
        THandler sendHeartbeatResponseHandler = std::bind_front(&TFmrCoordinatorServer::SendHeartbeatResponseHandler, this);
        THandler getFmrTableInfoHandler = std::bind_front(&TFmrCoordinatorServer::GetFmrTableInfoHandler, this);
        THandler clearSessionHandler = std::bind_front(&TFmrCoordinatorServer::ClearSessionHandler, this);
        THandler dropTablesHandler = std::bind_front(&TFmrCoordinatorServer::DropTablesHandler, this);
        THandler pingHandler = std::bind_front(&TFmrCoordinatorServer::PingHandler, this);
        THandler openSessionHandler = std::bind_front(&TFmrCoordinatorServer::OpenSessionHandler, this);
        THandler pingSessionHandler = std::bind_front(&TFmrCoordinatorServer::PingSessionHandler, this);
        THandler listSessionsHandler = std::bind_front(&TFmrCoordinatorServer::ListSessionsHandler, this);
        THandler PrepareOperationHandler = std::bind_front(&TFmrCoordinatorServer::PrepareOperationHandler, this);

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
        YQL_CLOG(INFO, FastMapReduce) << "Coordinator server is listnening on url " <<  "http://" + Host_ + ":" + ToString(Port_);
    }

    void Stop() override {
        HttpServer_->Stop();
    }

    ~TFmrCoordinatorServer() override {
        Stop();
    }

    TClientRequest* CreateClient() override {
       return new TReplier(Handlers_, TvmClient_, AllowedSourceTvmIds_, Coordinator_);
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
