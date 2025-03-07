#include "yql_yt_coordinator_server.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers/yql_yt_coordinator_proto_helpers.h>

namespace NYql::NFmr {

namespace {

using THandler = std::function<THttpResponse(THttpInput& input)>;

enum class EOperationHandler {
    StartOperation,
    GetOperation,
    DeleteOperation,
    SendHeartbeatResponse,
    GetFmrTableInfo
};

class TReplier: public TRequestReplier {
public:
    TReplier(std::unordered_map<EOperationHandler, THandler>& handlers)
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
    std::unordered_map<EOperationHandler, THandler> Handlers_;

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
        }
        return Nothing();
    }
};

class TFmrCoordinatorServer: public THttpServer::ICallBack, public IRunnable {
public:
    TFmrCoordinatorServer(IFmrCoordinator::TPtr coordinator, const TFmrCoordinatorServerSettings& settings)
        : Coordinator_(coordinator), Port_(settings.Port), Host_(settings.Host)
    {
        THttpServer::TOptions opts;
        opts.AddBindAddress(Host_, Port_);
        HttpServer_ = MakeHolder<THttpServer>(this, opts.EnableKeepAlive(true).EnableCompression(true));

        THandler startOperationHandler = std::bind(&TFmrCoordinatorServer::StartOperationHandler, this, std::placeholders::_1);
        THandler getOperationHandler = std::bind(&TFmrCoordinatorServer::GetOperationHandler, this, std::placeholders::_1);
        THandler deleteOperationHandler = std::bind(&TFmrCoordinatorServer::DeleteOperationHandler, this, std::placeholders::_1);
        THandler sendHeartbeatResponseHandler = std::bind(&TFmrCoordinatorServer::SendHeartbeatResponseHandler, this, std::placeholders::_1);
        THandler getFmrTableInfoHandler = std::bind(&TFmrCoordinatorServer::GetFmrTableInfoHandler, this, std::placeholders::_1);


        Handlers_ = std::unordered_map<EOperationHandler, THandler>{
            {EOperationHandler::StartOperation, startOperationHandler},
            {EOperationHandler::GetOperation, getOperationHandler},
            {EOperationHandler::DeleteOperation, deleteOperationHandler},
            {EOperationHandler::SendHeartbeatResponse, sendHeartbeatResponseHandler},
            {EOperationHandler::GetFmrTableInfo, getFmrTableInfoHandler}
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
        return new TReplier(Handlers_);
    }

private:
    std::unordered_map<EOperationHandler, THandler> Handlers_;
    IFmrCoordinator::TPtr Coordinator_;
    THolder<THttpServer> HttpServer_;
    const ui16 Port_;
    const TString Host_;

    THttpResponse StartOperationHandler(THttpInput& input) {
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
        NProto::TGetFmrTableInfoRequest protoGetFmrTableInfoRequest;
        YQL_ENSURE(protoGetFmrTableInfoRequest.ParseFromString(input.ReadAll()));

        auto fmrTableInfoResponse = Coordinator_->GetFmrTableInfo(GetFmrTableInfoRequestFromProto(protoGetFmrTableInfoRequest)).GetValueSync();
        auto protoFmrTableInfoResponse = GetFmrTableInfoResponseToProto(fmrTableInfoResponse);

        THttpResponse httpResponse(HTTP_OK);
        httpResponse.SetContentType("application/x-protobuf");
        httpResponse.SetContent(protoFmrTableInfoResponse.SerializeAsString());
        return httpResponse;
    }
};

} // namespace

IFmrServer::TPtr MakeFmrCoordinatorServer(IFmrCoordinator::TPtr coordinator, const TFmrCoordinatorServerSettings& settings) {
    return MakeHolder<TFmrCoordinatorServer>(coordinator, settings);
}

} // namespace NYql::NFmr
