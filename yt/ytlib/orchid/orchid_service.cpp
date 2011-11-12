#include "stdafx.h"
#include "orchid_service.h"

#include "../ytree/yson_reader.h"
#include "../ytree/yson_writer.h"
//#include "../ytree/ypath.h"

namespace NYT {
namespace NOrchid {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TOrchidService::TOrchidService(
    NYTree::INode* root,
    NRpc::IServer* server,
    IInvoker* invoker)
    : NRpc::TServiceBase(
        invoker,
        TOrchidServiceProxy::GetServiceName(),
        OrchidLogger.GetCategory())
    , Root(root)
{
    YASSERT(root != NULL);
    YASSERT(server != NULL);

    //RegisterMethod(RPC_SERVICE_METHOD_DESC(Get));
    //RegisterMethod(RPC_SERVICE_METHOD_DESC(Set));
    //RegisterMethod(RPC_SERVICE_METHOD_DESC(Remove));

    server->RegisterService(this);
}

////////////////////////////////////////////////////////////////////////////////

//RPC_SERVICE_METHOD_IMPL(TOrchidService, Get)
//{
//    Stroka path = request->GetPath();
//
//    context->SetRequestInfo("Path: %s", ~path);
//
//    Stroka output;
//    TStringOutput outputStream(output);
//    TYsonWriter writer(&outputStream, TYsonWriter::EFormat::Binary);
//
//    try {
//        GetYPath(IYPathService::FromNode(~Root), path, &writer);
//    } catch (...) {
//        ythrow TServiceException(EErrorCode::YPathError) << CurrentExceptionMessage();
//    }
//
//    response->SetValue(output);
//    context->Reply();
//}
//
//RPC_SERVICE_METHOD_IMPL(TOrchidService, Set)
//{
//    UNUSED(response);
//
//    Stroka path = request->GetPath();
//    Stroka value = request->GetValue();
//
//    context->SetRequestInfo("Path: %s", ~path);
//
//    TStringInput input(value);
//    auto producer = TYsonReader::GetProducer(&input);
//
//    try {
//        SetYPath(IYPathService::FromNode(~Root), path, producer);
//    } catch (...) {
//        ythrow TServiceException(EErrorCode::YPathError) << CurrentExceptionMessage();
//    }
//
//    context->Reply();
//}
//
//RPC_SERVICE_METHOD_IMPL(TOrchidService, Remove)
//{
//    UNUSED(response);
//
//    Stroka path = request->GetPath();
//
//    context->SetRequestInfo("Path: %s", ~path);
//
//    try {
//        RemoveYPath(IYPathService::FromNode(~Root), path);
//    } catch (...) {
//        ythrow TServiceException(EErrorCode::YPathError) << CurrentExceptionMessage();
//    }
//
//    context->Reply();
//}

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT

