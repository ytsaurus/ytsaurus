#include "stdafx.h"
#include "ypath_client.h"

#include "../rpc/message.h"
#include "rpc.pb.h"

namespace NYT {
namespace NYTree {

using namespace NBus;
using namespace NRpc;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = YTreeLogger;

////////////////////////////////////////////////////////////////////////////////

TYPathRequest::TYPathRequest(const Stroka& verb)
    : Verb_(verb)
{ }

IMessage::TPtr TYPathRequest::Serialize()
{
    // Serialize body.
    TBlob bodyData;
    if (!SerializeBody(&bodyData)) {
        LOG_FATAL("Error serializing request body");
    }

    // Compose message.
    return CreateRequestMessage(
        TRequestId(),
        "",
        Verb_,
        MoveRV(bodyData),
        Attachments_);
}

////////////////////////////////////////////////////////////////////////////////

void TYPathResponse::Deserialize(NBus::IMessage* message)
{
    YASSERT(message != NULL);

    const auto& parts = message->GetParts();
    YASSERT(parts.ysize() >= 2);

    // Deserialize RPC header.
    TRequestHeader header;
    if (!DeserializeMessage(&header, parts[0])) {
        LOG_FATAL("Error deserializing response header");
    }

    // Deserialize body.
    DeserializeBody(parts[1]);

    // Load attachments.
    Attachments_.clear();
    NStl::copy(
        parts.begin() + 2,
        parts.end(),
        NStl::back_inserter(Attachments_));
}

EErrorCode TYPathResponse::GetErrorCode() const
{
    return Error_.GetCode();
}

bool TYPathResponse::IsOK() const
{
    return Error_.IsOK();
}

////////////////////////////////////////////////////////////////////////////////

void WrapYPathRequest(TClientRequest* outerRequest, TYPathRequest* innerRequest)
{
    auto message = innerRequest->Serialize();
    auto parts = message->GetParts();
    auto& attachments = outerRequest->Attachments();
    attachments.clear();
    NStl::copy(
        parts.begin(),
        parts.end(),
        NStl::back_inserter(attachments));
}

void UnwrapYPathResponse(TClientResponse* outerResponse, TYPathResponse* innerResponse)
{
    auto parts = outerResponse->Attachments();
    auto message = CreateMessageFromParts(parts);
    innerResponse->Deserialize(~message);
}

void SetYPathErrorResponse(const NRpc::TError& error, TYPathResponse* innerResponse)
{
    innerResponse->SetError(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
