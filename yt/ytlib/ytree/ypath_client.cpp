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

TYPathRequest::TYPathRequest(const Stroka& verb, TYPath path)
    : Verb_(verb)
    , Path_(path)
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
        Path_,
        Verb_,
        MoveRV(bodyData),
        Attachments_);
}

////////////////////////////////////////////////////////////////////////////////

void TYPathResponse::Deserialize(NBus::IMessage* message)
{
    YASSERT(message != NULL);

    const auto& parts = message->GetParts();
    YASSERT(parts.ysize() >= 1);

    // Deserialize RPC header.
    TResponseHeader header;
    if (!DeserializeMessage(&header, parts[0])) {
        LOG_FATAL("Error deserializing response header");
    }

    Error_ = TError(
        EErrorCode(header.GetErrorCode(), header.GetErrorCodeString()),
        header.GetErrorMessage());

    if (Error_.IsOK()) {
        // Deserialize body.
        DeserializeBody(parts[1]);

        // Load attachments.
        Attachments_.clear();
        NStl::copy(
            parts.begin() + 2,
            parts.end(),
            NStl::back_inserter(Attachments_));
    }
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

} // namespace NYTree
} // namespace NYT
