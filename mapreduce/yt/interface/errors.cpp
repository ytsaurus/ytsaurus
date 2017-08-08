#include "errors.h"

#include <mapreduce/yt/node/node_io.h>
#include <mapreduce/yt/node/node_visitor.h>

#include <mapreduce/yt/interface/error_codes.h>

#include <library/json/json_reader.h>
#include <library/yson/writer.h>

#include <util/string/builder.h>
#include <util/stream/str.h>

namespace NYT {

using namespace NJson;

////////////////////////////////////////////////////////////////////

static void WriteErrorDescription(const TYtError& error, TOutputStream* out)
{
    (*out) << '`' << error.GetMessage() << '\'';
    const auto& innerErrorList = error.InnerErrors();
    if (!innerErrorList.empty()) {
        (*out) << " { ";
        bool first = true;
        for (const auto& innerError : innerErrorList) {
            if (first) {
                first = false;
            } else {
                (*out) << " ; ";
            }
            WriteErrorDescription(innerError, out);
        }
        (*out) << " }";
    }
}

static void SerializeError(const TYtError& error, IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    {
        consumer->OnKeyedItem("code");
        consumer->OnInt64Scalar(error.GetCode());

        consumer->OnKeyedItem("message");
        consumer->OnStringScalar(error.GetMessage());

        if (!error.GetAttributes().empty()) {
            consumer->OnKeyedItem("attributes");
            consumer->OnBeginMap();
            {
                for (const auto& item : error.GetAttributes()) {
                    consumer->OnKeyedItem(item.first);
                    TNodeVisitor(consumer).Visit(item.second);
                }
            }
            consumer->OnEndMap();
        }

        if (!error.InnerErrors().empty()) {
            consumer->OnKeyedItem("inner_errors");
            {
                consumer->OnBeginList();
                for (const auto& innerError : error.InnerErrors()) {
                    SerializeError(innerError, consumer);
                }
                consumer->OnEndList();
            }
        }
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////

TYtError::TYtError()
    : Code_(0)
{ }

TYtError::TYtError(const TString& message)
    : Code_(NYT::NClusterErrorCodes::Generic)
    , Message_(message)
{ }

TYtError::TYtError(int code, const TString& message)
    : Code_(code)
    , Message_(message)
{ }

TYtError::TYtError(const TJsonValue& value)
{
    const TJsonValue::TMap& map = value.GetMap();
    TJsonValue::TMap::const_iterator it = map.find("message");
    if (it != map.end()) {
        Message_ = it->second.GetString();
    }

    it = map.find("code");
    if (it != map.end()) {
        Code_ = static_cast<int>(it->second.GetInteger());
    } else {
        Code_ = NYT::NClusterErrorCodes::Generic;
    }

    it = map.find("inner_errors");
    if (it != map.end()) {
        const TJsonValue::TArray& innerErrors = it->second.GetArray();
        for (const auto& innerError : innerErrors) {
            InnerErrors_.push_back(TYtError(innerError));
        }
    }

    it = map.find("attributes");
    if (it != map.end()) {
        auto attributes = NYT::NodeFromJsonValue(it->second);
        if (attributes.IsMap()) {
            Attributes_ = std::move(attributes.AsMap());
        }
    }
}

TYtError::TYtError(const TNode& node)
{
    const auto& map = node.AsMap();
    auto it = map.find("message");
    if (it != map.end()) {
        Message_ = it->second.AsString();
    }

    it = map.find("code");
    if (it != map.end()) {
        Code_ = static_cast<int>(it->second.AsInt64());
    } else {
        Code_ = NYT::NClusterErrorCodes::Generic;
    }

    it = map.find("inner_errors");
    if (it != map.end()) {
        const auto& innerErrors = it->second.AsList();
        for (const auto& innerError : innerErrors) {
            InnerErrors_.push_back(TYtError(innerError));
        }
    }

    it = map.find("attributes");
    if (it != map.end()) {
        auto& attributes = it->second;
        if (attributes.IsMap()) {
            Attributes_ = std::move(attributes.AsMap());
        }
    }
}

int TYtError::GetCode() const
{
    return Code_;
}

const TString& TYtError::GetMessage() const
{
    return Message_;
}

const yvector<TYtError>& TYtError::InnerErrors() const
{
    return InnerErrors_;
}

void TYtError::ParseFrom(const TString& jsonError)
{
    TJsonValue value;
    TStringInput input(jsonError);
    ReadJsonTree(&input, &value);
    *this = TYtError(value);
}

int TYtError::GetInnerCode() const
{
    if (Code_ >= 100) {
        return Code_;
    }
    for (const auto& error : InnerErrors_) {
        int code = error.GetInnerCode();
        if (code) {
            return code;
        }
    }
    return 0;
}

bool TYtError::ContainsErrorCode(int code) const
{
    if (Code_ == code) {
        return true;
    }
    for (const auto& error : InnerErrors_) {
        if (error.ContainsErrorCode(code)) {
            return true;
        }
    }
    return false;
}


bool TYtError::ContainsText(const TStringBuf& text) const
{
    if (Message_.Contains(text)) {
        return true;
    }
    for (const auto& error : InnerErrors_) {
        if (error.ContainsText(text)) {
            return true;
        }
    }
    return false;
}

bool TYtError::HasAttributes() const
{
    return !Attributes_.empty();
}

const TNode::TMap& TYtError::GetAttributes() const
{
    return Attributes_;
}

TString TYtError::GetYsonText() const
{
    TStringStream out;
    TYsonWriter writer(&out, YF_TEXT);
    SerializeError(*this, &writer);
    return std::move(out.Str());
}

////////////////////////////////////////////////////////////////////////////////

TErrorResponse::TErrorResponse(int httpCode, const TString& requestId)
    : HttpCode_(httpCode)
    , RequestId_(requestId)
{ }

bool TErrorResponse::IsOk() const
{
    return Error_.GetCode() == 0;
}

void TErrorResponse::SetRawError(const TString& message)
{
    Error_ = TYtError(message);
    Setup();
}

void TErrorResponse::SetError(TYtError error)
{
    Error_ = std::move(error);
    Setup();
}

void TErrorResponse::ParseFromJsonError(const TString& jsonError)
{
    Error_.ParseFrom(jsonError);
    Setup();
}

int TErrorResponse::GetHttpCode() const
{
    return HttpCode_;
}

TString TErrorResponse::GetRequestId() const
{
    return RequestId_;
}

const TYtError& TErrorResponse::GetError() const
{
    return Error_;
}

bool TErrorResponse::IsResolveError() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NYTree::ResolveError);
}

bool TErrorResponse::IsAccessDenied() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NSecurityClient::AuthorizationError);
}

bool TErrorResponse::IsConcurrentTransactionLockConflict() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NCypressClient::ConcurrentTransactionLockConflict);
}

bool TErrorResponse::IsRequestRateLimitExceeded() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NSecurityClient::RequestQueueSizeLimitExceeded);
}

bool TErrorResponse::IsRequestQueueSizeLimitExceeded() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NRpc::RequestQueueSizeLimitExceeded);
}

bool TErrorResponse::IsChunkUnavailable() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NChunkClient::ChunkUnavailable);
}

bool TErrorResponse::IsRequestTimedOut() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::Timeout);
}

bool TErrorResponse::IsNoSuchTransaction() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NTransactionClient::NoSuchTransaction);
}

bool TErrorResponse::IsConcurrentOperationsLimitReached() const
{
    return Error_.ContainsErrorCode(NClusterErrorCodes::NScheduler::TooManyOperations);
}

void TErrorResponse::Setup()
{
    TStringStream s;
    WriteErrorDescription(Error_, &s);
    *this << s.Str() << "; full error: " << Error_.GetYsonText();
}

////////////////////////////////////////////////////////////////////

} // namespace NYT
