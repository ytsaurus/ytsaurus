#include "error.h"
#include "node.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/formats/config.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/ytree/node.h>

#include <yt/core/json/json_writer.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;
using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

namespace {

static Persistent<FunctionTemplate> ConstructorTemplate;

static Persistent<String> ErrorCode;
static Persistent<String> ErrorMessage;
static Persistent<String> ErrorAttributes;
static Persistent<String> ErrorInnerErrors;

Handle<Value> IsBasicYtError(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 1);

    bool result =
        args[0]->IsObject() &&
        ConstructorTemplate->HasInstance(args[0]);

    return scope.Close(result ? v8::True() : v8::False());
}

Handle<Value> SpawnBasicYtError(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 3);

    EXPECT_THAT_IS(args[0], Uint32);
    EXPECT_THAT_IS(args[1], String);
    EXPECT_THAT_HAS_INSTANCE(args[2], TNodeWrap);

    auto code = args[0]->Uint32Value();
    String::AsciiValue message (args[1]);
    auto attributes = TNodeWrap::UnwrapNode(args[2])->AsMap();

    TError fakeError;
    fakeError.SetCode(code);
    fakeError.SetMessage(TString(*message, message.length()));

    auto children = attributes->GetChildren();
    for (const auto& child : children) {
        fakeError.Attributes().Set(child.first, child.second);
    }

    return scope.Close(ConvertErrorToV8(fakeError));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Handle<Value> ConvertErrorToV8(const TError& error)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<Object> attributes = Object::New();
    {
        const auto& errorAttributes = error.Attributes();
        for (const auto& key : errorAttributes.List()) {
            auto value = errorAttributes.GetYson(key);

            TString encodedValue;
            encodedValue.reserve(value.GetData().length() * 2);
            TStringOutput valueStream(encodedValue);
            auto valueWriter = CreateJsonConsumer(&valueStream);
            Serialize(value, valueWriter.get());
            valueWriter->Flush();

            attributes->Set(
                String::New(key.data(), key.length()),
                String::New(encodedValue.data(), encodedValue.length()));
        }
    }

    Local<Array> inners = Array::New(error.InnerErrors().size());
    {
        const auto& errorInners = error.InnerErrors();
        for (int i = 0; i < errorInners.size(); ++i) {
            inners->Set(i, ConvertErrorToV8(errorInners[i]));
        }
    }

    Local<Object> result = ConstructorTemplate->GetFunction()->NewInstance();
    result->Set(ErrorCode, Integer::New(error.GetCode()));
    result->Set(ErrorMessage, String::New(error.GetMessage().c_str(), error.GetMessage().length()));
    result->Set(ErrorAttributes, attributes);
    result->Set(ErrorInnerErrors, inners);

    return scope.Close(result);
}

void InitializeError(Handle<Object> target)
{
    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New());
    ConstructorTemplate->SetClassName(String::NewSymbol("BasicYtError"));

    target->Set(
        String::NewSymbol("BasicYtError"),
        ConstructorTemplate->GetFunction());

    target->Set(
        String::NewSymbol("IsBasicYtError"),
        FunctionTemplate::New(IsBasicYtError)->GetFunction());

    target->Set(
        String::NewSymbol("SpawnBasicYtError"),
        FunctionTemplate::New(SpawnBasicYtError)->GetFunction());

    target->Set(
        String::NewSymbol("NRpc_UnavailableYtErrorCode"),
        Integer::New(static_cast<int>(NRpc::EErrorCode::Unavailable)));

    target->Set(
        String::NewSymbol("NSecurityClient_UserBannedYtErrorCode"),
        Integer::New(static_cast<int>(NSecurityClient::EErrorCode::UserBanned)));

    target->Set(
        String::NewSymbol("NSecurityClient_RequestQueueSizeLimitExceededYtErrorCode"),
        Integer::New(static_cast<int>(NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded)));

    target->Set(
        String::NewSymbol("NRpc_RequestQueueSizeLimitExceededYtErrorCode"),
        Integer::New(static_cast<int>(NRpc::EErrorCode::RequestQueueSizeLimitExceeded)));

    target->Set(
        String::NewSymbol("NChunkClient_AllTargetNodesFailedYtErrorCode"),
        Integer::New(static_cast<int>(NChunkClient::EErrorCode::AllTargetNodesFailed)));

    ErrorCode = NODE_PSYMBOL("code");
    ErrorMessage = NODE_PSYMBOL("message");
    ErrorAttributes = NODE_PSYMBOL("attributes");
    ErrorInnerErrors = NODE_PSYMBOL("inner_errors");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
