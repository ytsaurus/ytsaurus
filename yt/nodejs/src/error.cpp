#include "error.h"
#include "node.h"

#include <ytlib/ytree/node.h>
#include <ytlib/formats/json_writer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYTree;
using namespace NFormats;

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

    YASSERT(args.Length() == 1);

    bool result =
        args[0]->IsObject() &&
        ConstructorTemplate->HasInstance(args[0]);
    return result ? v8::True() : v8::False();
}

Handle<Value> SpawnBasicYtError(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 3);

    EXPECT_THAT_IS(args[0], Uint32);
    EXPECT_THAT_IS(args[1], String);
    EXPECT_THAT_HAS_INSTANCE(args[2], TNodeJSNode);

    auto code = args[0]->Uint32Value();
    String::AsciiValue message (args[1]);
    auto attributes = TNodeJSNode::Node(args[2])->AsMap();

    TError fakeError;
    fakeError.SetCode(code);
    fakeError.SetMessage(Stroka(*message, message.length()));

    auto children = attributes->GetChildren();
    FOREACH (const auto& child, children) {
        fakeError.Attributes().Set(child.First(), child.Second());
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
        FOREACH (const auto& key, errorAttributes.List()) {
            auto value = errorAttributes.GetYson(key);

            Stroka encodedValue;
            encodedValue.reserve(value.Data().length() * 2);
            TStringOutput valueStream(encodedValue);
            TJsonWriter valueWriter(&valueStream);

            Consume(value, &valueWriter);
            valueWriter.Flush();

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

    ErrorCode = NODE_PSYMBOL("code");
    ErrorMessage = NODE_PSYMBOL("message");
    ErrorAttributes = NODE_PSYMBOL("attributes");
    ErrorInnerErrors = NODE_PSYMBOL("inner_errors");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
