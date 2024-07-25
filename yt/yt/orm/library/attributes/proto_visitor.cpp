#include "proto_visitor.h"

#include "helpers.h"

#include <library/cpp/yt/misc/variant.h>

#include <util/string/cast.h>

namespace NYT::NOrm::NAttributes {

using NProtoBuf::FieldDescriptor;
using NProtoBuf::Message;

////////////////////////////////////////////////////////////////////////////////

void TProtoVisitorBase::SkipSlash()
{
    if (Tokenizer_.Skip(NYPath::ETokenType::Slash)) {
        return;
    }

    if (LeadingSlashOptional_ && Tokenizer_.GetInput() == Tokenizer_.GetPath()) {
        return;
    }

    Throw(EErrorCode::MalformedPath, "Expected slash but got %Qv", Tokenizer_.GetToken());
}

void TProtoVisitorBase::Push(TToken token)
{
    Visit(token,
        [this] (int index) { CurrentPath_.Push(index); },
        [this] (TStringBuf key) { CurrentPath_.Push(key); });
}

void TProtoVisitorBase::AdvanceOver(TToken token)
{
    if (!PathComplete()) {
        Tokenizer_.Advance();
    }
    Push(token);
}

void TProtoVisitorBase::AdvanceOverAsterisk()
{
    if (!AllowAsterisk_) {
        Throw(EErrorCode::Unimplemented, "Cannot handle asterisks");
    }

    Tokenizer_.Advance();
}

TProtoVisitorBase::TCheckpoint::TCheckpoint(NYPath::TTokenizer& tokenizer)
    : TokenizerCheckpoint_(tokenizer)
{ }

TProtoVisitorBase::TCheckpoint::~TCheckpoint()
{
    if (Defer_) {
        Defer_();
    }
}

void TProtoVisitorBase::TCheckpoint::Defer(std::function<void()> defer)
{
    if (Defer_) {
        Defer_ = [oldDefer = std::move(Defer_), defer = std::move(defer)] () {
            oldDefer();
            defer();
        };
    } else {
        Defer_ = defer;
    }
}

TProtoVisitorBase::TCheckpoint TProtoVisitorBase::CheckpointBranchedTraversal(TToken token)
{
    TCheckpoint result(Tokenizer_);
    Push(token);
    result.Defer([this] () { CurrentPath_.Pop(); });
    return result;
}

void TProtoVisitorBase::Expect(NYPath::ETokenType type) const
{
    if (Tokenizer_.GetType() == type) {
        return;
    }

    TError error(EErrorCode::MalformedPath,
        "Expected %Qlv but got %Qlv",
        type,
        Tokenizer_.GetType());

    if (Tokenizer_.GetPreviousType() == NYPath::ETokenType::Slash) {
        error <<= TErrorAttribute("note", "the path cannot normally end with a slash");
    }

    Throw(error);
}

bool TProtoVisitorBase::PathComplete() const
{
    return Tokenizer_.GetType() == NYPath::ETokenType::EndOfStream;
}

void TProtoVisitorBase::Reset(NYPath::TYPathBuf path)
{
    Tokenizer_.Reset(path);
    Tokenizer_.Advance();
    CurrentPath_.Reset();
    StopIteration_ = false;
}

TErrorOr<TIndexParseResult> TProtoVisitorBase::ParseCurrentListIndex(int size) const
{
    Expect(NYPath::ETokenType::Literal);
    auto indexParseResult = ParseListIndex(Tokenizer_.GetToken(), size);

    if (indexParseResult.IsOutOfBounds(size)) {
        return TError(EErrorCode::OutOfBounds,
            "Index %Qv out of bounds for repeated field of size %v",
            Tokenizer_.GetToken(),
            size);
    }

    return indexParseResult;
}

std::unique_ptr<Message> TProtoVisitorBase::MakeMapKeyMessage(
    const FieldDescriptor* fieldDescriptor,
    const TString& key) const
{
    auto* descriptor = fieldDescriptor->message_type();
    std::unique_ptr<Message> result{
        NProtoBuf::MessageFactory::generated_factory()->GetPrototype(descriptor)->New()};

    auto* keyFieldDescriptor = descriptor->map_key();
    auto* reflection = result->GetReflection();

    switch (keyFieldDescriptor->cpp_type()) {
        case FieldDescriptor::CppType::CPPTYPE_INT32: {
            i32 value;
            if (!TryFromString(key, value)) {
                Throw(EErrorCode::MalformedPath,
                    "Path entry %v is not convertible to an integer",
                    key);
            }
            reflection->SetInt32(result.get(), keyFieldDescriptor, value);
            break;
        }
        case FieldDescriptor::CppType::CPPTYPE_UINT32: {
            ui32 value;
            if (!TryFromString(key, value)) {
                Throw(EErrorCode::MalformedPath,
                    "Path entry %v is not convertible to an integer",
                    key);
            }
            reflection->SetUInt32(result.get(), keyFieldDescriptor, value);
            break;
        }
        case FieldDescriptor::CppType::CPPTYPE_INT64: {
            i64 value;
            if (!TryFromString(key, value)) {
                Throw(EErrorCode::MalformedPath,
                    "Path entry %v is not convertible to an integer",
                    key);
            }
            reflection->SetInt64(result.get(), keyFieldDescriptor, value);
            break;
        }
        case FieldDescriptor::CppType::CPPTYPE_UINT64: {
            ui64 value;
            if (!TryFromString(key, value)) {
                Throw(EErrorCode::MalformedPath,
                    "Path entry %v is not convertible to an integer",
                    key);
            }
            reflection->SetUInt64(result.get(), keyFieldDescriptor, value);
            break;
        }
        case FieldDescriptor::CppType::CPPTYPE_STRING: {
            reflection->SetString(result.get(), keyFieldDescriptor, key);
            break;
        }
        default: {
            Throw(EErrorCode::InvalidData,
                "Fields of type %v are not supported as map keys",
                keyFieldDescriptor->type_name());
        }
    }

    return result;
}

void TProtoVisitorBase::ThrowOnError(TError error) const
{
    if (!error.IsOK()) {
        Throw(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
