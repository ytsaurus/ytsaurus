#include "lazy_parser.h"
#include "yson_lazy_map.h"

namespace NYT {
namespace NPython {

using namespace NYTree;

using NYson::NDetail::EndMapSymbol;
using NYson::NDetail::EndSymbol;
using NYson::NDetail::BeginMapSymbol;
using NYson::NDetail::KeyValueSeparatorSymbol;
using NYson::NDetail::StringMarker;
using NYson::NDetail::BeginAttributesSymbol;
using NYson::NDetail::EndAttributesSymbol;

using NYson::EYsonType;
using NYson::TToken;
using NYson::ETokenType;

////////////////////////////////////////////////////////////////////////////////

class TLazyDictConsumer
{
public:
    TLazyDictConsumer(const TNullable<TString>& encoding, bool alwaysCreateAttributes)
        : Encoding_(encoding)
        , AlwaysCreateAttributes_(alwaysCreateAttributes)
    {
        Py::Object encodingParam;
        if (encoding) {
            encodingParam = Py::String(encoding.Get());
        } else {
            encodingParam = Py::None();
        }
        Py::TupleN params(encodingParam, Py::Boolean(alwaysCreateAttributes));
        ResultObject_ = Py::Object(LazyYsonMapNew(&TLazyYsonMapType, Py_None, Py_None));
        LazyYsonMapInit(reinterpret_cast<TLazyYsonMap*>(ResultObject_.ptr()), params.ptr(), Py::Dict().ptr());

        TLazyYsonMap* object = reinterpret_cast<TLazyYsonMap*>(ResultObject_.ptr());
        TLazyYsonMapBase* attributes = reinterpret_cast<TLazyYsonMapBase*>(object->Attributes);
        LazyDict_ = object->super.Dict;
        LazyAttributesDict_ = attributes->Dict;
    }

    Py::Object ExtractObject()
    {
        return ResultObject_;
    }

    void OnBeginAttributes()
    {
        AttributesMode_ = true;
    }

    void OnEndAttributes()
    {
        AttributesMode_ = false;
    }

    void SetObject(Py::Object object)
    {
        object.increment_reference_count();
        if (LazyAttributesDict_->Length() > 0) {
            auto attributes = PyObject_GetAttrString(ResultObject_.ptr(), "attributes");
            Py_INCREF(attributes);
            PyObject_SetAttrString(object.ptr(), "attributes", attributes);
        }

        ResultObject_.decrement_reference_count();
        ResultObject_ = object;
    }

    void OnKeyValue(Py::Object key, TSharedRefArray value)
    {
        if (AttributesMode_) {
            LazyAttributesDict_->SetItem(key, value);
        } else {
            LazyDict_->SetItem(key, value);
        }
    }

    std::unique_ptr<TPythonObjectBuilder> GetPythonObjectBuilder()
    {
        bool hasAttributes = LazyAttributesDict_->Length() > 0;
        auto objectBuilder = new TPythonObjectBuilder(AlwaysCreateAttributes_ || hasAttributes, Encoding_);
        return std::unique_ptr<TPythonObjectBuilder>(objectBuilder);
    }

private:
    TLazyDict* LazyDict_;
    TLazyDict* LazyAttributesDict_;
    Py::Object ResultObject_;

    TNullable<TString> Encoding_;
    bool AlwaysCreateAttributes_;

    bool AttributesMode_ = false;
};

////////////////////////////////////////////////////////////////////////////////

template <class TBlockStream, bool EnableLinePositionInfo>
class TMemoryLexer
    : public NYson::NDetail::TLexer<TBlockStream, EnableLinePositionInfo>
{
public:
    TMemoryLexer(const TBlockStream& blockStream)
        : TBase(blockStream)
    { }

    void GetToken(TToken* token)
    {
        TBase::GetToken(token);
        LastToken_ = *token;
    }

    void SkipToken()
    {
        TToken token;
        TBase::GetToken(&token);
    }

    const TToken& GetLastToken()
    {
        return LastToken_;
    }

private:
    typedef NYson::NDetail::TLexer<TBlockStream, EnableLinePositionInfo> TBase;
    TToken LastToken_;
};

////////////////////////////////////////////////////////////////////////////////

class TLazyDictParser
    : TMemoryLexer<NYson::NDetail::TReaderWithContext<NYT::NPython::TStreamReader, 64>, true>
{
public:
    TLazyDictParser(
        TInputStream* stream,
        TPythonStringCache* keyCacher,
        TLazyDictConsumer* consumer);

    PyObject* DoParse(EYsonType parsingMode);

private:
    typedef TMemoryLexer<NYson::NDetail::TReaderWithContext<NYT::NPython::TStreamReader, 64>, true> TBase;

    void ParseMapFragment(char endSymbol, NYson::ETokenType finishTokenType);
    void ParseMapKey(char ch, TStringBuf* result);
    void ParseMap();
    void ParseAttributes();
    void ParseNode();

    bool PopRightBrace(const TToken& token, std::stack<ETokenType>& tokensStack, ETokenType expectedTopToken);

    TSharedRefArray ParseMapValue(NYson::ETokenType finishTokenType);

    TPythonStringCache* KeyCacher_;
    TLazyDictConsumer* Consumer_;
};


TLazyDictParser::TLazyDictParser(
    TInputStream* stream,
    TPythonStringCache* keyCacher,
    TLazyDictConsumer* consumer)
    : TBase(NYT::NPython::TStreamReader(stream))
    , KeyCacher_(keyCacher)
    , Consumer_(consumer)
{ }

PyObject* TLazyDictParser::DoParse(EYsonType parsingMode)
{
    YCHECK(parsingMode != EYsonType::ListFragment);
    try {
        switch (parsingMode) {
            case EYsonType::Node:
                ParseNode();
                break;

            case EYsonType::MapFragment:
                ParseMapFragment(EndSymbol, ETokenType::EndOfStream);
                break;

            default:
                Y_UNREACHABLE();
        }

        while (!(TBase::IsFinished() && TBase::IsEmpty())) {
            if (TBase::template SkipSpaceAndGetChar<true>() != EndSymbol) {
                THROW_ERROR_EXCEPTION("Stray %Qv found",
                    *TBase::Current())
                    << *this;
            } else if (!TBase::IsEmpty()) {
                TBase::Advance(1);
            }
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error occurred while parsing YSON")
            << TErrorAttribute("context", TBase::GetContextFromCheckpoint())
            << ex;
    }
    return Consumer_->ExtractObject().ptr();
}

void TLazyDictParser::ParseMap()
{
    TBase::CheckpointContext();
    ParseMapFragment(EndMapSymbol, ETokenType::RightBrace);
    TBase::CheckpointContext();
}

void TLazyDictParser::ParseAttributes()
{
    Consumer_->OnBeginAttributes();
    TBase::CheckpointContext();
    ParseMapFragment(EndAttributesSymbol, ETokenType::RightAngle);
    Consumer_->OnEndAttributes();
    TBase::CheckpointContext();
}

void TLazyDictParser::ParseNode()
{
    TBase::CheckpointContext();
    char ch = TBase::SkipSpaceAndGetChar();
    if (ch == BeginAttributesSymbol) {
        TBase::Advance(1);
        ParseAttributes();
        ch = TBase::SkipSpaceAndGetChar();
        TBase::CheckpointContext();
    }

    if (ch == BeginMapSymbol) {
        TBase::Advance(1);
        ParseMap();
        return;
    }

    auto objectBuilder = Consumer_->GetPythonObjectBuilder();
    NYson::TYsonParser parser(objectBuilder.get());

    auto readData = [&] {
        size_t readBytesCount = TBase::End() - TBase::Current();
        parser.Read(TStringBuf(TBase::Current(), readBytesCount));
        TBase::Advance(readBytesCount);
    };

    while (!TBase::IsFinished()) {
        readData();
        TBase::RefreshBlock();
    }
    readData();
    parser.Finish();

    Consumer_->SetObject(objectBuilder->ExtractObject());
}

void TLazyDictParser::ParseMapFragment(char endSymbol, ETokenType finishTokenType)
{
    char ch = TBase::SkipSpaceAndGetChar();
    while (ch) {
        if (ch == endSymbol) {
            TBase::SkipToken();
            break;
        }
        TStringBuf key;
        ParseMapKey(ch, &key);
        TBase::CheckpointContext();

        auto* itemKey = KeyCacher_->GetPythonString(key);

        ch = TBase::SkipSpaceAndGetChar();
        if (ch == KeyValueSeparatorSymbol) {
            TBase::Advance(1);
        } else {
            THROW_ERROR_EXCEPTION("Expected %Qv but %Qv found",
                KeyValueSeparatorSymbol,
                ch)
                << *this;
        }
        auto value = ParseMapValue(finishTokenType);
        TBase::CheckpointContext();

        auto lastToken = TBase::GetLastToken();

        Consumer_->OnKeyValue(Py::Object(itemKey), value);

        lastToken.ExpectTypes({ETokenType::Semicolon, finishTokenType});
        if (lastToken.GetType() == ETokenType::Semicolon) {
            ch = TBase::SkipSpaceAndGetChar();
        } else {
            break;
        }
    }
}

void TLazyDictParser::ParseMapKey(char ch, TStringBuf* result)
{
    TBase::CheckpointContext();
    switch (ch) {
        case '"': {
            TBase::Advance(1);
            TBase::ReadQuotedString(result);
            break;
        }
        case StringMarker: {
            TBase::Advance(1);
            TBase::ReadBinaryString(result);
            break;
        }
        default: {
            if (isalpha(ch) || ch == '_') {
                TBase::ReadUnquotedString(result);
            } else {
                THROW_ERROR_EXCEPTION("Unexpected %Qv while parsing key",
                    ch)
                    << *this;
            }
        }
    }
}

bool TLazyDictParser::PopRightBrace(
    const TToken& token,
    std::stack<ETokenType>& tokensStack,
    ETokenType expectedTopToken)
{
    if (tokensStack.empty()) {
        return false;
    }
    if (tokensStack.top() != expectedTopToken) {
        THROW_ERROR_EXCEPTION("Unexpected token %Qv in YSON map", token);
    }
    tokensStack.pop();
    return true;
}

TSharedRefArray TLazyDictParser::ParseMapValue(ETokenType finishTokenType)
{
    TBase::CheckpointContext();
    TBase::ExtractPrefix();
    bool finished = false;
    std::stack<ETokenType> tokens;
    TToken token;
    std::vector<TSharedRef> prefixBlobs;

    while (!finished) {
        TBase::GetToken(&token);
        auto type = token.GetType();
        prefixBlobs.push_back(TBase::ExtractPrefix());

        switch (type) {
            case ETokenType::EndOfStream:
                finished = true;
                break;
            case ETokenType::LeftAngle:
            case ETokenType::LeftBracket:
            case ETokenType::LeftBrace:
                tokens.push(type);
                break;
            case ETokenType::RightAngle:
                finished = !PopRightBrace(token, tokens, ETokenType::LeftAngle);
                break;
            case ETokenType::RightBracket:
                finished = !PopRightBrace(token, tokens, ETokenType::LeftBracket);
                break;
            case ETokenType::RightBrace:
                finished = !PopRightBrace(token, tokens, ETokenType::LeftBrace);
                break;
            case ETokenType::Semicolon:
                if (tokens.size() == 0) {
                    prefixBlobs.pop_back();
                    auto result = TSharedRefArray(prefixBlobs);
                    if (result.Size() == 0) {
                        THROW_ERROR_EXCEPTION("Unexpected token %Qv in YSON map", TBase::GetLastToken());
                    }
                    return result;
                }
                break;
            case ETokenType::String:
            case ETokenType::Int64:
            case ETokenType::Uint64:
            case ETokenType::Double:
            case ETokenType::Boolean:
            case ETokenType::Hash:
            case ETokenType::Equals:
                break;
            default:
                THROW_ERROR_EXCEPTION("Unexpected token %Qv in YSON map", token);
        }
    }
    if (tokens.size() != 0) {
        THROW_ERROR_EXCEPTION("YSON map is incomplete");
    }

    auto lastToken = TBase::GetLastToken();
    if (lastToken.GetType() != finishTokenType) {
        THROW_ERROR_EXCEPTION("Unexpected token %Qv in YSON map", lastToken);
    }

    if (finishTokenType != ETokenType::EndOfStream) {
        prefixBlobs.pop_back();
    }
    auto result = TSharedRefArray(prefixBlobs);

    if (result.Size() == 0) {
        THROW_ERROR_EXCEPTION("Unexpected token %Qv in YSON map", lastToken);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

PyObject* ParseLazyDict(
    TInputStream* stream,
    NYson::EYsonType parsingMode,
    const TNullable<TString>& encoding,
    bool alwaysCreateAttributes,
    TPythonStringCache* keyCacher)
{
    TLazyDictConsumer consumer(encoding, alwaysCreateAttributes);
    TLazyDictParser parser(stream, keyCacher, &consumer);
    return parser.DoParse(parsingMode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
