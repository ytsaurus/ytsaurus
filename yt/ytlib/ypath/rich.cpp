#include "stdafx.h"
#include "rich.h"
#include "tokenizer.h"

#include <ytlib/misc/error.h>
#include <ytlib/yson/yson_consumer.h>
#include <ytlib/yson/tokenizer.h>
#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NYPath {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TRichYPath::TRichYPath()
{ }

TRichYPath::TRichYPath(const TRichYPath& other)
    : Path_(other.Path_)
    , Attributes_(~other.Attributes_ ? other.Attributes_->Clone() : NULL)
{ }

TRichYPath::TRichYPath(const char* path)
    : Path_(path)
{ }

TRichYPath::TRichYPath(const TYPath& path)
    : Path_(path)
{ }

TRichYPath::TRichYPath(TRichYPath&& other)
    : Path_(MoveRV(other.Path_))
    , Attributes_(other.Attributes_)
{ }

TRichYPath::TRichYPath(const TYPath& path, const IAttributeDictionary& attributes)
    : Path_(path)
    , Attributes_(attributes.Clone())
{ }

const TYPath& TRichYPath::GetPath() const
{
    return Path_;
}

void TRichYPath::SetPath(const TYPath& path)
{
    Path_ = path;
}

const IAttributeDictionary& TRichYPath::Attributes() const
{
    return ~Attributes_ ? *Attributes_ : EmptyAttributes();
}

IAttributeDictionary& TRichYPath::Attributes()
{
    if (!Attributes_) {
        Attributes_ = CreateEphemeralAttributes();
    }
    return *Attributes_;
}

TRichYPath& TRichYPath::operator = (const TRichYPath& other)
{
    if (this != &other) {
        Path_ = other.Path_;
        Attributes_ = ~other.Attributes_ ? other.Attributes_->Clone() : NULL;
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////
/*
namespace {

void ThrowUnexpectedToken(const TToken& token)
{
    THROW_ERROR_EXCEPTION("Token is unexpected");
}

TRichYPath ParseAttributes(const Stroka& str)
{
    TTokenizer tokenizer(str);

    if (!str.empty() && str[0] == TokenTypeToChar(NYson::ETokenType::LeftAngle)) {
        NYson::TTokenizer tokenizer(str);

        int depth = 0;
        int attrStartPosition = -1;
        int attrEndPosition = -1;
        int pathStartPosition = -1;
        while (true) {
            int positionBefore = str.length() - tokenizer.GetCurrentSuffix().length();
            if (!tokenizer.ParseNext()) {
                THROW_ERROR_EXCEPTION("Unmatched '<' in YPath");
            }
            int positionAfter = str.length() - tokenizer.GetCurrentSuffix().length();

            switch (tokenizer.CurrentToken().GetType()) {
                case NYson::ETokenType::LeftAngle:
                    ++depth;
                    break;
                case NYson::ETokenType::RightAngle:
                    --depth;
                    break;
                default:
                    break;
            }

            if (attrStartPosition < 0 && depth == 1) {
                attrStartPosition = positionAfter;
            }

            if (attrEndPosition < 0 && depth == 0) {
                attrEndPosition = positionBefore;
                pathStartPosition = positionAfter;
                break;
            }
        }

        NYTree::TYsonString attrYson(
            str.substr(attrStartPosition, attrEndPosition - attrStartPosition),
            NYson::EYsonType::MapFragment);

        return TRichYPath(
            TrimLeadingWhitespaces(str.substr(pathStartPosition)),
            *ConvertToAttributes(attrYson));
    } else {
        return TRichYPath(str);
    }
    
    UNREACHABLE();
}

void ParseColumns(TTokenizer& tokenizer, TAttributes* attributes)
{
    if (tokenizer.GetCurrentType() != BeginColumnSelectorToken) {
        return;
    }

    TChannel channel;
    
    tokenizer.ParseNext();
    while (tokenizer.GetCurrentType() != EndColumnSelectorToken) {
        Stroka begin;
        bool isRange = false;
        switch (tokenizer.GetCurrentType()) {
            case ETokenType::String:
                begin.assign(tokenizer.CurrentToken().GetStringValue());
                tokenizer.ParseNext();
                if (tokenizer.GetCurrentType() == RangeToken) {
                    isRange = true;
                    tokenizer.ParseNext();
                }
                break;
            case RangeToken:
                isRange = true;
                tokenizer.ParseNext();
                break;
            default:
                ThrowUnexpectedToken(tokenizer.CurrentToken());
                YUNREACHABLE();
        }
        if (isRange) {
            switch (tokenizer.GetCurrentType()) {
                case ETokenType::String: {
                    Stroka end(tokenizer.CurrentToken().GetStringValue());
                    channel->AddRange(begin, end);
                    tokenizer.ParseNext();
                    break;
                }
                case ColumnSeparatorToken:
                case EndColumnSelectorToken:
                    channel->AddRange(TRange(begin));
                    break;
                default:
                    ThrowUnexpectedToken(tokenizer.CurrentToken());
                    YUNREACHABLE();
            }
        } else {
            channel->AddColumn(begin);
        }
        switch (tokenizer.GetCurrentType()) {
            case ColumnSeparatorToken:
                tokenizer.ParseNext();
                break;
            case EndColumnSelectorToken:
                break;
            default:
                ThrowUnexpectedToken(tokenizer.CurrentToken());
                YUNREACHABLE();
        }
    }
    tokenizer.ParseNext();

    attributes->Set("columns", ConvertToYsonString(channel));
}

void ParseKeyPart(
    TTokenizer& tokenizer,
    TKey* key)
{
    auto *keyPart = key->add_parts();

    switch (tokenizer.GetCurrentType()) {
        case ETokenType::String: {
            auto value = tokenizer.CurrentToken().GetStringValue();
            keyPart->set_str_value(value.begin(), value.size());
            keyPart->set_type(EKeyPartType::String);
            break;
        }

        case ETokenType::Integer: {
            auto value = tokenizer.CurrentToken().GetIntegerValue();
            keyPart->set_int_value(value);
            keyPart->set_type(EKeyPartType::Integer);
            break;
        }

        case ETokenType::Double: {
            auto value = tokenizer.CurrentToken().GetDoubleValue();
            keyPart->set_double_value(value);
            keyPart->set_type(EKeyPartType::Double);
            break;
        }

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            break;
    }
    tokenizer.ParseNext();
}

void ParseRowLimit(
    TTokenizer& tokenizer,
    ETokenType separator,
    TReadLimit* limit)
{
    if (tokenizer.GetCurrentType() == separator) {
        tokenizer.ParseNext();
        return;
    }

    switch (tokenizer.GetCurrentType()) {
        case RowIndexMarkerToken:
            tokenizer.ParseNext();
            limit->set_row_index(tokenizer.CurrentToken().GetIntegerValue());
            tokenizer.ParseNext();
            break;

        case BeginTupleToken:
            tokenizer.ParseNext();
            limit->mutable_key();
            while (tokenizer.GetCurrentType() != EndTupleToken) {
                ParseKeyPart(tokenizer, limit->mutable_key());
                switch (tokenizer.GetCurrentType()) {
                    case KeySeparatorToken:
                        tokenizer.ParseNext();
                        break;
                    case EndTupleToken:
                        break;
                    default:
                        ThrowUnexpectedToken(tokenizer.CurrentToken());
                        YUNREACHABLE();
                }
            }
            tokenizer.ParseNext();
            break;

        default:
            ParseKeyPart(tokenizer, limit->mutable_key());
            break;
    }

    tokenizer.CurrentToken().CheckType(separator);
    tokenizer.ParseNext();
}

void ParseRowLimits(
    TTokenizer& tokenizer,
    TReadLimit* lowerLimit,
    TReadLimit* upperLimit)
{
    *lowerLimit = TReadLimit();
    *upperLimit = TReadLimit();
    if (tokenizer.GetCurrentType() == BeginRowSelectorToken) {
        tokenizer.ParseNext();
        ParseRowLimit(tokenizer, RangeToken, lowerLimit);
        ParseRowLimit(tokenizer, EndRowSelectorToken, upperLimit);
    }
}

void TTableNodeProxy::ParseYPath(
    const TYPath& path,
    TChannel* channel,
    TReadLimit* lowerBound,
    TReadLimit* upperBound)
{
    TTokenizer tokenizer(path);
    tokenizer.ParseNext();
    ParseChannel(tokenizer, channel);
    ParseRowLimits(tokenizer, lowerBound, upperBound);
    tokenizer.CurrentToken().CheckType(ETokenType::EndOfStream);
}

} // namespace

static TRichYPath TRichYPath::Parse(const Stroka& str)
{
    TAutoPtr<NYTree::IAttributeDictionary> attributes;
    auto richYPath = ParseAttributes(str);

}
*/
////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRichYPath& path)
{
    auto keys = path.Attributes().List();
    if (keys.empty()) {
        return path.GetPath();
    }

    return
        Stroka('<') +
        ConvertToYsonString(path.Attributes()).Data() +
        Stroka('>') +
        path.GetPath();
}

void Serialize(const TRichYPath& richPath, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Items(richPath.Attributes())
        .EndAttributes()
        .Scalar(richPath.GetPath());
}

void Deserialize(TRichYPath& richPath, INodePtr node)
{
    if (node->GetType() != ENodeType::String) {
        THROW_ERROR_EXCEPTION("YPath can only be parsed from String");
    }
    richPath.SetPath(node->GetValue<Stroka>());
    richPath.Attributes().Clear();
    richPath.Attributes().MergeFrom(node->Attributes());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
