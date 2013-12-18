#include "stdafx.h"
#include "rich.h"

#include <core/ypath/tokenizer.h>

#include <core/misc/error.h>

#include <core/yson/consumer.h>
#include <core/yson/tokenizer.h>
#include <core/yson/token.h>

#include <core/ytree/fluent.h>

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

namespace NYT {

namespace NYPath {

using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace  NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

const NYson::ETokenType::EDomain BeginColumnSelectorToken = NYson::ETokenType::LeftBrace;
const NYson::ETokenType::EDomain EndColumnSelectorToken = NYson::ETokenType::RightBrace;
const NYson::ETokenType::EDomain ColumnSeparatorToken = NYson::ETokenType::Comma;
const NYson::ETokenType::EDomain BeginRowSelectorToken = NYson::ETokenType::LeftBracket;
const NYson::ETokenType::EDomain EndRowSelectorToken = NYson::ETokenType::RightBracket;
const NYson::ETokenType::EDomain RowIndexMarkerToken = NYson::ETokenType::Hash;
const NYson::ETokenType::EDomain BeginTupleToken = NYson::ETokenType::LeftParenthesis;
const NYson::ETokenType::EDomain EndTupleToken = NYson::ETokenType::RightParenthesis;
const NYson::ETokenType::EDomain KeySeparatorToken = NYson::ETokenType::Comma;
const NYson::ETokenType::EDomain RangeToken = NYson::ETokenType::Colon;

////////////////////////////////////////////////////////////////////////////////

TRichYPath::TRichYPath()
{ }

TRichYPath::TRichYPath(const TRichYPath& other)
    : Path_(other.Path_)
    , Attributes_(~other.Attributes_ ? other.Attributes_->Clone() : nullptr)
{ }

TRichYPath::TRichYPath(const char* path)
    : Path_(path)
{ }

TRichYPath::TRichYPath(const TYPath& path)
    : Path_(path)
{ }

TRichYPath::TRichYPath(TRichYPath&& other)
    : Path_(std::move(other.Path_))
    , Attributes_(std::move(other.Attributes_))
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

namespace {

void ThrowUnexpectedToken(const TToken& token)
{
    THROW_ERROR_EXCEPTION("Token is unexpected: %s", ~token.ToString());
}

Stroka ParseAttributes(const Stroka& str, IAttributeDictionary* attributes)
{
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

        TYsonString attrYson(
            str.substr(attrStartPosition, attrEndPosition - attrStartPosition),
            NYson::EYsonType::MapFragment);
        attributes->MergeFrom(*ConvertToAttributes(attrYson));

        return TrimLeadingWhitespaces(str.substr(pathStartPosition));
    }
    return str;
}

void ParseChannel(NYson::TTokenizer& tokenizer, IAttributeDictionary* attributes)
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
            case NYson::ETokenType::String:
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
                case NYson::ETokenType::String: {
                    Stroka end(tokenizer.CurrentToken().GetStringValue());
                    channel.AddRange(begin, end);
                    tokenizer.ParseNext();
                    break;
                }
                case ColumnSeparatorToken:
                case EndColumnSelectorToken:
                    channel.AddRange(TRange(begin));
                    break;
                default:
                    ThrowUnexpectedToken(tokenizer.CurrentToken());
                    YUNREACHABLE();
            }
        } else {
            channel.AddColumn(begin);
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

    attributes->Set("channel", ConvertToYsonString(channel));
}

void ParseKeyPart(
    NYson::TTokenizer& tokenizer,
    TUnversionedOwningRowBuilder* rowBuilder)
{
    // We don't fill id here, because key part columns are well known.
    // Also we don't have a name table for them :)
    TUnversionedValue value;

    switch (tokenizer.GetCurrentType()) {
        case NYson::ETokenType::String: {
            auto str = tokenizer.CurrentToken().GetStringValue();
            value = MakeUnversionedStringValue(str);
            break;
        }

        case NYson::ETokenType::Integer: {
            value = MakeUnversionedIntegerValue(tokenizer.CurrentToken().GetIntegerValue());
            break;
        }

        case NYson::ETokenType::Double: {
            value = MakeUnversionedDoubleValue(tokenizer.CurrentToken().GetDoubleValue());
            break;
        }

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            break;
    }
    rowBuilder->AddValue(value);
    tokenizer.ParseNext();
}

void ParseRowLimit(
    NYson::TTokenizer& tokenizer,
    NYson::ETokenType separator,
    TReadLimit* limit)
{
    if (tokenizer.GetCurrentType() == separator) {
        tokenizer.ParseNext();
        return;
    }

    TUnversionedOwningRowBuilder rowBuilder;
    switch (tokenizer.GetCurrentType()) {
        case RowIndexMarkerToken:
            tokenizer.ParseNext();
            limit->SetRowIndex(tokenizer.CurrentToken().GetIntegerValue());
            tokenizer.ParseNext();
            break;

        case BeginTupleToken:
            tokenizer.ParseNext();
            while (tokenizer.GetCurrentType() != EndTupleToken) {
                ParseKeyPart(tokenizer, &rowBuilder);
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
            ParseKeyPart(tokenizer, &rowBuilder);
            break;
    }

    auto key = rowBuilder.Finish();
    if (key.GetValueCount() > 0) {
        limit->SetKey(key);
    }

    tokenizer.CurrentToken().CheckType(separator);
    tokenizer.ParseNext();
}

void ParseRowLimits(NYson::TTokenizer& tokenizer, IAttributeDictionary* attributes)
{
    if (tokenizer.GetCurrentType() == BeginRowSelectorToken) {
        tokenizer.ParseNext();

        TReadLimit lowerLimit, upperLimit;
        ParseRowLimit(tokenizer, RangeToken, &lowerLimit);
        ParseRowLimit(tokenizer, EndRowSelectorToken, &upperLimit);

        if (lowerLimit.HasKey() || lowerLimit.HasRowIndex()) {
            attributes->SetYson("lower_limit", ConvertToYsonString(lowerLimit));
        }
        if (upperLimit.HasKey() || upperLimit.HasRowIndex()) {
            attributes->SetYson("upper_limit", ConvertToYsonString(upperLimit));
        }
    }
}

} // anonymous namespace

TRichYPath TRichYPath::Parse(const Stroka& str)
{
    auto attributes = CreateEphemeralAttributes();

    auto strWithoutAttributes = ParseAttributes(str, ~attributes);
    TTokenizer ypathTokenizer(strWithoutAttributes);

    while (ypathTokenizer.GetType() != ETokenType::EndOfStream && ypathTokenizer.GetType() != ETokenType::Range) {
        ypathTokenizer.Advance();
    }
    auto path = ypathTokenizer.GetPrefix();
    auto rangeStr = ypathTokenizer.GetToken();

    if (ypathTokenizer.GetType() == ETokenType::Range) {
        NYson::TTokenizer ysonTokenizer(rangeStr);
        ysonTokenizer.ParseNext();
        ParseChannel(ysonTokenizer, ~attributes);
        ParseRowLimits(ysonTokenizer, ~attributes);
        ysonTokenizer.CurrentToken().CheckType(NYson::ETokenType::EndOfStream);
    }

    return TRichYPath(path, *attributes);
}

TRichYPath TRichYPath::Simplify() const
{
    auto parsed = TRichYPath::Parse(Path_);
    parsed.Attributes().MergeFrom(Attributes());
    return parsed;
}

void TRichYPath::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, Path_);
    Save(context, Attributes_);
}

void TRichYPath::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    Load(context, Path_);
    Load(context, Attributes_);
}

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

std::vector<TRichYPath> Simplify(const std::vector<TRichYPath>& paths)
{
    std::vector<TRichYPath> result;
    for (const auto& path : paths) {
        result.push_back(path.Simplify());
    }
    return result;
}

void Serialize(const TRichYPath& richPath, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Items(richPath.Attributes())
        .EndAttributes()
        .Value(richPath.GetPath());
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
