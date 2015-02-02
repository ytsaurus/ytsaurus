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
#include <ytlib/chunk_client/chunk_owner_ypath.pb.h>

namespace NYT {
namespace NYPath {

using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

const NYson::ETokenType BeginColumnSelectorToken = NYson::ETokenType::LeftBrace;
const NYson::ETokenType EndColumnSelectorToken = NYson::ETokenType::RightBrace;
const NYson::ETokenType ColumnSeparatorToken = NYson::ETokenType::Comma;
const NYson::ETokenType BeginRowSelectorToken = NYson::ETokenType::LeftBracket;
const NYson::ETokenType EndRowSelectorToken = NYson::ETokenType::RightBracket;
const NYson::ETokenType RowIndexMarkerToken = NYson::ETokenType::Hash;
const NYson::ETokenType BeginTupleToken = NYson::ETokenType::LeftParenthesis;
const NYson::ETokenType EndTupleToken = NYson::ETokenType::RightParenthesis;
const NYson::ETokenType KeySeparatorToken = NYson::ETokenType::Comma;
const NYson::ETokenType RangeToken = NYson::ETokenType::Colon;
const NYson::ETokenType RangeSeparatorToken = NYson::ETokenType::Comma;

////////////////////////////////////////////////////////////////////////////////

TRichYPath::TRichYPath()
{ }

TRichYPath::TRichYPath(const TRichYPath& other)
    : Path_(other.Path_)
    , Attributes_(other.Attributes_ ? other.Attributes_->Clone() : nullptr)
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
    return Attributes_ ? *Attributes_ : EmptyAttributes();
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
        Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void ThrowUnexpectedToken(const TToken& token)
{
    THROW_ERROR_EXCEPTION("Unexpected token %Qv",
        token);
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

        case NYson::ETokenType::Int64: {
            value = MakeUnversionedInt64Value(tokenizer.CurrentToken().GetInt64Value());
            break;
        }

        case NYson::ETokenType::Uint64: {
            value = MakeUnversionedUint64Value(tokenizer.CurrentToken().GetUint64Value());
            break;
        }

        case NYson::ETokenType::Double: {
            value = MakeUnversionedDoubleValue(tokenizer.CurrentToken().GetDoubleValue());
            break;
        }

        case NYson::ETokenType::Boolean: {
            value = MakeUnversionedBooleanValue(tokenizer.CurrentToken().GetBooleanValue());
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
    std::vector<NYson::ETokenType> separators,
    TReadLimit* limit)
{
    if (std::find(separators.begin(), separators.end(), tokenizer.GetCurrentType()) != separators.end()) {
        return;
    }

    TUnversionedOwningRowBuilder rowBuilder;
    bool hasKeyLimit = false;
    switch (tokenizer.GetCurrentType()) {
        case RowIndexMarkerToken:
            tokenizer.ParseNext();
            limit->SetRowIndex(tokenizer.CurrentToken().GetInt64Value());
            tokenizer.ParseNext();
            break;

        case BeginTupleToken:
            tokenizer.ParseNext();
            hasKeyLimit = true;
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
            hasKeyLimit = true;
            break;
    }

    if (hasKeyLimit) {
        auto key = rowBuilder.FinishRow();
        limit->SetKey(key);
    }

    tokenizer.CurrentToken().CheckType(separators);
}

void ParseRowRanges(NYson::TTokenizer& tokenizer, IAttributeDictionary* attributes)
{
    if (tokenizer.GetCurrentType() == BeginRowSelectorToken) {
        tokenizer.ParseNext();

        std::vector<TReadRange> ranges;

        bool finished = false;
        while (!finished) {
            TReadLimit lowerLimit, upperLimit;
            ParseRowLimit(tokenizer, {RangeToken}, &lowerLimit);
            tokenizer.ParseNext();

            ParseRowLimit(tokenizer, {RangeSeparatorToken, EndRowSelectorToken}, &upperLimit);
            if (tokenizer.CurrentToken().GetType() == EndRowSelectorToken) {
                finished = true;
            }
            tokenizer.ParseNext();

            ranges.push_back(TReadRange(lowerLimit, upperLimit));
        }

        attributes->Set("ranges", ConvertToYsonString(ranges));
    }
}

} // namespace

TRichYPath TRichYPath::Parse(const Stroka& str)
{
    auto attributes = CreateEphemeralAttributes();

    auto strWithoutAttributes = ParseAttributes(str, attributes.get());
    TTokenizer ypathTokenizer(strWithoutAttributes);

    while (ypathTokenizer.GetType() != ETokenType::EndOfStream && ypathTokenizer.GetType() != ETokenType::Range) {
        ypathTokenizer.Advance();
    }
    auto path = ypathTokenizer.GetPrefix();
    auto rangeStr = ypathTokenizer.GetToken();

    if (ypathTokenizer.GetType() == ETokenType::Range) {
        NYson::TTokenizer ysonTokenizer(rangeStr);
        ysonTokenizer.ParseNext();
        ParseChannel(ysonTokenizer, attributes.get());
        ParseRowRanges(ysonTokenizer, attributes.get());
        ysonTokenizer.CurrentToken().CheckType(NYson::ETokenType::EndOfStream);
    }
    return TRichYPath(path, *attributes);
}

TRichYPath TRichYPath::Normalize() const
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

bool TRichYPath::GetAppend() const
{
    return Attributes().Get("append", false);
}

TChannel TRichYPath::GetChannel() const
{
    return Attributes().Get("channel", TChannel::Universal());
}

std::vector<NChunkClient::TReadRange> TRichYPath::GetRanges() const
{
    // COMPAT(ignat): lower and upper limit attributes processed for compatibility.
    auto lowerLimitAttribute = Attributes().Find<TReadLimit>("lower_limit");
    auto upperLimitAttribute = Attributes().Find<TReadLimit>("upper_limit");
    auto rangesAttribute = Attributes().Find<std::vector<TReadLimit>>("ranges");
    if ((lowerLimitAttribute || upperLimitAttribute) && rangesAttribute) {
        THROW_ERROR_EXCEPTION("Path contains both multiple (\"ranges\" attribute) and single ranges (\"lower_limit\" or \"upper_limit\" attributes), which is forbidden");
    }

    if (lowerLimitAttribute || upperLimitAttribute) {
        return std::vector<TReadRange>({
            TReadRange(
                Attributes().Get("lower_limit", TReadLimit()),
                Attributes().Get("upper_limit", TReadLimit())
            )});
    } else {
        return Attributes().Get("ranges", std::vector<TReadRange>({TReadRange()}));
    }
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

std::vector<TRichYPath> Normalize(const std::vector<TRichYPath>& paths)
{
    std::vector<TRichYPath> result;
    for (const auto& path : paths) {
        result.push_back(path.Normalize());
    }
    return result;
}

void InitializeFetchRequest(
    NChunkClient::NProto::TReqFetch* request,
    const TRichYPath& richPath)
{
    auto channel = richPath.GetChannel();
    if (channel.IsUniversal()) {
        request->clear_channel();
    } else {
        ToProto(request->mutable_channel(), channel);
    }

    ToProto(request->mutable_ranges(), richPath.GetRanges());
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
