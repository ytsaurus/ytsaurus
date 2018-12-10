#include "rich.h"

#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/column_rename_descriptor.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/parser_helpers.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/token.h>
#include <yt/core/yson/tokenizer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NYPath {

using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NTableClient;

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

bool operator== (const TRichYPath& lhs, const TRichYPath& rhs)
{
    return lhs.GetPath() == rhs.GetPath() && lhs.Attributes() == rhs.Attributes();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void ThrowUnexpectedToken(const TToken& token)
{
    THROW_ERROR_EXCEPTION("Unexpected token %Qv",
        token);
}

TString ParseAttributes(const TString& str, IAttributeDictionary* attributes)
{
    int spaceCount = 0;
    {
        size_t index = 0;
        while (index < str.Size() && IsSpace(str[index])) {
            ++index;
        }
        if (index == str.Size() || str[index] != TokenTypeToChar(NYson::ETokenType::LeftAngle)) {
            return str;
        }
        spaceCount = index;
    }

    NYson::TTokenizer tokenizer(TStringBuf(str).SubStr(spaceCount));
    tokenizer.ParseNext();
    YCHECK(tokenizer.CurrentToken().GetType() == NYson::ETokenType::LeftAngle);

    int depth = 0;
    int attrStartPosition = spaceCount + 1;

    while (true) {
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

        if (depth == 0) {
            break;
        }

        if (!tokenizer.ParseNext()) {
            THROW_ERROR_EXCEPTION("Unmatched '<' in YPath");
        }
    }

    int attrEndPosition = spaceCount + tokenizer.GetPosition() - 1;
    int pathStartPosition = attrEndPosition + 1;

    TYsonString attrYson(
        str.substr(attrStartPosition, attrEndPosition - attrStartPosition),
        NYson::EYsonType::MapFragment);
    attributes->MergeFrom(*ConvertToAttributes(attrYson));

    return TrimLeadingWhitespaces(str.substr(pathStartPosition));
}

void ParseColumns(NYson::TTokenizer& tokenizer, IAttributeDictionary* attributes)
{
    if (tokenizer.GetCurrentType() != BeginColumnSelectorToken) {
        return;
    }

    std::vector<TString> columns;

    tokenizer.ParseNext();
    while (tokenizer.GetCurrentType() != EndColumnSelectorToken) {
        TString begin;
        switch (tokenizer.GetCurrentType()) {
            case NYson::ETokenType::String:
                begin.assign(tokenizer.CurrentToken().GetStringValue());
                tokenizer.ParseNext();
                break;
            default:
                ThrowUnexpectedToken(tokenizer.CurrentToken());
                Y_UNREACHABLE();
        }

        columns.push_back(begin);

        switch (tokenizer.GetCurrentType()) {
            case ColumnSeparatorToken:
                tokenizer.ParseNext();
                break;
            case EndColumnSelectorToken:
                break;
            default:
                ThrowUnexpectedToken(tokenizer.CurrentToken());
                Y_UNREACHABLE();
        }
    }
    tokenizer.ParseNext();

    attributes->Set("columns", ConvertToYsonString(columns));
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

        case NYson::ETokenType::Hash: {
            value = MakeUnversionedSentinelValue(EValueType::Null);
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
                        Y_UNREACHABLE();
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

    tokenizer.CurrentToken().ExpectTypes(separators);
}

void ParseRowRanges(NYson::TTokenizer& tokenizer, IAttributeDictionary* attributes)
{
    if (tokenizer.GetCurrentType() == BeginRowSelectorToken) {
        tokenizer.ParseNext();

        std::vector<TReadRange> ranges;

        bool finished = false;
        while (!finished) {
            TReadLimit lowerLimit, upperLimit;
            ParseRowLimit(tokenizer, {RangeToken, RangeSeparatorToken, EndRowSelectorToken}, &lowerLimit);
            if (tokenizer.GetCurrentType() == RangeToken) {
                tokenizer.ParseNext();

                ParseRowLimit(tokenizer, {RangeSeparatorToken, EndRowSelectorToken}, &upperLimit);
                ranges.push_back(TReadRange(lowerLimit, upperLimit));
            } else {
                // The case of exact limit.
                ranges.push_back(TReadRange(lowerLimit));
            }
            if (tokenizer.CurrentToken().GetType() == EndRowSelectorToken) {
                finished = true;
            }
            tokenizer.ParseNext();
        }

        attributes->Set("ranges", ConvertToYsonString(ranges));
    }
}

template <class TFunc>
auto RunAttributeAccessor(const TRichYPath& path, const TString& key, TFunc accessor) -> decltype(accessor())
{
    try {
        return accessor();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute %Qv of rich YPath %v",
            key,
            path.GetPath()) << ex;
    }
}

template <class T>
T GetAttribute(const TRichYPath& path, const TString& key, const T& defaultValue)
{
    return RunAttributeAccessor(path, key, [&] () {
        return path.Attributes().Get(key, defaultValue);
    });
}

template <class T>
std::optional<T> FindAttribute(const TRichYPath& path, const TString& key)
{
    return RunAttributeAccessor(path, key, [&] () {
        return path.Attributes().Find<T>(key);
    });
}

TYsonString FindAttributeYson(const TRichYPath& path, const TString& key)
{
    return RunAttributeAccessor(path, key, [&] () {
        return path.Attributes().FindYson(key);
    });
}

} // namespace

TRichYPath TRichYPath::Parse(const TString& str)
{
    auto attributes = CreateEphemeralAttributes();

    auto strWithoutAttributes = ParseAttributes(str, attributes.get());
    TTokenizer ypathTokenizer(strWithoutAttributes);

    while (ypathTokenizer.GetType() != ETokenType::EndOfStream && ypathTokenizer.GetType() != ETokenType::Range) {
        ypathTokenizer.Advance();
    }
    auto path = TYPath(ypathTokenizer.GetPrefix());
    auto rangeStr = ypathTokenizer.GetToken();

    if (ypathTokenizer.GetType() == ETokenType::Range) {
        NYson::TTokenizer ysonTokenizer(rangeStr);
        ysonTokenizer.ParseNext();
        ParseColumns(ysonTokenizer, attributes.get());
        ParseRowRanges(ysonTokenizer, attributes.get());
        ysonTokenizer.CurrentToken().ExpectType(NYson::ETokenType::EndOfStream);
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
    return GetAttribute(*this, "append", false);
}

void TRichYPath::SetAppend(bool value)
{
    Attributes().Set("append", value);
}

bool TRichYPath::GetTeleport() const
{
    return GetAttribute(*this, "teleport", false);
}

bool TRichYPath::GetPrimary() const
{
    return GetAttribute(*this, "primary", false);
}

bool TRichYPath::GetForeign() const
{
    return GetAttribute(*this, "foreign", false);
}

void TRichYPath::SetForeign(bool value)
{
    Attributes().Set("foreign", value);
}

std::optional<std::vector<TString>> TRichYPath::GetColumns() const
{
    if (Attributes().Contains("channel")) {
        THROW_ERROR_EXCEPTION("Deprecated attribute 'channel' in YPath");
    }
    return FindAttribute<std::vector<TString>>(*this, "columns");
}

void TRichYPath::SetColumns(const std::vector<TString>& columns)
{
    Attributes().Set("columns", columns);
}

std::vector<NChunkClient::TReadRange> TRichYPath::GetRanges() const
{
    // COMPAT(ignat): top-level "lower_limit" and "upper_limit" are processed for compatibility.
    auto maybeLowerLimit = FindAttribute<TReadLimit>(*this, "lower_limit");
    auto maybeUpperLimit = FindAttribute<TReadLimit>(*this, "upper_limit");
    auto maybeRanges = FindAttribute<std::vector<TReadRange>>(*this, "ranges");

    if (maybeLowerLimit || maybeUpperLimit) {
        if (maybeRanges) {
            THROW_ERROR_EXCEPTION("YPath cannot be annotated with both multiple (\"ranges\" attribute) "
                "and single (\"lower_limit\" or \"upper_limit\" attributes) ranges");
        }
        return std::vector<TReadRange>({
            TReadRange(
                maybeLowerLimit.value_or(TReadLimit()),
                maybeUpperLimit.value_or(TReadLimit())
            )});
    } else {
        return maybeRanges.value_or(std::vector<TReadRange>({TReadRange()}));
    }
}

void TRichYPath::SetRanges(const std::vector<NChunkClient::TReadRange>& value)
{
    Attributes().Set("ranges", value);
    // COMPAT(ignat)
    Attributes().Remove("lower_limit");
    Attributes().Remove("upper_limit");
}

bool TRichYPath::HasNontrivialRanges() const
{
    auto maybeLowerLimit = FindAttribute<TReadLimit>(*this, "lower_limit");
    auto maybeUpperLimit = FindAttribute<TReadLimit>(*this, "upper_limit");
    auto maybeRanges = FindAttribute<std::vector<TReadRange>>(*this, "ranges");

    return maybeUpperLimit || maybeUpperLimit || maybeRanges;
}

std::optional<TString> TRichYPath::GetFileName() const
{
    return FindAttribute<TString>(*this, "file_name");
}

std::optional<bool> TRichYPath::GetExecutable() const
{
    return FindAttribute<bool>(*this, "executable");
}

TYsonString TRichYPath::GetFormat() const
{
    return FindAttributeYson(*this, "format");
}

std::optional<TTableSchema> TRichYPath::GetSchema() const
{
    return RunAttributeAccessor(*this, "schema", [&] () {
        auto schema = FindAttribute<TTableSchema>(*this, "schema");
        if (schema) {
            ValidateTableSchema(*schema);
        }
        return schema;
    });
}

std::optional<TColumnRenameDescriptors> TRichYPath::GetColumnRenameDescriptors() const
{
    return FindAttribute<TColumnRenameDescriptors>(*this, "rename_columns");
}

TKeyColumns TRichYPath::GetSortedBy() const
{
    return GetAttribute(*this, "sorted_by", TKeyColumns());
}

void TRichYPath::SetSortedBy(const TKeyColumns& value)
{
    if (value.empty()) {
        Attributes().Remove("sorted_by");
    } else {
        Attributes().Set("sorted_by", value);
    }
}

std::optional<i64> TRichYPath::GetRowCountLimit() const
{
    return FindAttribute<i64>(*this, "row_count_limit");
}

std::optional<NTransactionClient::TTimestamp> TRichYPath::GetTimestamp() const
{
    return FindAttribute<NTransactionClient::TTimestamp>(*this, "timestamp");
}

std::optional<NTableClient::EOptimizeFor> TRichYPath::GetOptimizeFor() const
{
    return FindAttribute<NTableClient::EOptimizeFor>(*this, "optimize_for");
}

std::optional<NCompression::ECodec> TRichYPath::GetCompressionCodec() const
{
    return FindAttribute<NCompression::ECodec>(*this, "compression_codec");
}

std::optional<NErasure::ECodec> TRichYPath::GetErasureCodec() const
{
    return FindAttribute<NErasure::ECodec>(*this, "erasure_codec");
}

bool TRichYPath::GetAutoMerge() const
{
    return GetAttribute<bool>(*this, "auto_merge", true);
}

std::optional<NObjectClient::TTransactionId> TRichYPath::GetTransactionId() const
{
    return FindAttribute<NObjectClient::TTransactionId>(*this, "transaction_id");
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TRichYPath& path)
{
    auto keys = path.Attributes().List();
    if (keys.empty()) {
        return path.GetPath();
    }

    return
        TString('<') +
        ConvertToYsonString(path.Attributes()).GetData() +
        TString('>') +
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
        THROW_ERROR_EXCEPTION("YPath can only be parsed from %Qlv but got %Qlv",
            ENodeType::String,
            node->GetType());
    }
    richPath.SetPath(node->GetValue<TString>());
    richPath.Attributes().Clear();
    richPath.Attributes().MergeFrom(node->Attributes());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYPath
} // namespace NYT
