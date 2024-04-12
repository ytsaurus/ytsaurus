#include "helpers.h"

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/misc/error.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NOrm::NAttributes {

using namespace NYPath;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

const TProtobufMessageType* GetMessageTypeByYPath(
    const TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    bool allowAttributeDictionary)
{
    auto result = ResolveProtobufElementByYPath(rootType, path);
    auto* messageElement = std::get_if<std::unique_ptr<TProtobufMessageElement>>(&result.Element);
    if (messageElement) {
        return (*messageElement)->Type;
    }
    if (allowAttributeDictionary) {
        auto* attributeDictionaryElement =
            std::get_if<std::unique_ptr<TProtobufAttributeDictionaryElement>>(&result.Element);
        if (attributeDictionaryElement) {
            return (*attributeDictionaryElement)->Type;
        }
    }
    THROW_ERROR_EXCEPTION("Attribute %v is not a protobuf message",
        result.HeadPath);
}

NYTree::INodePtr ConvertProtobufToNode(
    const TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    const TString& payload)
{
    const auto* payloadType = GetMessageTypeByYPath(rootType, path, /*allowAttributeDictionary*/ false);
    google::protobuf::io::ArrayInputStream protobufInputStream(payload.data(), payload.length());

    auto builder = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
    builder->BeginTree();
    ParseProtobuf(&*builder, &protobufInputStream, payloadType);
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

TYsonStringWriterHelper::TYsonStringWriterHelper(EYsonFormat format, EYsonType type)
    : Output_(ValueString_)
    , Writer_(CreateYsonWriter(&Output_, format, type, /*enableRaw*/ format == EYsonFormat::Binary))
{ }

IYsonConsumer* TYsonStringWriterHelper::GetConsumer()
{
    return Writer_.get();
}

TYsonString TYsonStringWriterHelper::Flush()
{
    Writer_->Flush();
    auto result = TYsonString(ValueString_);
    ValueString_.clear();
    return result;
}

bool TYsonStringWriterHelper::IsEmpty() const
{
    return ValueString_.Empty();
}

////////////////////////////////////////////////////////////////////////////////

void TIndexParseResult::EnsureIndexType(EListIndexType indexType, TStringBuf path)
{
    THROW_ERROR_EXCEPTION_UNLESS(IndexType == indexType,
        "Error traversing path %Qv: index token must be %v",
        path,
        indexType);
}

void TIndexParseResult::EnsureIndexIsWithinBounds(i64 count, TStringBuf path)
{
    THROW_ERROR_EXCEPTION_IF(IsOutOfBounds(count),
        "Repeated field index at %Qv must be in range [-%v, %v), but got %v",
        path,
        count,
        count,
        Index);
}

bool TIndexParseResult::IsOutOfBounds(i64 count)
{
    return Index < 0 || Index >= count;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAttributesDetectingConsumer
    : public NYson::IYsonConsumer
{
public:
    TAttributesDetectingConsumer(std::function<void()> reporter)
        : Reporter_(std::move(reporter))
    { }

    void OnStringScalar(TStringBuf /*string*/) override final
    { };

    void OnBeginList() override final
    { };

    void OnListItem() override final
    { };

    void OnEndList() override final
    { };

    void OnBeginMap() override final
    { };

    void OnKeyedItem(TStringBuf /*key*/) override final
    { };

    void OnEndMap() override final
    { };

    void OnInt64Scalar(i64 /*value*/) override final
    { };

    void OnUint64Scalar(ui64 /*value*/) override final
    { };

    void OnDoubleScalar(double /*value*/) override final
    { };

    void OnBooleanScalar(bool /*value*/) override final
    { };

    void OnEntity() override final
    { };

    void OnRaw(TStringBuf /*yson*/, NYson::EYsonType /*type*/) override final
    { };

    void OnBeginAttributes() override final
    {
        Reporter_();
    };

    void OnEndAttributes() override final
    { };

private:
    const std::function<void()> Reporter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateAttributesDetectingConsumer(std::function<void()> callback)
{
    return std::make_unique<TAttributesDetectingConsumer>(std::move(callback));
}

////////////////////////////////////////////////////////////////////////////////

TIndexParseResult ParseListIndex(TStringBuf token, i64 count)
{
    auto parseAbsoluteIndex = [count] (TStringBuf token) {
        i64 index = NYPath::ParseListIndex(token);
        if (index < 0) {
            index += count;
        }
        return index;
    };

    if (token == NYPath::ListBeginToken) {
        return TIndexParseResult{
            .Index = 0,
            .IndexType = EListIndexType::Relative
        };
    } else if (token == NYPath::ListEndToken) {
        return TIndexParseResult{
            .Index = count,
            .IndexType = EListIndexType::Relative
        };
    } else if (token.StartsWith(NYPath::ListBeforeToken) || token.StartsWith(NYPath::ListAfterToken)) {
        auto index = parseAbsoluteIndex(NYPath::ExtractListIndex(token));

        if (token.StartsWith(NYPath::ListAfterToken)) {
            ++index;
        }
        return TIndexParseResult{
            .Index = index,
            .IndexType = EListIndexType::Relative
        };
    } else {
        return TIndexParseResult{
            .Index = parseAbsoluteIndex(token),
            .IndexType = EListIndexType::Absolute
        };
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
