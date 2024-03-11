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

} // namespace NYT::NOrm::NAttributes
