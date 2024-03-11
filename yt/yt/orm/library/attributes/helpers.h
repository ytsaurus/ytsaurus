#pragma once

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

const NYson::TProtobufMessageType* GetMessageTypeByYPath(
    const NYson::TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    bool allowAttributeDictionary);

NYTree::INodePtr ConvertProtobufToNode(
    const NYson::TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    const TString& payload);

////////////////////////////////////////////////////////////////////////////////

class TYsonStringWriterHelper
{
public:
    TYsonStringWriterHelper(
        NYson::EYsonFormat format = NYson::EYsonFormat::Binary,
        NYson::EYsonType type = NYson::EYsonType::Node);

    NYson::IYsonConsumer* GetConsumer();
    NYson::TYsonString Flush();
    bool IsEmpty() const;

private:
    TString ValueString_;
    TStringOutput Output_;
    std::unique_ptr<NYson::IFlushableYsonConsumer> Writer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
