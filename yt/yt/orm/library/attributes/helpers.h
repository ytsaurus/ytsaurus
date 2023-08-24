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

} // namespace NYT::NOrm::NAttributes
