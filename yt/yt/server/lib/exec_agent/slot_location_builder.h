#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct TSlotConfig
    : public NYTree::TYsonSerializable
{
    int Index;

    std::optional<int> Uid;

    TSlotConfig();
};

DEFINE_REFCOUNTED_TYPE(TSlotConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSlotLocationBuilderConfig
    : public NYTree::TYsonSerializable
{
    TString LocationPath;

    int NodeUid;

    std::vector<TSlotConfigPtr> SlotConfigs;

    TSlotLocationBuilderConfig();
};

DEFINE_REFCOUNTED_TYPE(TSlotLocationBuilderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSlotLocationBuilderTool
{
    void operator()(const TSlotLocationBuilderConfigPtr& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
