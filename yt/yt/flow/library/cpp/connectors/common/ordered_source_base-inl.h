#pragma once

#ifndef ORDERED_SOURCE_BASE_INL_H_
    #error "Direct inclusion of this file is not allowed, include ordered_source_base.h"
    // For the sake of sane code completion.
    #include "ordered_source_base.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class EPolicy>
NYson::TYsonString MakeMalformedMessagePolicyReadSettings(EPolicy policy)
{
    // clang-format off
    return NYTree::BuildYsonStringFluently()
        .BeginMap()
            .Item(MalformedMessagePolicyReadSetting).Value(policy)
        .EndMap();
    // clang-format on
}

template <class EPolicy>
EPolicy ParseMalformedMessagePolicyReadSettings(const NYson::TYsonString& settings, EPolicy defaultPolicy)
{
    if (!settings) {
        return defaultPolicy;
    }

    auto node = NYTree::ConvertToNode(settings);
    if (node->GetType() != NYTree::ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Unknown value of read settings")
            .With("settings", NYson::ConvertToYsonString(settings, NYson::EYsonFormat::Text));
    }

    auto child = node->AsMap()->FindChild(std::string(MalformedMessagePolicyReadSetting));
    if (!child) {
        return defaultPolicy;
    }

    try {
        return NYTree::ConvertTo<EPolicy>(child);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Unknown value of read setting %Qv",
            MalformedMessagePolicyReadSetting)
            .With(TError(ex));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
