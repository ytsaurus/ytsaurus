#pragma once

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <string>

namespace NYT::NFlow::NJobtruck {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf LogsFormatVersionAttribute = "yt_logs_format_version";
inline constexpr int CurrentLogsFormatVersion = 1;

struct TSourceMeta
    : public NYTree::TYsonStruct
{
    std::string Kind;
    std::string DisplayName;
    NYTree::INodePtr NodeInfo;

    REGISTER_YSON_STRUCT(TSourceMeta);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSourceMeta);

struct TLogConfig
    : public NYTree::TYsonStruct
{
    std::string LogLevel;

    REGISTER_YSON_STRUCT(TLogConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLogConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NJobtruck
