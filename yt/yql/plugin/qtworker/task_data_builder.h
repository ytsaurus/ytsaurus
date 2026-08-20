#pragma once

#include <yt/yql/plugin/plugin.h>

#include <yt/yt/ytlib/yql_client/public.h>

#include <yql/tools/yqlworker/interface/proto/task.pb.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/generic/maybe.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

struct TTaskDataBuildContext
{
    TQueryId QueryId;
    const TString& User;
    const TString& QueryText;
    const NYson::TYsonString& Settings;
    const NYson::TYsonString& Credentials;
    const std::vector<TQueryFile>& Files;
    const TString& FunctionRegistryData;
    const std::optional<NYql::TGatewaysConfig>& GatewaysConfig;
    TMaybe<TString> MaxYqlLangVersion;
    TMaybe<TString> DefaultYqlLangVersion;
    NYqlClient::EQueryType QueryType = NYqlClient::EQueryType::Regular;
};

////////////////////////////////////////////////////////////////////////////////

class ITaskDataBuilder
{
public:
    virtual ~ITaskDataBuilder() = default;

    virtual NYql::NProto::TTaskData Build(const TTaskDataBuildContext& context) = 0;
};

using TBuilderFactory = std::function<std::unique_ptr<ITaskDataBuilder>()>;

////////////////////////////////////////////////////////////////////////////////

void RegisterTaskDataBuilder(const TString& flavor, TBuilderFactory factory);

struct TBuilderRegistrar
{
    TBuilderRegistrar(const TString& flavor, TBuilderFactory factory);
};

////////////////////////////////////////////////////////////////////////////////

//! Throws if no builder is registered for the given flavor.
std::unique_ptr<ITaskDataBuilder> CreateTaskDataBuilder(const TString& flavor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
