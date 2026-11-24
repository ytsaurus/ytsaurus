#include <yt/yt/flow/extensions/ydb/cpp/ydb_topic_driver.h>

#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/convert.h>

#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <library/cpp/json/json_writer.h>

#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/env.h>

namespace NYT::NFlow::NYdbAuthProbe {

////////////////////////////////////////////////////////////////////////////////

constexpr auto DescribeTimeout = TDuration::Seconds(30);

struct TProbeArguments
{
    std::string Endpoint;
    std::string Database;
    std::string Topic;
    NYson::TYsonString ResourceParameters;
    std::optional<std::string> InjectedUser;
    std::optional<std::string> InjectedPasswordEnvVar;
};

TYdbTopicDriverPtr CreateDriverResource(const TProbeArguments& arguments)
{
    auto context = New<TResourceContext>();
    context->ResourceSpec = New<TResourceSpec>();
    context->ResourceSpec->ResourceClassName = "NYT::NFlow::TYdbTopicDriver";
    context->ResourceSpec->Parameters = NYTree::ConvertTo<NYTree::IMapNodePtr>(arguments.ResourceParameters);
    auto dynamicContext = New<TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = New<TDynamicResourceSpec>();

    std::shared_ptr<NYdb::ICredentialsProviderFactory> injectedCredentials;
    if (arguments.InjectedUser) {
        injectedCredentials = NYdb::CreateLoginCredentialsProviderFactory({
            .User = *arguments.InjectedUser,
            .Password = std::string(GetEnv(TString(*arguments.InjectedPasswordEnvVar))),
        });
    }
    return New<TYdbTopicDriver>(std::move(context), std::move(dynamicContext), std::move(injectedCredentials));
}

NJson::TJsonValue DescribeTopic(const TProbeArguments& arguments)
{
    NJson::TJsonValue result;
    TYdbTopicDriverPtr driver;
    try {
        driver = CreateDriverResource(arguments);
    } catch (const std::exception& ex) {
        result["stage"] = "configure";
        result["error"] = ex.what();
        return result;
    }
    result["stage"] = "describe";
    auto client = driver->CreateTopicClient(arguments.Endpoint, arguments.Database);
    auto future = client->DescribeTopic(arguments.Topic);
    if (!future.Wait(DescribeTimeout)) {
        result["status"] = "PROBE_TIMEOUT";
        return result;
    }
    const auto& describeResult = future.GetValue();
    result["status"] = ToString(describeResult.GetStatus());
    result["issues"] = describeResult.GetIssues().ToString();
    if (describeResult.IsSuccess()) {
        result["partitionCount"] = describeResult.GetTopicDescription().GetPartitions().size();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbAuthProbe

int main(int argc, const char** argv)
{
    using namespace NYT::NFlow::NYdbAuthProbe;
    if (argc != 5 && argc != 7) {
        Cerr << "Usage: " << argv[0]
             << " ENDPOINT DATABASE TOPIC RESOURCE_PARAMETERS [INJECTED_USER INJECTED_PASSWORD_ENV_VAR]" << Endl;
        return 2;
    }
    TProbeArguments arguments{
        .Endpoint = argv[1],
        .Database = argv[2],
        .Topic = argv[3],
        .ResourceParameters = NYT::NYson::TYsonString(TString(argv[4])),
    };
    if (argc == 7) {
        arguments.InjectedUser = argv[5];
        arguments.InjectedPasswordEnvVar = argv[6];
    }
    Cout << NJson::WriteJson(DescribeTopic(arguments), /*formatOutput*/ false) << Endl;
    return 0;
}
