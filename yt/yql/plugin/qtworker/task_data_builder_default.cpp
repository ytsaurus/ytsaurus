#include "task_data_builder.h"

#include "helpers.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

namespace NYT::NYqlPlugin {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::optional<TString> ExtractDefaultCluster(const NYql::TGatewaysConfig& config)
{
    if (config.HasYt()) {
        for (const auto& mapping : config.GetYt().GetClusterMapping()) {
            if (mapping.GetDefault()) {
                return mapping.GetName();
            }
        }
    }
    return {};
}

TString SerializeCredentials(const NYson::TYsonString& credentials)
{
    NYql::NProto::TTaskAuthTokens authTokens;

    auto credentialsNode = ConvertToNode(credentials);
    if (credentialsNode->GetType() != ENodeType::Map) {
        return authTokens.SerializeAsString();
    }

    for (const auto& [alias, value] : credentialsNode->AsMap()->GetChildren()) {
        auto valueMap = value->AsMap();
        auto* token = authTokens.AddTokens();
        token->SetAlias(TString(alias));
        token->SetCategory(valueMap->GetChildValueOrDefault<TString>("category", ""));
        token->SetSubcategory(valueMap->GetChildValueOrDefault<TString>("subcategory", ""));
        token->SetContent(valueMap->GetChildValueOrDefault<TString>("content", ""));
    }

    return authTokens.SerializeAsString();
}

////////////////////////////////////////////////////////////////////////////////

class TDefaultTaskDataBuilder
    : public ITaskDataBuilder
{
public:
    NYql::NProto::TTaskData Build(const TTaskDataBuildContext& context) override
    {
        NYql::NProto::TTaskData data;
        data.SetId(ToString(context.QueryId));
        data.SetSyntax((context.QueryType == NYqlClient::EQueryType::UdfMeta)
            ? NYql::NProto::ESyntax::UDF_META
            : NYql::NProto::ESyntax::SQLv1);
        data.SetProgram(context.QueryText);
        data.SetUsername(context.User);
        data.SetResultFormat(NYql::NProto::EDataFormat::YSON_TEXT);
        data.SetAuthData(SerializeCredentials(context.Credentials));
        data.SetIsSystemRequest(false);
        data.SetFunctionRegistryData(context.FunctionRegistryData);
        data.SetPersistedId(true);

        std::optional<TString> defaultTranslationCluster;
        if (context.GatewaysConfig) {
            TString fullTextProto;
            if (!::google::protobuf::TextFormat::PrintToString(*context.GatewaysConfig, &fullTextProto)) {
                ythrow yexception() << "Failed to serialize gateways config to TextProto";
            }

            data.SetGatewaysConfig(fullTextProto);
            defaultTranslationCluster = ExtractDefaultCluster(*context.GatewaysConfig);
        }

        auto settingsMap = ConvertTo<IMapNodePtr>(context.Settings);
        if (auto cluster = settingsMap->FindChildValue<TString>("cluster")) {
            defaultTranslationCluster = *cluster;
        }
        if (context.MaxYqlLangVersion) {
            data.SetMaxLangVer(*context.MaxYqlLangVersion);
        }

        if (auto version = settingsMap->FindChildValue<TString>("yql_version")) {
            data.SetLangVer(*version);
        } else if (context.DefaultYqlLangVersion) {
            data.SetLangVer(*context.DefaultYqlLangVersion);
        }
        if (auto parameters = settingsMap->FindChildValue<TString>("declared_parameters")) {
            data.SetParameters(*parameters);
        }

        if (defaultTranslationCluster) {
            data.SetDefaultTranslationCluster(*defaultTranslationCluster);
            data.SetUrl(*defaultTranslationCluster);
        }
        data.SetRunner("yql-agent");

        for (const auto& file : context.Files) {
            auto* protoFile = data.MutableFiles()->Add();
            protoFile->SetName(TString(file.Name));
            protoFile->SetType(FileTypeToProto(file.Type));
            protoFile->SetContent(TString(file.Content));
        }

        return data;
    }
};

////////////////////////////////////////////////////////////////////////////////

const TBuilderRegistrar DefaultRegistrar(
    TString(DefaultFlavor),
    [] { return std::make_unique<TDefaultTaskDataBuilder>(); });

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
