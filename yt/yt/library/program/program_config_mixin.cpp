#include "program_config_mixin.h"

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/yaml/config.h>
#include <yt/yt/core/yaml/parser.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <util/folder/path.h>

#include <util/stream/file.h>

#include <util/string/ascii.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::optional<EConfigFormat> DeriveConfigFormatFromFileName(TStringBuf path)
{
    auto extension = TFsPath(TString(path)).GetExtension();
    if (AsciiEqualsIgnoreCase(extension, "yson")) {
        return EConfigFormat::Yson;
    } else if (AsciiEqualsIgnoreCase(extension, "json")) {
        return EConfigFormat::Json;
    } else if (AsciiEqualsIgnoreCase(extension, "yaml") || AsciiEqualsIgnoreCase(extension, "yml")) {
        return EConfigFormat::Yaml;
    }
    return std::nullopt;
}

NYTree::INodePtr ParseConfigNode(const std::string& path, EConfigFormat format)
{
    // TODO(babenko): migrate to std::string
    TIFStream stream{TString(path)};

    switch (format) {
        case EConfigFormat::Yson:
            return NYTree::ConvertToNode(&stream);

        case EConfigFormat::Json:
        case EConfigFormat::Yaml: {
            auto builder = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
            builder->BeginTree();
            switch (format) {
                case EConfigFormat::Json:
                    NJson::ParseJson(
                        &stream,
                        builder.get(),
                        New<NJson::TJsonFormatConfig>(),
                        NYson::EYsonType::Node);
                    break;

                case EConfigFormat::Yaml:
                    NYaml::ParseYaml(
                        &stream,
                        builder.get(),
                        New<NYaml::TYamlFormatConfig>(),
                        NYson::EYsonType::Node);
                    break;

                default:
                    YT_ABORT();
            }
            return builder->EndTree();
        }
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
