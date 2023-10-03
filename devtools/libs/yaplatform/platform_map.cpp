#include "platform_map.h"
#include "platform.h"

#include <library/cpp/json/json_reader.h>


namespace NYa {
    TPlatformMap MappingFromJsonString(TStringBuf mapping) {
        NJson::TJsonValue jsonConfig;
        try {
            NJson::ReadJsonTree(mapping, &jsonConfig, true);
        } catch (const yexception& e) {
            throw TPlatformMappingError() << "Cannot parse json: " << e.what();
        }

        NJson::TJsonValue byPlatform;
        if (!jsonConfig.GetValue("by_platform", &byPlatform) || !byPlatform.IsMap()) {
            throw TPlatformMappingError() << "'by_platform' is not found or it's value is not an object";
        }

        TPlatformMap result;
        for (const auto& [platform, dest] : byPlatform.GetMap()) {
            NJson::TJsonValue uri;
            if (!dest.GetValue("uri", &uri) || !uri.IsString()) {
                throw TPlatformMappingError() << "Wrong platform '" << platform << "' description: 'uri' is not found or it's value is not a string";
            }
            TString canonizedPlatform = NYa::CanonizePlatform(platform).AsString();
            NJson::TJsonValue stripPrefixVal;
            unsigned long long stripPrefix = 0;
            if (dest.GetValue("strip_prefix", &stripPrefixVal)) {
                if (!stripPrefixVal.IsUInteger()) {
                    throw TPlatformMappingError() << "Wrong platform '" << platform << "' description: 'strip_prefix' value should be integral";
                }
                stripPrefix = stripPrefixVal.GetUInteger();
                if (stripPrefix > Max<ui16>()) {
                    throw TPlatformMappingError() << "Wrong platform '" << platform << "' description: 'strip_prefix' value of " << stripPrefix << " is too big";
                }
            }
            result.emplace(canonizedPlatform, TResourceDesc{uri.GetString(), static_cast<ui32>(stripPrefix)});
        }

        if (result.empty()) {
            throw TPlatformMappingError() << "Platform mapping is empty";
        }

        return result;
    }

    TString ResourceDirName(const NYa::TResourceDesc& desc) {
        return ResourceDirName(desc.Uri, desc.StripPrefix);
    }

    TString ResourceDirName(TStringBuf uri, ui32 stripPrefix) {
        constexpr TStringBuf sbrPrefix{"sbr:"};
        constexpr TStringBuf httpPrefix{"https:"};

        TString dirName;
        if (uri.StartsWith(sbrPrefix)) {
            dirName = uri.substr(sbrPrefix.size());
        } else if (uri.StartsWith(httpPrefix)) {
            size_t pos = uri.rfind('#');
            if (pos == std::string::npos) {
                throw yexception() << "Wrong uri. No '#' symbol is found: " << uri;
            } else if (pos == uri.size() - 1) {
                throw yexception() << "Wrong uri. '#' symbol must be followed by resource md5: " << uri;
            }
            dirName = uri.substr(pos + 1);
        } else {
            throw yexception() << "Wrong uri. No known schema is found: " << uri;
        }
        if (stripPrefix) {
            dirName += "-" + ToString(stripPrefix);
        }
        return dirName;
    }

    TString ResourceVarName(TStringBuf baseName, const NYa::TResourceDesc& desc) {
        return ResourceVarName(baseName, desc.Uri, desc.StripPrefix);
    }

    TString ResourceVarName(TStringBuf baseName, TStringBuf uri, ui32 stripPrefix) {
        TString varName = TString::Join(baseName, "-", uri);
        if (stripPrefix) {
            varName += "-" + ToString(stripPrefix);
        }
        return varName;
    }
}
