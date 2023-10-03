#pragma once

#include <library/cpp/json/json_writer.h>

#include <util/digest/sequence.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NYa {
    class TPlatformMappingError: public yexception {
    };

    constexpr TStringBuf ANY_PLATFORM_UC{"ANY"};

    struct TResourceDesc {
        TString Uri;
        ui32 StripPrefix = 0;
    };

    /// These are maps CANONICAL_PLATFORM_NAME -> RESOURCE_URI
    /// These are ordered maps to ensure stable serialization and hashing
    using TPlatformMap = TMap<TString, TResourceDesc, std::less<>>;
    using TTempPlatformMap = TMap<TStringBuf, TResourceDesc, std::less<>>;

    /// Create owning platform map from `resource.json` format, that is used
    /// in ya.conf.json and ymake's `DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON` macro
    /// Expected JSON is:
    /// ```
    /// { "by_platform" : {
    ///        PLATFORM1 : {"uri": uri1},
    ///        PLATFORM2 : {"uri": uri2, "strip_prefix": num2}
    /// }}
    /// ```
    /// Where strip_prefix is optional, and 0 by default
    TPlatformMap MappingFromJsonString(TStringBuf mapping);

    /// Get variable name to refer the mapping.
    /// The name is used in commaned of json-graph and as "pattern" in resource description
    template <typename PlatformMapT>
    TString MappingVarName(TStringBuf baseName, const PlatformMapT& mapping) {
         return TString::Join(baseName, "-", ToString<ui32>(ComputeHash(mapping)));
    }

    /// Produce mapping description for JSON-graph
    /// This includes "pattern" for varName and "platform" for mapping itself
    /// The resulting JSON looks like this:
    /// ```
    /// { "pattern" : VAR_NAME_WITH_HASH
    ///   "resources": [
    ///        {"platform": PLATFORM1, "resource": uri1},
    ///        {"platform": PLATFORM2, "resource": uri2, "strip_preifx": num2},
    ///   ]
    /// }
    /// ```
    /// Where strip_prefix is added only if it is non-zero
    template <typename PlatformMapT>
    void MappingPatternToJson(TStringBuf varNameWithHash, const PlatformMapT& mapping, NJson::TJsonWriter& writer) {
        writer.OpenMap();
        writer.Write("pattern", varNameWithHash);
        writer.OpenArray("resources");
        for (const auto& [platform, res]: mapping) {
            writer.OpenMap();
            writer.Write("platform", platform);
            writer.Write("resource", res.Uri);
            if (res.StripPrefix)
                writer.Write("strip_prefix", res.StripPrefix);
            writer.CloseMap();
        }
        writer.CloseArray();
        writer.CloseMap();
        writer.Flush();
    }


    /// Produce mapping description for JSON-graph as string
    /// The exact format see in comment to `MappingPatternToJson`
    template <typename PlatformMapT>
    TString MappingPatternToJsonString(TStringBuf varNameWithHash, const PlatformMapT& mapping) {
        TStringStream ss;
        NJson::TJsonWriter writer(&ss, false, false, false);
        MappingPatternToJson(varNameWithHash, mapping, writer);
        return ss.Str();
    }

    inline void ResourceToJson(TStringBuf varNameWithUri, const TResourceDesc& res, NJson::TJsonWriter& writer) {
        writer.OpenMap();
        writer.Write("pattern", varNameWithUri);
        writer.Write("resource", res.Uri);
        if (res.StripPrefix)
            writer.Write("strip_prefix", res.StripPrefix);
        writer.CloseMap();
        writer.Flush();
    }

    inline TString ResourceToJsonString(TStringBuf varNameWithUri, const TResourceDesc& res) {
        TStringStream ss;
        NJson::TJsonWriter writer(&ss, false, false, false);
        ResourceToJson(varNameWithUri, res, writer);
        return ss.Str();
    }

    TString ResourceDirName(const NYa::TResourceDesc& desc);
    TString ResourceDirName(TStringBuf uri, ui32 stripPrefix);

    TString ResourceVarName(TStringBuf baseName, const NYa::TResourceDesc& desc);
    TString ResourceVarName(TStringBuf baseName, TStringBuf uri, ui32 stripPrefix);
}

template<>
struct THash<NYa::TResourceDesc> {
    ui64 operator()(const NYa::TResourceDesc& desc) const {
         return THash<TStringBuf>()(desc.Uri) ^ desc.StripPrefix;
    }
};

template<>
struct THash<NYa::TPlatformMap>: TSimpleRangeHash {
};

template<>
struct THash<NYa::TTempPlatformMap>: TSimpleRangeHash {
};

