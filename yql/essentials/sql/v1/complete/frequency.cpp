#include "frequency.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

#include <util/string/ascii.h>
#include <util/string/split.h>

namespace NSQLComplete {

    constexpr const char* JsonKeyParent = "parent";
    constexpr const char* JsonKeyRule = "rule";
    constexpr const char* JsonKeySum = "sum";

    constexpr const char* JsonParentFunc = "FUNC";
    constexpr const char* JsonParentModule = "MODULE";
    constexpr const char* JsonParentModuleFunc = "MODULE_FUNC";

    struct TFrequencyItem {
        TString Parent;
        TString Rule;
        size_t Sum;

        static TFrequencyItem FromJson(NJson::TJsonValue::TMapType&& jMap) {
            return {
                .Parent = jMap.at(JsonKeyParent).GetStringSafe(),
                .Rule = jMap.at(JsonKeyRule).GetStringSafe(),
                .Sum = jMap.at(JsonKeySum).GetUIntegerSafe(),
            };
        }
    };

    TVector<TFrequencyItem> LoadFrequencyItems(const TStringBuf text) {
        NJson::TJsonValue json = NJson::ReadJsonFastTree(text);

        TVector<TFrequencyItem> items;

        for (auto& jVal : json.GetArraySafe()) {
            auto item = TFrequencyItem::FromJson(std::move(jVal.GetMapSafe()));
            items.emplace_back(std::move(item));
        }

        return items;
    }

    void ToLowerInplace(TString& text) {
        for (char& ch : text) {
            ch = ToLower(ch);
        }
    }

    TFrequencyData LoadFrequencyData() {
        TString text;
        Y_ENSURE(NResource::FindExact("rules_corr_basic.json", &text));

        TFrequencyData data;

        for (auto& item : LoadFrequencyItems(text)) {
            if (item.Parent == JsonParentFunc || item.Parent == JsonParentModuleFunc) {
                auto& name = item.Rule;
                ToLowerInplace(name);
                data.Functions[name] += item.Sum;
            } else if (item.Parent == JsonParentModule) {
                // Modules are not supported yet
            } else {
                // Parser Call Stacks are not supported yet
            }
        }

        return data;
    }

}
