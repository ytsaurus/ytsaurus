#include "name_service.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

namespace NSQLComplete {

    NJson::TJsonValue LoadJsonResource(const TStringBuf filename) {
        TString text;
        Y_ENSURE(NResource::FindExact(filename, &text));
        return NJson::ReadJsonFastTree(text);
    }

    TVector<TString> Names(NJson::TJsonValue::TArray& json) {
        TVector<TString> keys;
        keys.reserve(json.size());
        for (auto& item : json) {
            keys.emplace_back(item.GetMapSafe().at("name").GetStringSafe());
        }
        return keys;
    }

    TVector<TString> ParseTypes(NJson::TJsonValue json) {
        return Names(json.GetArraySafe());
    }

    TVector<TString> ParseFunctions(NJson::TJsonValue json) {
        return Names(json.GetArraySafe());
    }

    NameSet MakeDefaultNameSet() {
        return {
            .Types = ParseTypes(LoadJsonResource("types.json")),
            .Functions = ParseFunctions(LoadJsonResource("sql_functions.json")),
        };
    }

} // namespace NSQLComplete
