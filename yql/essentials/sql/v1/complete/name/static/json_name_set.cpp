#include "name_service.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>

namespace NSQLComplete {

    NJson::TJsonValue LoadJsonResource(const TStringBuf filename) {
        TString text;
        Y_ENSURE(NResource::FindExact(filename, &text));
        return NJson::ReadJsonFastTree(text);
    }

    TVector<TString> Keys(NJson::TJsonValue::TMapType& json) {
        TVector<TString> keys;
        for (auto& [k, v] : json) {
            keys.emplace_back(k);
        }
        return keys;
    }

    TVector<TString> ParseTypes(NJson::TJsonValue json) {
        return Keys(json.GetMapSafe());
    }

    TVector<TString> ParseFunctions(NJson::TJsonValue json) {
        return Keys(json.GetMapSafe());
    }

    NameSet MakeDefaultNameSet() {
        return {
            .Types = ParseTypes(LoadJsonResource("types.json")),
            .Functions = ParseFunctions(LoadJsonResource("sql_functions.json")),
        };
    }

} // namespace NSQLComplete
