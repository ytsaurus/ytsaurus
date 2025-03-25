#include "name_service.h"

#include <yql/essentials/public/types/yql_types.pb.h>

namespace NSQLComplete {

    TVector<TString> GetTypeNames() {
        const auto* enumDesc = google::protobuf::GetEnumDescriptor<NYql::NProto::TypeIds>();

        TVector<TString> names;
        for (int i = 0; i < enumDesc->value_count(); ++i) {
            const auto* valueDesc = enumDesc->value(i);

            const auto name = valueDesc->name();
            if (name == "UNUSED") {
                continue;
            }

            names.emplace_back(name);
        }
        return names;
    }

    NameSet MakeDefaultNameSet() {
        return {
            .Types = GetTypeNames(),
        };
    }

} // namespace NSQLComplete
