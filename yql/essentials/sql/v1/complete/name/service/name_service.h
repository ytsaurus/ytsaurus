#pragma once

#include <yql/essentials/sql/v1/complete/name/name.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    struct TNameRequest {
        TVector<TString> Keywords;
        struct {
            TMaybe<TPragmaName::TConstraints> Pragma;
            TMaybe<TTypeName::TConstraints> Type;
            TMaybe<TFunctionName::TConstraints> Function;
            TMaybe<THintName::TConstraints> Hint;
            TMaybe<TTableName::TConstraints> Table;
        } Constraints;
        TString Prefix = "";
        size_t Limit = 128;

        bool IsEmpty() const {
            return Keywords.empty() &&
                   !Constraints.Pragma &&
                   !Constraints.Type &&
                   !Constraints.Function &&
                   !Constraints.Hint &&
                   !Constraints.Table;
        }
    };

    struct TNameResponse {
        TVector<TGenericName> RankedNames;
    };

    class INameService {
    public:
        using TPtr = THolder<INameService>;

        virtual NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) = 0;
        virtual ~INameService() = default;
    };

} // namespace NSQLComplete
