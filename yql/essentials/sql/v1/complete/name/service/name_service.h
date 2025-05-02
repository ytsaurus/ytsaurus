#pragma once

#include <yql/essentials/sql/v1/complete/core/statement.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    using NThreading::TFuture; // TODO(YQL-19747): remove

    struct TIndentifier {
        TString Indentifier;
    };

    struct TNamespaced {
        TString Namespace;
    };

    struct TKeyword {
        TString Content;
    };

    struct TPragmaName: TIndentifier {
        struct TConstraints: TNamespaced {};
    };

    struct TTypeName: TIndentifier {
        struct TConstraints {};
    };

    struct TFunctionName: TIndentifier {
        struct TConstraints: TNamespaced {};
    };

    struct THintName: TIndentifier {
        struct TConstraints {
            EStatementKind Statement;
        };
    };

    struct TFolderName: TIndentifier {
        struct TConstraints {};
    };

    struct TTableName: TIndentifier {
        struct TConstraints {};
    };

    struct TClusterName: TIndentifier {
        struct TConstraints {};
    };

    struct TUnkownName {
        TString Content;
        TString Type;
    };

    using TGenericName = std::variant<
        TKeyword,
        TPragmaName,
        TTypeName,
        TFunctionName,
        THintName,
        TFolderName,
        TTableName,
        TClusterName,
        TUnkownName>;

    struct TNameConstraints {
        TMaybe<TPragmaName::TConstraints> Pragma;
        TMaybe<TTypeName::TConstraints> Type;
        TMaybe<TFunctionName::TConstraints> Function;
        TMaybe<THintName::TConstraints> Hint;
        TMaybe<TFolderName::TConstraints> Folder;
        TMaybe<TTableName::TConstraints> Table;
        TMaybe<TClusterName::TConstraints> Cluster;

        TGenericName Qualified(TGenericName unqualified) const;
        TGenericName Unqualified(TGenericName qualified) const;
        TVector<TGenericName> Qualified(TVector<TGenericName> unqualified) const;
        TVector<TGenericName> Unqualified(TVector<TGenericName> qualified) const;
    };

    struct TNameRequest {
        TVector<TString> Keywords;
        TNameConstraints Constraints;
        TString Prefix = "";
        size_t Limit = 128;

        bool IsEmpty() const {
            return Keywords.empty() &&
                   !Constraints.Pragma &&
                   !Constraints.Type &&
                   !Constraints.Function &&
                   !Constraints.Hint &&
                   !Constraints.Folder &&
                   !Constraints.Table &&
                   !Constraints.Cluster;
        }
    };

    struct TNameResponse {
        TVector<TGenericName> RankedNames;
        TMaybe<size_t> NameHintLength;

        bool IsEmpty() const {
            return RankedNames.empty();
        }
    };

    class INameService: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<INameService>;

        virtual TFuture<TNameResponse> Lookup(TNameRequest request) const = 0;
        virtual ~INameService() = default;
    };

    TString NormalizeName(TStringBuf name);

} // namespace NSQLComplete
