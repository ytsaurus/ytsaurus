#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    using TPath = TString;

    using TObjectType = TString;

    constexpr struct {
        const char* Folder = "Folder";
        const char* Table = "Table";
    } ObjectType;

    struct TFolderEntry {
        TString Name;
        TObjectType Type;
    };

    struct TListFilter {
        TMaybe<THashSet<TObjectType>> Types;
    };

    struct TListRequest {
        const TString& System;
        const TPath& Path;
        const TListFilter& Filter;
        size_t Limit;
    };

    struct TListResponse {
        size_t NameLength;
        TVector<TFolderEntry> Entries;
    };

    class ISchema {
    public:
        ~ISchema() = default;

        virtual NThreading::TFuture<TListResponse> List(const TListRequest& request) = 0;
    };

} // namespace NSQLComplete
