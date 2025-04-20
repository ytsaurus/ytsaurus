#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    // `Path` structure is defined by a `System`.
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

        // Can end with a folder entry name hint.
        // For example, `/local/exa` lists a folder `/local`,
        // but can rank and filter entries by a hint `exa`.
        const TPath& Path;

        const TListFilter& Filter;
        size_t Limit;
    };

    struct TListResponse {
        size_t NameHintLength;
        TVector<TFolderEntry> Entries;
    };

    class ISchema {
    public:
        using TPtr = THolder<ISchema>;

        ~ISchema() = default;
        virtual NThreading::TFuture<TListResponse> List(const TListRequest& request) = 0;
    };

} // namespace NSQLComplete
