#pragma once

#include "auth_token.h"
#include "document.h"
#include "objects.h"
#include "path.h"
#include "range_filter.h"
#include "table_partition.h"
#include "table_reader.h"
#include "table_schema.h"

#include <util/generic/string.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>

#include <memory>
#include <vector>

namespace NInterop {

using TStringList = std::vector<TString>;

////////////////////////////////////////////////////////////////////////////////

struct TObjectListItem
{
    TString Name;
    TObjectAttributes Attributes;
};

using TObjectList = std::vector<TObjectListItem>;

////////////////////////////////////////////////////////////////////////////////

class IStorage
{
public:
    virtual ~IStorage() = default;

    // Related services

    virtual const IPathService* PathService() = 0;

    virtual IAuthorizationTokenService* AuthTokenService() = 0;

    // Access data / metadata

    virtual TTableList ListTables(
        const IAuthorizationToken& token,
        const TString& path = {},
        bool recursive = false) = 0;

    virtual TTablePtr GetTable(
        const IAuthorizationToken& token,
        const TString& name) = 0;

    virtual TTableList GetTables(const TString& jobSpec) = 0;

    virtual TTablePartList GetTableParts(
        const IAuthorizationToken& token,
        const TString& name,
        const IRangeFilterPtr& rangeFilter = nullptr,
        size_t maxParts = 1) = 0;

    virtual TTablePartList ConcatenateAndGetTableParts(
        const IAuthorizationToken& token,
        const std::vector<TString> names,
        const IRangeFilterPtr& rangeFilter = nullptr,
        size_t maxParts = 1) = 0;

    virtual TTableReaderList CreateTableReaders(
        const IAuthorizationToken& token,
        const TString& jobSpec,
        const TStringList& columns,
        const TSystemColumns& systemColumns,
        size_t maxStreamCount,
        const TTableReaderOptions& options) = 0;

    virtual ITableReaderPtr CreateTableReader(
        const IAuthorizationToken& token,
        const TString& name,
        const TTableReaderOptions& options) = 0;

    virtual TString ReadFile(
        const IAuthorizationToken& token,
        const TString& name) = 0;

    virtual IDocumentPtr ReadDocument(
        const IAuthorizationToken& token,
        const TString& name) = 0;

    virtual bool Exists(
        const IAuthorizationToken& token,
        const TString& name) = 0;

    virtual TObjectList ListObjects(
        const IAuthorizationToken& token,
        const TString& path) = 0;

    virtual TObjectAttributes GetObjectAttributes(
        const IAuthorizationToken& token,
        const TString& path) = 0;

    // We still need this for effective polling through metadata cache
    // TODO: replace by CreateObjectPoller

    virtual TMaybe<TRevision> GetObjectRevision(
        const IAuthorizationToken& token,
        const TString& name,
        bool throughCache) = 0;
};

using IStoragePtr = std::shared_ptr<IStorage>;

}   // namespace NInterop
