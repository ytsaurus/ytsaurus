#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/tablet_helpers.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaTablePathDescriptor
{
    ESequoiaTable Table;
    // Used to create Sequoia table name for tables that are automatically created
    // for each master cell (refresh queues for now).
    std::optional<NObjectClient::TCellTag> MasterCellTag;

    bool operator==(const TSequoiaTablePathDescriptor&) const = default;
};

////////////////////////////////////////////////////////////////////////////////

struct ITableDescriptor
{
    virtual ~ITableDescriptor() = default;

    virtual const TString& GetTableName() const = 0;

    virtual bool IsSorted() const = 0;

    virtual const NTableClient::IRecordDescriptor* GetRecordDescriptor() const = 0;
    virtual const NQueryClient::TColumnEvaluatorPtr& GetColumnEvaluator() const = 0;

    virtual const NTableClient::TTableSchemaPtr& GetPrimarySchema() const = 0;
    virtual const NTableClient::TTableSchemaPtr& GetWriteSchema() const = 0;
    virtual const NTableClient::TTableSchemaPtr& GetDeleteSchema() const = 0;
    virtual const NTableClient::TTableSchemaPtr& GetLockSchema() const = 0;

    virtual const NTableClient::TNameTableToSchemaIdMapping& GetNameTableToPrimarySchemaIdMapping() const = 0;
    virtual const NTableClient::TNameTableToSchemaIdMapping& GetNameTableToWriteSchemaIdMapping() const = 0;
    virtual const NTableClient::TNameTableToSchemaIdMapping& GetNameTableToDeleteSchemaIdMapping() const = 0;
    virtual const NTableClient::TNameTableToSchemaIdMapping& GetNameTableToLockSchemaIdMapping() const = 0;

    /// Schedules table descriptor initialization for all Sequoia tables.
    /*!
     *  Initialization may take seconds. It's better do it somewhere in process'
     *  bootstrap rather than rely on lazy initialization at some unexpected
     *  moment.
     */
    static void ScheduleInitialization();
    static const ITableDescriptor* Get(ESequoiaTable table);
};

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetSequoiaTablePath(
    const NApi::NNative::IClientPtr& client,
    const TSequoiaTablePathDescriptor& tablePathDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NSequoiaClient::TSequoiaTablePathDescriptor>
{
    size_t operator()(const NYT::NSequoiaClient::TSequoiaTablePathDescriptor& descriptor) const;
};

////////////////////////////////////////////////////////////////////////////////
