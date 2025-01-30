#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

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

    operator size_t() const;
    bool operator==(const TSequoiaTablePathDescriptor&) const = default;
};

////////////////////////////////////////////////////////////////////////////////

struct ITableDescriptor
{
    virtual ~ITableDescriptor() = default;

    virtual const TString& GetTableName() const = 0;

    virtual const NTableClient::IRecordDescriptor* GetRecordDescriptor() const = 0;
    virtual const NQueryClient::TColumnEvaluatorPtr& GetColumnEvaluator() const = 0;

    static const ITableDescriptor* Get(ESequoiaTable table);
};

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetSequoiaTablePath(
    const NApi::NNative::IClientPtr& client,
    const TSequoiaTablePathDescriptor& tablePathDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
