#pragma once

#include "public.h"

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NSequoiaClient {

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

} // namespace NYT::NSequoiaClient
