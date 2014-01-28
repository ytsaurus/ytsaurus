#pragma once

#include <core/misc/common.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TColumnFilter;
struct TLookupRowsOptions;

DECLARE_REFCOUNTED_STRUCT(IRowset)

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(ITransaction)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(ITransaction)

DECLARE_REFCOUNTED_STRUCT(IFileReader)
DECLARE_REFCOUNTED_STRUCT(IFileWriter)

DECLARE_REFCOUNTED_CLASS(TConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TFileReaderConfig)
DECLARE_REFCOUNTED_CLASS(TFileWriterConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

