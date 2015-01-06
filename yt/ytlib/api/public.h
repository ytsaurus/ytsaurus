#pragma once

#include <core/misc/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TTransactionStartOptions;
struct TLookupRowsOptions;
struct TSelectRowsOptions;
struct TGetNodeOptions;
struct TSetNodeOptions;
struct TListNodesOptions;
struct TCreateNodeOptions;
struct TLockNodeOptions;
struct TCopyNodeOptions;
struct TMoveNodeOptions;
struct TLinkNodeOptions;
struct TNodeExistsOptions;
struct TCreateObjectOptions;
struct TFileReaderOptions;
struct TFileWriterOptions;

DECLARE_REFCOUNTED_STRUCT(IRowset)

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(ITransaction)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(ITransaction)

DECLARE_REFCOUNTED_STRUCT(IFileReader)
DECLARE_REFCOUNTED_STRUCT(IFileWriter)

DECLARE_REFCOUNTED_STRUCT(IJournalReader)
DECLARE_REFCOUNTED_STRUCT(IJournalWriter)

DECLARE_REFCOUNTED_CLASS(TMasterConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TFileReaderConfig)
DECLARE_REFCOUNTED_CLASS(TFileWriterConfig)
DECLARE_REFCOUNTED_CLASS(TJournalReaderConfig)
DECLARE_REFCOUNTED_CLASS(TJournalWriterConfig)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

