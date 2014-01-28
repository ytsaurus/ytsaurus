#pragma once

#include <core/misc/common.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TColumnFilter;
struct TLookupRowsOptions;

struct IConnection;
typedef TIntrusivePtr<IConnection> IConnectionPtr;

struct IRowset;
typedef TIntrusivePtr<IRowset> IRowsetPtr;

struct IClient;
typedef TIntrusivePtr<IClient> IClientPtr;

struct ITransaction;
typedef TIntrusivePtr<ITransaction> ITransactionPtr;

struct IFileReader;
typedef TIntrusivePtr<IFileReader> IFileReaderPtr;

struct IFileWriter;
typedef TIntrusivePtr<IFileWriter> IFileWriterPtr;

class TConnectionConfig;
typedef TIntrusivePtr<TConnectionConfig> TConnectionConfigPtr;

class TFileReaderConfig;
typedef TIntrusivePtr<TFileReaderConfig> TFileReaderConfigPtr;

class TFileWriterConfig;
typedef TIntrusivePtr<TFileWriterConfig> TFileWriterConfigPtr;

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

