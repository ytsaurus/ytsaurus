#pragma once

#include <core/misc/common.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TColumnFilter;
struct TLookupOptions;

struct IConnection;
typedef TIntrusivePtr<IConnection> IConnectionPtr;

struct IRowset;
typedef TIntrusivePtr<IRowset> IRowsetPtr;

struct IClient;
typedef TIntrusivePtr<IClient> IClientPtr;

struct ITransaction;
typedef TIntrusivePtr<ITransaction> ITransactionPtr;

class TConnectionConfig;
typedef TIntrusivePtr<TConnectionConfig> TConnectionConfigPtr;

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

