#pragma once

#include <core/misc/common.h>

#include <ytlib/election/public.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellGuid;
using NElection::NullCellGuid;

using NTabletClient::TTabletCellId;
using NTabletClient::TTabletId;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;
using NTransactionClient::LastCommittedTimestamp;

////////////////////////////////////////////////////////////////////////////////
    
class TTransactionManagerConfig;
typedef TIntrusivePtr<TTransactionManagerConfig> TTransactionManagerConfigPtr;

class TTabletManagerConfig;
typedef TIntrusivePtr<TTabletManagerConfig> TTabletManagerConfigPtr;

class TTabletNodeConfig;
typedef TIntrusivePtr<TTabletNodeConfig> TTabletNodeConfigPtr;

class TTabletCellController;
typedef TIntrusivePtr<TTabletCellController> TTabletCellControllerPtr;

class TTabletSlot;
typedef TIntrusivePtr<TTabletSlot> TTabletSlotPtr;

class TTabletAutomaton;
typedef TIntrusivePtr<TTabletAutomaton> TTabletAutomatonPtr;

class TSaveContext;
class TLoadContext;

class TTabletManager;
typedef TIntrusivePtr<TTabletManager> TTabletManagerPtr;

class TTransactionManager;
typedef TIntrusivePtr<TTransactionManager> TTransactionManagerPtr;

class TTabletService;
typedef TIntrusivePtr<TTabletService> TTabletServicePtr;

class TTablet;
class TTransaction;

struct IStore;
typedef TIntrusivePtr<IStore> IStorePtr;

struct IStoreScanner;

class TDynamicMemoryStore;
typedef TIntrusivePtr<TDynamicMemoryStore> TDynamicMemoryStorePtr;

class TStaticMemoryStoreBuilder;

class TStaticMemoryStore;
typedef TIntrusivePtr<TStaticMemoryStore> TStaticMemoryStorePtr;

class TStoreManager;
typedef TIntrusivePtr<TStoreManager> TStoreManagerPtr;

struct TDynamicRowHeader;
class TDynamicRow;
struct TDynamicRowRef;

struct TEditListHeader;
template <class T>
class TEditList;
typedef TEditList<NVersionedTableClient::TVersionedValue> TValueList;
typedef TEditList<NVersionedTableClient::TTimestamp> TTimestampList;

struct TStaticRowHeader;
class TStaticRow;

class TMemoryCompactor;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
