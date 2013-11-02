#pragma once

#include <core/misc/common.h>

#include <ytlib/election/public.h>

#include <ytlib/tablet_client/public.h>

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

class TMemoryTable;
typedef TIntrusivePtr<TMemoryTable> TMemoryTablePtr;

struct TRowGroupHeader;
class TRowGroup;

struct TRowGroupItemHeader;
class TRowGroupItem;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
