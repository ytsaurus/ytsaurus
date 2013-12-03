#pragma once

#include "public.h"

#include <core/misc/rcu_tree.h>

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/row.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TMemoryCompactor
{
public:
    TMemoryCompactor(
        TTabletManagerConfigPtr config,
        TTablet* tablet);

    TStaticMemoryStorePtr Run(
        TDynamicMemoryStorePtr dynamicStore,
        TStaticMemoryStorePtr staticStore);

private:
    class TStaticScanner;
    typedef TRcuTreeScannerPtr<TDynamicRow, NVersionedTableClient::TKeyPrefixComparer> TDynamicScanner;

    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    NVersionedTableClient::TKeyPrefixComparer Comparer_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
