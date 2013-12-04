#include "stdafx.h"
#include "compaction.h"
#include "config.h"
#include "tablet.h"
#include "static_memory_store.h"
#include "dynamic_memory_store.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMemoryCompactor::TRowCombiner
{
public:
    TRowCombiner(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        TStaticMemoryStoreBuilder* builder)
        : Config_(config)
        , Tablet_(tablet)
        , Builder_(builder)
        , KeyCount_(static_cast<int>(Tablet_->KeyColumns().size()))
        , SchemaColumnCount_(static_cast<int>(Tablet_->Schema().Columns().size()))
    { }

    void BeginRow()
    {

    }

    void Combine(const IStoreScanner* scanner)
    {

    }

    void EndRow()
    {

    }

private:
    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;
    TStaticMemoryStoreBuilder* Builder_;

    int KeyCount_;
    int SchemaColumnCount_;

};

////////////////////////////////////////////////////////////////////////////////

TMemoryCompactor::TMemoryCompactor(
    TTabletManagerConfigPtr config,
    TTablet* tablet)
    : Config_(std::move(config))
    , Tablet_(tablet)
    , Comparer_(Tablet_->KeyColumns().size())
{ }

TStaticMemoryStorePtr TMemoryCompactor::Run(
    TDynamicMemoryStorePtr dynamicStore,
    TStaticMemoryStorePtr staticStore)
{
    LOG_INFO("Memory compaction started (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
    
    TStaticMemoryStoreBuilder builder(Config_, Tablet_);

    TRowCombiner combiner(
        Config_,
        Tablet_,
        &builder);

    auto dynamicScanner = dynamicStore->CreateScanner();
    auto dynamicResult = dynamicScanner->BeginScan(EmptyKey(), LastCommittedTimestamp);

    auto staticScanner = staticStore->CreateScanner();
    auto staticResult = staticScanner->BeginScan(EmptyKey(), LastCommittedTimestamp);

    while (dynamicResult != NullTimestamp || staticResult != NullTimestamp) {
        int compareResult;
        if (dynamicResult != NullTimestamp && staticResult != NullTimestamp) {
            compareResult = Comparer_(staticScanner->GetKeys(), dynamicScanner->GetKeys());
        } else if (staticScanner != NullTimestamp) {
            compareResult = -1;
        } else {
            compareResult = +1;
        }

        combiner.BeginRow();

        if (dynamicResult != NullTimestamp) {
            combiner.Combine(dynamicScanner.get());
        }
        if (staticResult != NullTimestamp) {
            combiner.Combine(staticScanner.get());
        }

        if (compareResult <= 0) {
            staticResult = staticScanner->Advance();
        }
        if (compareResult >= 0) {
            dynamicResult = dynamicScanner->Advance();
        }

        combiner.EndRow();
    }

    LOG_INFO("Memory compaction completed (TabletId: %s)",
        ~ToString(Tablet_->GetId()));

    return builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
