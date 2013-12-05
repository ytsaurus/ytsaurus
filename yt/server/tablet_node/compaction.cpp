#include "stdafx.h"
#include "compaction.h"
#include "config.h"
#include "tablet.h"
#include "static_memory_store.h"
#include "dynamic_memory_store.h"
#include "private.h"

#include <ytlib/transaction_client/public.h>

#include <ytlib/tablet_client/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMemoryCompactor::TRowCombiner
{
public:
    TRowCombiner(
        TTablet* tablet,
        TStaticMemoryStoreBuilder* builder)
        : Tablet_(tablet)
        , Builder_(builder)
        , KeyCount_(static_cast<int>(Tablet_->KeyColumns().size()))
        , SchemaColumnCount_(static_cast<int>(Tablet_->Schema().Columns().size()))
        , KeysListed_(false)
    {
        Values_.resize(SchemaColumnCount_);
        for (int index = 0; index < SchemaColumnCount_; ++index) {
            Values_[index].reserve(Tablet_->GetConfig()->MaxVersions);
        }
    }

    void BeginRow()
    {
        Builder_->BeginRow();
    }

    void Combine(const IStoreScanner* scanner)
    {
        if (!KeysListed_) {
            const auto* srcKeys = scanner->GetKeys();
            auto* dstKeys = Builder_->AllocateKeys();
            memcpy(dstKeys, srcKeys, sizeof (TUnversionedValue) * KeyCount_);
            KeysListed_ = true;
        }

        for (int index = 0; index < SchemaColumnCount_; ++index) {
            auto& values = Values_[index];
            int maxVersions = Tablet_->GetConfig()->MaxVersions;
            int remainingVersions = maxVersions - static_cast<int>(values.size());
            scanner->GetFixedValues(index, remainingVersions, &values);
        }

        scanner->GetTimestamps(&Timestamps_);
    }

    void EndRow()
    {
        KeysListed_ = false;

        auto firstCommitTimestamp = MaxTimestamp;

        for (int index = 0; index < SchemaColumnCount_; ++index) {
            auto& srcValues = Values_[index];
            int count = static_cast<int>(srcValues.size());
            if (count > 0) {
                auto* dstValues = Builder_->AllocateFixedValues(index, count);
                memcpy(dstValues, srcValues.data(), sizeof (TVersionedValue) * count);
                firstCommitTimestamp = std::min(firstCommitTimestamp, srcValues.back().Timestamp);
                srcValues.clear();
            }
        }

        {
            auto it = Timestamps_.begin();
            auto jt = Timestamps_.begin();
            while (it != Timestamps_.end()) {
                if ((*it) & TimestampValueMask < firstCommitTimestamp)
                    break;
                if (it == Timestamps_.begin() || (*it & TombstoneTimestampMask) != (*(it - 1) & TombstoneTimestampMask)) {
                    *jt++ = *it;
                }
                ++it;
            }
            int count = std::distance(Timestamps_.begin(), jt);
            auto* dstTimestamps = Builder_->AllocateTimestamps(count);
            memcpy(dstTimestamps, Timestamps_.data(), sizeof (TTimestamp) * count);
            Timestamps_.clear();
        }

        Builder_->EndRow();
    }

private:
    TTablet* Tablet_;
    TStaticMemoryStoreBuilder* Builder_;

    int KeyCount_;
    int SchemaColumnCount_;


    bool KeysListed_;
    
    std::vector<std::vector<TVersionedValue>> Values_;
    std::vector<TTimestamp> Timestamps_;

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
