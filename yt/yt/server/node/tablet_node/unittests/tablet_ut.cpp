#include "ordered_dynamic_store_ut_helpers.h"

#include <yt/yt/server/node/tablet_node/tablet.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTabletNode {
namespace {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TOrderedTabletTest
    : public TOrderedDynamicStoreTestBase
{
protected:
    TOrderedDynamicStorePtr Store_;

    ECommitOrdering GetCommitOrdering() const override
    {
        return ECommitOrdering::Strong;
    }

    void CreateDynamicStore() override
    {
        Store_ = New<TOrderedDynamicStore>(
            Tablet_->GenerateId(EObjectType::OrderedDynamicTabletStore),
            Tablet_.get(),
            StoreContext_);
    }

    TTimestamp WriteRow(const TUnversionedOwningRow& row)
    {
        TWriteContext context;
        context.Phase = EWritePhase::Commit;
        context.CommitTimestamp = GenerateTimestamp();
        EXPECT_NE(TOrderedDynamicRow(), Store_->WriteRow(row, &context));
        return context.CommitTimestamp;
    }

private:
    const IStoreContextPtr StoreContext_ = New<TStoreContextMock>();


    IDynamicStorePtr GetDynamicStore() override
    {
        return Store_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedTabletTest, TestValidateTrimmedRowCountPrecedeTimestamp)
{
    auto doValidate = [this] (i64 trimmedRowCount, TTimestamp timestamp) {
        ValidateTrimmedRowCountPrecedesTimestamp(Tablet_.get(), trimmedRowCount, timestamp);
    };

    ASSERT_NO_THROW(doValidate(0, MinTimestamp));

    CreateDynamicStore();
    Tablet_->AddStore(Store_, false);

    ASSERT_EQ(Store_->GetMinTimestamp(), MaxTimestamp);
    //Overtrim of empty store.
    ASSERT_ANY_THROW(doValidate(1, MinTimestamp));

    auto row = Format("a=%v", 0);
    auto ts00 = WriteRow(BuildRow(row));
    auto ts01 = WriteRow(BuildRow(row));
    auto ts02 = WriteRow(BuildRow(row));

    auto startingRowIndex = Store_->GetStartingRowIndex() + Store_->GetRowCount();
    CreateDynamicStore();
    Store_->SetStartingRowIndex(startingRowIndex);
    Tablet_->AddStore(Store_, false);

    ASSERT_EQ(Store_->GetMinTimestamp(), MaxTimestamp);
    ASSERT_EQ(Store_->GetStartingRowIndex(), 3);
    ASSERT_ANY_THROW(doValidate(3, ts01));
    ASSERT_NO_THROW(doValidate(3, ts02));
    ASSERT_ANY_THROW(doValidate(4, ts02)); //Overtrim of empty store.

    auto ts10 = WriteRow(BuildRow(row));
    auto ts11 = WriteRow(BuildRow(row));
    auto ts12 = WriteRow(BuildRow(row));

    ASSERT_NO_THROW(doValidate(0, ts00));
    ASSERT_NO_THROW(doValidate(0, ts01));
    ASSERT_NO_THROW(doValidate(0, ts02));

    ASSERT_ANY_THROW(doValidate(0, ts00 - 1));
    ASSERT_ANY_THROW(doValidate(1, ts00));
    ASSERT_ANY_THROW(doValidate(2, ts01));
    ASSERT_NO_THROW(doValidate(2, ts02));

    ASSERT_NO_THROW(doValidate(2, ts10));
    ASSERT_NO_THROW(doValidate(2, ts11));
    ASSERT_NO_THROW(doValidate(2, ts12));

    ASSERT_NO_THROW(doValidate(3, ts10));
    ASSERT_NO_THROW(doValidate(3, ts11));
    ASSERT_NO_THROW(doValidate(3, ts12));
    ASSERT_NO_THROW(doValidate(4, ts12));
    ASSERT_NO_THROW(doValidate(5, ts12));
    ASSERT_NO_THROW(doValidate(6, ts12));

    ASSERT_ANY_THROW(doValidate(3, ts10 - 1));
    ASSERT_ANY_THROW(doValidate(4, ts10));
    ASSERT_ANY_THROW(doValidate(5, ts11));
    ASSERT_ANY_THROW(doValidate(7, ts12)); // Overtrim.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode
