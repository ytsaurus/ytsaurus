#include <yt/yt/server/lib/chaos_election/chaos_lease.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NChaosElection {
namespace {

using namespace NApi;
using namespace NChaosClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

using ::testing::_;
using ::testing::Invoke;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf CellIdsPath = "//sys/chaos_cell_bundles/test-bundle/@tablet_cell_ids";

TCellId MakeCellId(ui32 index)
{
    return TCellId(0xaaaa, 0xbbbb, 0xcccc, index);
}

TChaosLeaseId MakeLeaseId(ui32 index)
{
    return TChaosLeaseId(0x1111, 0x2222, 0x3333, index);
}

class TChaosLeaseFactoryTest
    : public ::testing::Test
{
protected:
    const TIntrusivePtr<StrictMock<TMockClient>> Client_ = New<StrictMock<TMockClient>>();
    const TChaosLeaseFactoryPtr Factory_ = New<TChaosLeaseFactory>(Client_, "test-bundle");
    //! The same factory with the cell cache disabled, so that every creation refetches the list.
    const TChaosLeaseFactoryPtr ExpiringFactory_ = New<TChaosLeaseFactory>(
        Client_,
        "test-bundle",
        /*cellIdsExpirationTime*/ TDuration::Zero());

    //! Cells the factory targeted, in creation-attempt order.
    std::vector<TCellId> TargetedCells_;

    void ExpectCellFetches(std::vector<std::vector<TCellId>> fetchResults)
    {
        auto& expectation = EXPECT_CALL(*Client_, GetNode(NYPath::TYPath(CellIdsPath), _))
            .Times(std::ssize(fetchResults));
        for (const auto& cells : fetchResults) {
            expectation.WillOnce(Invoke([cells] (const NYPath::TYPath&, const TGetNodeOptions&) {
                return MakeFuture(ConvertToYsonString(cells));
            }));
        }
    }

    //! Every creation on a cell from |disabledCells| is rejected the way a non-serving cell
    //! rejects it; any other cell yields a fresh lease id.
    void ExpectCreations(THashSet<TCellId> disabledCells = {})
    {
        EXPECT_CALL(*Client_, CreateObject(EObjectType::ChaosLease, _))
            .WillRepeatedly(Invoke(
                [this, disabledCells = std::move(disabledCells)] (
                    EObjectType,
                    const TCreateObjectOptions& options)
                {
                    auto cellId = options.Attributes->Get<TCellId>("chaos_cell_id");
                    TargetedCells_.push_back(cellId);
                    if (disabledCells.contains(cellId)) {
                        return MakeFuture<TObjectId>(TError(
                            NChaosClient::EErrorCode::ChaosCellIsNotEnabled,
                            "Cell is not enabled"));
                    }
                    return MakeFuture<TObjectId>(MakeLeaseId(TargetedCells_.size()));
                }));
    }
};

TEST_F(TChaosLeaseFactoryTest, SpreadsCreationsOverCellsRoundRobin)
{
    // A single fetch also pins the cell list cache: three creations, one lookup.
    ExpectCellFetches({{MakeCellId(0), MakeCellId(1), MakeCellId(2)}});
    ExpectCreations();

    for (int index = 0; index < 3; ++index) {
        bool created = Factory_->CreateLease(TDuration::Seconds(15)).BlockingGet().IsOK();
        EXPECT_TRUE(created);
    }

    EXPECT_EQ(
        TargetedCells_,
        (std::vector<TCellId>{MakeCellId(0), MakeCellId(1), MakeCellId(2)}));
}

TEST_F(TChaosLeaseFactoryTest, WalksPastNotEnabledCells)
{
    ExpectCellFetches({{MakeCellId(0), MakeCellId(1)}});
    ExpectCreations(/*disabledCells*/ {MakeCellId(0)});

    auto leaseId = Factory_->CreateLease(TDuration::Seconds(15)).BlockingGet().ValueOrThrow();

    EXPECT_EQ(leaseId, MakeLeaseId(2));
    EXPECT_EQ(TargetedCells_, (std::vector<TCellId>{MakeCellId(0), MakeCellId(1)}));
}

// A bundle holds sibling pairs, and of a pair only one sibling serves leases at a time, so about
// half the cells reject every creation. Learning that from the first walk keeps the steady state
// free of rejected requests instead of paying for one on every lease.
TEST_F(TChaosLeaseFactoryTest, StopsStartingTheWalkAtCellsThatRejected)
{
    ExpectCellFetches({{MakeCellId(0), MakeCellId(1), MakeCellId(2), MakeCellId(3)}});
    ExpectCreations(/*disabledCells*/ {MakeCellId(0), MakeCellId(2)});

    for (int index = 0; index < 3; ++index) {
        bool created = Factory_->CreateLease(TDuration::Seconds(15)).BlockingGet().IsOK();
        EXPECT_TRUE(created);
    }

    // Each rejecting cell is paid for once: creation 1 walks 0 (rejected) then 1, creation 2 walks
    // 2 (rejected) then 3, and creation 3 — with both rejections now known — goes straight to a
    // serving cell. From here on the rotation only alternates between 1 and 3.
    EXPECT_EQ(
        TargetedCells_,
        (std::vector<TCellId>{
            MakeCellId(0),
            MakeCellId(1),
            MakeCellId(2),
            MakeCellId(3),
            MakeCellId(1),
        }));
}

// Serving migrates between sibling cells, so a cell that once rejected has to stay reachable: it
// is demoted to the back of the rotation, never dropped.
TEST_F(TChaosLeaseFactoryTest, ComesBackToACellThatStartedServing)
{
    ExpectCellFetches({{MakeCellId(0), MakeCellId(1)}});
    THashSet<TCellId> disabledCells{MakeCellId(0)};
    EXPECT_CALL(*Client_, CreateObject(EObjectType::ChaosLease, _))
        .WillRepeatedly(Invoke([&] (EObjectType, const TCreateObjectOptions& options) {
            auto cellId = options.Attributes->Get<TCellId>("chaos_cell_id");
            TargetedCells_.push_back(cellId);
            if (disabledCells.contains(cellId)) {
                return MakeFuture<TObjectId>(TError(
                    NChaosClient::EErrorCode::ChaosCellIsNotEnabled,
                    "Cell is not enabled"));
            }
            return MakeFuture<TObjectId>(MakeLeaseId(TargetedCells_.size()));
        }));

    bool created = Factory_->CreateLease(TDuration::Seconds(15)).BlockingGet().IsOK();
    EXPECT_TRUE(created);
    TargetedCells_.clear();

    // The pair swaps roles, exactly as a lease migration between siblings leaves it.
    disabledCells = {MakeCellId(1)};
    created = Factory_->CreateLease(TDuration::Seconds(15)).BlockingGet().IsOK();
    EXPECT_TRUE(created);

    // Cell 1 is tried first because it served last time; it rejects now, and the walk falls
    // through to cell 0 instead of failing.
    EXPECT_EQ(TargetedCells_, (std::vector<TCellId>{MakeCellId(1), MakeCellId(0)}));
}

TEST_F(TChaosLeaseFactoryTest, RefreshesTheStaleCellListOnce)
{
    // The cached cell is gone from serving; the walk exhausts the list, refetches it and
    // succeeds on the replacement cell.
    ExpectCellFetches({{MakeCellId(0)}, {MakeCellId(1)}});
    ExpectCreations(/*disabledCells*/ {MakeCellId(0)});

    bool created = Factory_->CreateLease(TDuration::Seconds(15)).BlockingGet().IsOK();
    EXPECT_TRUE(created);

    EXPECT_EQ(TargetedCells_, (std::vector<TCellId>{MakeCellId(0), MakeCellId(1)}));
}

// Waiting for the whole cached list to reject a creation is not enough to notice a reconfigured
// bundle: while one cached cell keeps accepting, a newly added cell would never be targeted.
TEST_F(TChaosLeaseFactoryTest, PicksUpCellsAddedToTheBundleWhenTheCacheExpires)
{
    ExpectCellFetches({
        {MakeCellId(0)},
        {MakeCellId(0), MakeCellId(1)},
        {MakeCellId(0), MakeCellId(1)},
    });
    ExpectCreations();

    for (int index = 0; index < 3; ++index) {
        bool created = ExpiringFactory_->CreateLease(TDuration::Seconds(15)).BlockingGet().IsOK();
        EXPECT_TRUE(created);
    }

    // The first creation had a single cell to choose from, and a single-cell list does not advance
    // the rotation; the ones that follow see the enlarged list, and the cell added to the bundle
    // takes its turn — which it never would if the list were only refetched on a total rejection.
    EXPECT_EQ(
        TargetedCells_,
        (std::vector<TCellId>{MakeCellId(0), MakeCellId(0), MakeCellId(1)}));
}

// A refresh that cannot reach Cypress leaves the caller with the previous list rather than with an
// error: the cells it names are validated by the creation walk anyway.
TEST_F(TChaosLeaseFactoryTest, FallsBackToTheCachedCellsWhenTheRefreshFails)
{
    EXPECT_CALL(*Client_, GetNode(NYPath::TYPath(CellIdsPath), _))
        .WillOnce(Invoke([] (const NYPath::TYPath&, const TGetNodeOptions&) {
            return MakeFuture(ConvertToYsonString(std::vector<TCellId>{MakeCellId(0)}));
        }))
        .WillOnce(Invoke([] (const NYPath::TYPath&, const TGetNodeOptions&) {
            return MakeFuture<TYsonString>(TError("Cypress is unavailable"));
        }));
    ExpectCreations();

    for (int index = 0; index < 2; ++index) {
        bool created = ExpiringFactory_->CreateLease(TDuration::Seconds(15)).BlockingGet().IsOK();
        EXPECT_TRUE(created);
    }

    EXPECT_EQ(TargetedCells_, (std::vector<TCellId>{MakeCellId(0), MakeCellId(0)}));
}

TEST_F(TChaosLeaseFactoryTest, FailsWhenNoCellAcceptsAfterRefresh)
{
    ExpectCellFetches({{MakeCellId(0)}, {MakeCellId(0)}});
    ExpectCreations(/*disabledCells*/ {MakeCellId(0)});

    auto result = Factory_->CreateLease(TDuration::Seconds(15)).BlockingGet();

    EXPECT_FALSE(result.IsOK());
    EXPECT_THAT(result.GetMessage(), testing::HasSubstr("No enabled chaos cell"));
}

TEST_F(TChaosLeaseFactoryTest, PropagatesUnexpectedCreationErrors)
{
    // Anything but the not-enabled rejection must surface immediately instead of being retried
    // on the remaining cells: it can be a created-but-unacknowledged lease.
    ExpectCellFetches({{MakeCellId(0), MakeCellId(1)}});
    EXPECT_CALL(*Client_, CreateObject(EObjectType::ChaosLease, _))
        .WillOnce(Invoke([] (EObjectType, const TCreateObjectOptions&) {
            return MakeFuture<TObjectId>(TError("Creation lost"));
        }));

    auto result = Factory_->CreateLease(TDuration::Seconds(15)).BlockingGet();

    EXPECT_FALSE(result.IsOK());
    EXPECT_THAT(result.GetMessage(), testing::HasSubstr("Creation lost"));
}

TEST_F(TChaosLeaseFactoryTest, ForwardsTimeoutAndCallerAttributes)
{
    ExpectCellFetches({{MakeCellId(0)}});

    IAttributeDictionaryPtr seenAttributes;
    EXPECT_CALL(*Client_, CreateObject(EObjectType::ChaosLease, _))
        .WillOnce(Invoke([&] (EObjectType, const TCreateObjectOptions& options) {
            seenAttributes = options.Attributes->Clone();
            return MakeFuture<TObjectId>(MakeLeaseId(0));
        }));

    auto callerAttributes = CreateEphemeralAttributes();
    callerAttributes->Set("marker", 42);

    auto lease = Factory_->CreateLease(TDuration::Seconds(21), callerAttributes).BlockingGet();
    bool leaseOk = lease.IsOK();
    EXPECT_TRUE(leaseOk);

    EXPECT_EQ(seenAttributes->Get<TDuration>("timeout"), TDuration::Seconds(21));
    EXPECT_EQ(seenAttributes->Get<int>("marker"), 42);
    EXPECT_EQ(seenAttributes->Get<TCellId>("chaos_cell_id"), MakeCellId(0));
    // The caller's dictionary is cloned, not adopted.
    EXPECT_FALSE(callerAttributes->Contains("chaos_cell_id"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosElection
