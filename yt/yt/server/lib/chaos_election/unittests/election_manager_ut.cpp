#include <yt/yt/server/lib/chaos_election/config.h>
#include <yt/yt/server/lib/chaos_election/election_manager.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChaosElection {
namespace {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;

using ::testing::_;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf LockTablePath = "//tmp/pipeline/leader_election_lock";
constexpr TStringBuf GroupName = "controller";
constexpr TStringBuf CellIdsPath = "//sys/chaos_cell_bundles/test-bundle/@tablet_cell_ids";

constexpr auto LeaseTimeout = TDuration::Seconds(30);

TChaosLeaseId MakeLeaseId()
{
    return TChaosLeaseId(0x1111, 0x2222, 0x3333, 0x4444);
}

//! The lock row of a leader that stopped pinging long ago, as a contender reads it.
TUnversionedLookupRowsResult MakeStaleLockRow()
{
    auto rowBuffer = New<TRowBuffer>();
    auto leaseId = ToString(MakeLeaseId());

    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue(GroupName, /*id*/ 0));
    builder.AddValue(MakeUnversionedStringValue(leaseId, /*id*/ 1));
    builder.AddValue(MakeUnversionedStringValue("dead-leader", /*id*/ 2));
    builder.AddValue(MakeUnversionedUint64Value(LeaseTimeout.MicroSeconds(), /*id*/ 3));
    builder.AddValue(MakeUnversionedUint64Value(
        (TInstant::Now() - 100 * LeaseTimeout).MicroSeconds(),
        /*id*/ 4));

    auto row = rowBuffer->CaptureRow(builder.GetRow());
    return {
        .Rowset = CreateRowset(
            GetChaosElectionLockTableSchema(),
            MakeSharedRange(std::vector<TUnversionedRow>{row}, std::move(rowBuffer))),
    };
}

class TChaosElectionManagerTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("ChaosElectionTest");
    const TIntrusivePtr<NiceMock<TMockClient>> Client_ = New<NiceMock<TMockClient>>();
    const TIntrusivePtr<NiceMock<TMockTransaction>> Transaction_ = New<NiceMock<TMockTransaction>>();

    NLockElection::ILockElectionManagerPtr CreateManager()
    {
        auto config = New<TChaosElectionManagerConfig>();
        config->LockTablePath = NYPath::TYPath(LockTablePath);
        config->ChaosCellBundle = "test-bundle";
        config->LeaseTimeout = LeaseTimeout;
        config->LeasePingPeriod = TDuration::Seconds(1);
        config->LockAcquisitionPeriod = TDuration::MilliSeconds(50);

        auto options = New<TChaosElectionManagerOptions>();
        options->GroupName = TString(GroupName);
        options->MemberName = "contender";

        return CreateChaosElectionManager(
            Client_,
            ActionQueue_->GetInvoker(),
            std::move(config),
            std::move(options));
    }

    //! Set once a takeover gets as far as asking the bundle for its cells.
    TPromise<void> TakeoverStarted_ = NewPromise<void>();

    //! A contender reads the stale row of a leader that is no longer pinging. The takeover that
    //! may follow is stopped at the cell fetch: what happens after it is not what these tests are
    //! about, and an unmocked call would hand the factory an empty future.
    void ExpectStaleLockRowIsRead()
    {
        EXPECT_CALL(*Client_, GetNode(NYPath::TYPath(CellIdsPath), _))
            .WillRepeatedly([this] (const NYPath::TYPath&, const TGetNodeOptions&) {
                TakeoverStarted_.TrySet();
                return MakeFuture<NYson::TYsonString>(TError("Bundle is unreachable"));
            });

        EXPECT_CALL(*Client_, StartTransaction(ETransactionType::Tablet, _))
            .WillRepeatedly([this] (ETransactionType, const TTransactionStartOptions&) {
                return MakeFuture<ITransactionPtr>(Transaction_);
            });
        EXPECT_CALL(*Transaction_, LookupRows(NYPath::TYPath(LockTablePath), _, _, _))
            .WillRepeatedly([] (
                const NYPath::TYPath&,
                TNameTablePtr,
                const TSharedRange<TLegacyKey>&,
                const TLookupRowsOptions&)
            {
                return MakeFuture(MakeStaleLockRow());
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

// Probing the recorded lease must not prolong it. Attaching pings by default, so a probe that
// keeps the default would refresh the dead leader's lease on every lock acquisition period: the
// lease would outlive its owner for as long as any contender keeps looking at it, and the takeover
// would never happen.

TEST_F(TChaosElectionManagerTest, ProbesTheRecordedLeaseWithoutPingingIt)
{
    ExpectStaleLockRowIsRead();

    auto probed = NewPromise<bool>();
    EXPECT_CALL(*Client_, AttachChaosLease(MakeLeaseId(), _))
        .WillRepeatedly([probed] (TChaosLeaseId, const TChaosLeaseAttachOptions& options) {
            probed.TrySet(options.Ping);
            return MakeFuture<IPrerequisitePtr>(TError(NYTree::EErrorCode::ResolveError, "No such object"));
        });

    auto manager = CreateManager();
    manager->Start();
    auto ping = WaitFor(probed.ToFuture().WithTimeout(TDuration::Seconds(10)))
        .ValueOrThrow();
    WaitFor(manager->Stop())
        .ThrowOnError();

    EXPECT_FALSE(ping);
}

// A probe that reports the lease gone is what unblocks the takeover. Only its start is observed
// here — the first thing a takeover does is ask the bundle for its cells; that it runs to a new
// leadership is covered end to end by tests/chaos_leases.
TEST_F(TChaosElectionManagerTest, StartsTakeoverOnceTheRecordedLeaseIsGone)
{
    ExpectStaleLockRowIsRead();

    EXPECT_CALL(*Client_, AttachChaosLease(MakeLeaseId(), _))
        .WillRepeatedly([] (TChaosLeaseId, const TChaosLeaseAttachOptions&) {
            return MakeFuture<IPrerequisitePtr>(TError(NYTree::EErrorCode::ResolveError, "No such object"));
        });

    auto manager = CreateManager();
    manager->Start();
    auto result = WaitFor(TakeoverStarted_.ToFuture().WithTimeout(TDuration::Seconds(10)));
    WaitFor(manager->Stop())
        .ThrowOnError();

    EXPECT_TRUE(result.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosElection
