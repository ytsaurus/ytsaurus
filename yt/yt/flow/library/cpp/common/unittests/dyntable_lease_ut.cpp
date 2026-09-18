#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/dyntable_lease.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

////////////////////////////////////////////////////////////////////////////////

const TYPath FlowControlPath = "//pipeline/flow_control";
const TYPath LeasesPath = "//pipeline/leases";

constexpr TStringBuf LeaderLeaseKey = "leader_lease";
constexpr TStringBuf ExistenceSubkey = "existence";
constexpr TStringBuf ExpirationSubkey = "expiration";
//! The pipeline-wide deadline lives at the one key no partition id can spell.
constexpr TStringBuf DeadlineKey = "";

const auto Ttl = TDuration::Minutes(10);
const auto Now = TInstant::Seconds(1'700'000'000);

//! The two tables of the protocol are keyed alike here: the leases table by (key, subkey), the
//! flow_control table by (key, "").
using TRowKey = std::pair<std::string, std::string>;

struct TModification
{
    TYPath Path;
    bool Delete;
    TRowKey Key;
    TYsonString Value;
};

////////////////////////////////////////////////////////////////////////////////

//! Both tables of a pipeline as plain maps, driven through mock transactions: a lookup serves the
//! rows at their current value, a modification records itself and is applied, so what the protocol
//! writes is what the next call reads.
class TDyntableLeaseTest
    : public ::testing::Test
{
protected:
    TDyntableLeases Leases_{FlowControlPath, LeasesPath};

    std::map<TRowKey, TYsonString> ControlRows_;
    std::map<TRowKey, TYsonString> LeaseRows_;
    std::vector<TModification> Modifications_;

    TIncarnationId IncarnationId_ = TIncarnationId(TGuid::Create());
    TPartitionId PartitionId_ = TPartitionId(TGuid::Create());
    TJobId JobId_ = TJobId(TGuid::Create());
    std::string Address_ = "leader.yt:9012";

    ITransactionPtr MakeTransaction(TInstant startInstant = Now)
    {
        auto transaction = New<NiceMock<TMockTransaction>>();
        // Every instant the protocol reasons about comes from the transaction start timestamp,
        // never from a local clock, so this is the only clock a test needs to set.
        transaction->StartTimestamp = InstantToTimestamp(startInstant).first;

        ON_CALL(*transaction, LookupRows(_, _, _, _))
            .WillByDefault([this] (
                const TYPath& path,
                TNameTablePtr nameTable,
                const TSharedRange<TLegacyKey>& keys,
                const TLookupRowsOptions& /*options*/) {
                return MakeFuture(Lookup(path, nameTable, keys));
            });
        ON_CALL(*transaction, ModifyRows(_, _, _, _))
            .WillByDefault([this] (
                const TYPath& path,
                TNameTablePtr nameTable,
                TSharedRange<TRowModification>
                    modifications,
                const TModifyRowsOptions& /*options*/) {
                Apply(path, nameTable, modifications);
            });
        ON_CALL(*transaction, Commit(_))
            .WillByDefault(Return(MakeFuture(TTransactionCommitResult{})));
        ON_CALL(*transaction, Abort(_))
            .WillByDefault(Return(OKFuture));

        return transaction;
    }

    //! A client that hands out |transaction| and nothing else.
    IClientPtr MakeClient(const ITransactionPtr& transaction)
    {
        auto client = New<NiceMock<TMockClient>>();
        ON_CALL(*client, StartTransaction(_, _))
            .WillByDefault(Return(MakeFuture(transaction)));
        return client;
    }

    //! A client whose select over the leases table returns |keys| as its only column.
    IClientPtr MakeSelectingClient(const std::vector<std::string>& keys)
    {
        auto client = New<NiceMock<TMockClient>>();
        ON_CALL(*client, SelectRows(_, _))
            .WillByDefault([keys] (const std::string& /*query*/, const TSelectRowsOptions& /*options*/) {
                auto rowBuffer = New<TRowBuffer>();
                std::vector<TUnversionedRow> rows;
                for (const auto& key : keys) {
                    auto row = rowBuffer->AllocateUnversioned(1);
                    row[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(key, /*id*/ 0));
                    rows.push_back(row);
                }
                auto schema = New<TTableSchema>(std::vector{TColumnSchema("key", EValueType::String)});
                return MakeFuture(TSelectRowsResult{
                    .Rowset = CreateRowset(std::move(schema), MakeSharedRange(std::move(rows), std::move(rowBuffer))),
                });
            });
        return client;
    }

    // Table setup.

    void SetLeader(const TIncarnationId& incarnationId, TInstant expirationInstant)
    {
        TLeaderLeaseValue value;
        value.IncarnationId = incarnationId;
        value.Address = "someone.yt:9012";
        value.ExpirationInstant = expirationInstant;
        ControlRows_[{std::string(LeaderLeaseKey), ""}] = ConvertToYsonString(value);
    }

    void SetPartitionLease(const TPartitionId& partitionId, const TJobId& existenceJobId, const TJobId& expirationJobId)
    {
        SetLeaseRow(ToString(partitionId), ExistenceSubkey, existenceJobId);
        SetLeaseRow(ToString(partitionId), ExpirationSubkey, expirationJobId);
    }

    //! What phase 1 of a revocation leaves behind: the existence row alone.
    void SetLeaseRowsForRevokedPartition()
    {
        SetLeaseRow(ToString(PartitionId_), ExistenceSubkey, JobId_);
    }

    void SetDeadline(TInstant deadline)
    {
        TLeaseValue value;
        value.ExpirationInstant = deadline;
        LeaseRows_[{std::string(DeadlineKey), std::string(ExpirationSubkey)}] = ConvertToYsonString(value);
    }

    // Assertions.

    //! Every row modification the protocol asked for, in order, as "<write|delete> <table>:<key>/<subkey>".
    std::vector<std::string> ModificationLog() const
    {
        std::vector<std::string> log;
        for (const auto& modification : Modifications_) {
            log.push_back(Format("%v %v:%v/%v",
                modification.Delete ? "delete" : "write",
                modification.Path == LeasesPath ? "leases" : "flow_control",
                modification.Key.first,
                modification.Key.second));
        }
        return log;
    }

    TLeaderLeaseValue LeaderRow() const
    {
        return ConvertTo<TLeaderLeaseValue>(GetOrCrash(ControlRows_, TRowKey{std::string(LeaderLeaseKey), ""}));
    }

    TLeaseValue LeaseRow(TStringBuf key, TStringBuf subkey) const
    {
        return ConvertTo<TLeaseValue>(GetOrCrash(LeaseRows_, TRowKey{std::string(key), std::string(subkey)}));
    }

private:
    void SetLeaseRow(TStringBuf key, TStringBuf subkey, const TJobId& jobId)
    {
        TLeaseValue value;
        value.JobId = jobId;
        LeaseRows_[{std::string(key), std::string(subkey)}] = ConvertToYsonString(value);
    }

    std::map<TRowKey, TYsonString>& RowsAt(const TYPath& path)
    {
        return path == LeasesPath ? LeaseRows_ : ControlRows_;
    }

    static TRowKey DecodeKey(TUnversionedRow row, int keyId, std::optional<int> subkeyId)
    {
        TRowKey key;
        for (const auto& value : row) {
            if (value.Id == keyId) {
                key.first = value.AsStringBuf();
            } else if (subkeyId && value.Id == *subkeyId) {
                key.second = value.AsStringBuf();
            }
        }
        return key;
    }

    TUnversionedLookupRowsResult Lookup(
        const TYPath& path,
        const TNameTablePtr& nameTable,
        const TSharedRange<TLegacyKey>& keys)
    {
        auto keyId = nameTable->GetIdOrThrow("key");
        auto subkeyId = nameTable->FindId("subkey");
        const auto& rows = RowsAt(path);

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> resultRows;
        for (auto key : keys) {
            auto it = rows.find(DecodeKey(key, keyId, subkeyId));
            if (it == rows.end()) {
                // KeepMissingRows: a missing row keeps its place as a null one.
                resultRows.push_back(TUnversionedRow());
                continue;
            }
            auto row = rowBuffer->AllocateUnversioned(1);
            row[0] = rowBuffer->CaptureValue(MakeUnversionedAnyValue(it->second.AsStringBuf(), /*id*/ 0));
            resultRows.push_back(row);
        }

        // The protocol filters the lookup down to the value column, so that is the only column
        // the rowset carries.
        auto schema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::Any)});
        return TUnversionedLookupRowsResult{
            .Rowset = CreateRowset(std::move(schema), MakeSharedRange(std::move(resultRows), std::move(rowBuffer))),
        };
    }

    void Apply(
        const TYPath& path,
        const TNameTablePtr& nameTable,
        const TSharedRange<TRowModification>& modifications)
    {
        auto keyId = nameTable->GetIdOrThrow("key");
        auto subkeyId = nameTable->FindId("subkey");
        auto valueId = nameTable->GetIdOrThrow("value");
        auto& rows = RowsAt(path);

        for (const auto& modification : modifications) {
            if (const auto* write = std::get_if<NRowModifications::TWriteRow>(&modification)) {
                auto key = DecodeKey(write->Row, keyId, subkeyId);
                TYsonString value;
                for (const auto& column : write->Row) {
                    if (column.Id == valueId) {
                        value = TYsonString(TString(column.AsStringBuf()));
                    }
                }
                rows[key] = value;
                Modifications_.push_back({path, /*Delete*/ false, key, value});
            } else if (const auto* deletion = std::get_if<NRowModifications::TDeleteRow>(&modification)) {
                auto key = DecodeKey(deletion->Key, keyId, subkeyId);
                rows.erase(key);
                Modifications_.push_back({path, /*Delete*/ true, key, TYsonString()});
            } else {
                GTEST_FAIL() << "Unexpected row modification kind";
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
// The leader lease.

TEST_F(TDyntableLeaseTest, AnAbsentLeaderRowIsCaptured)
{
    auto transaction = MakeTransaction();

    auto result = Leases_.TryCaptureLeader(
        MakeClient(transaction),
        IncarnationId_,
        Address_,
        Ttl,
        /*captureAllowed*/ true,
        /*renewAllowed*/ false);

    EXPECT_TRUE(result.IsLeader);
    EXPECT_TRUE(result.Renewed);
    EXPECT_TRUE(result.Error.IsOK());
    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{"write flow_control:leader_lease/"}));
    EXPECT_EQ(LeaderRow().IncarnationId, IncarnationId_);
    EXPECT_EQ(LeaderRow().Address, Address_);
    // The expiration is the transaction start plus the ttl, not a local clock reading.
    EXPECT_EQ(LeaderRow().ExpirationInstant, Now + Ttl);
}

TEST_F(TDyntableLeaseTest, ALiveForeignLeaderIsLeftAlone)
{
    auto foreignId = TIncarnationId(TGuid::Create());
    SetLeader(foreignId, Now + TDuration::Minutes(1));
    auto transaction = MakeTransaction();

    auto result = Leases_.TryCaptureLeader(
        MakeClient(transaction),
        IncarnationId_,
        Address_,
        Ttl,
        /*captureAllowed*/ true,
        /*renewAllowed*/ false);

    EXPECT_FALSE(result.IsLeader);
    ASSERT_TRUE(result.CurrentLeader.has_value());
    EXPECT_EQ(result.CurrentLeader->IncarnationId, foreignId);
    // Not a single write: the row of a live leader is untouchable.
    EXPECT_TRUE(ModificationLog().empty());
}

TEST_F(TDyntableLeaseTest, AnExpiredForeignLeaderIsCaptured)
{
    SetLeader(TIncarnationId(TGuid::Create()), Now - TDuration::Seconds(1));
    auto transaction = MakeTransaction();

    auto result = Leases_.TryCaptureLeader(
        MakeClient(transaction),
        IncarnationId_,
        Address_,
        Ttl,
        /*captureAllowed*/ true,
        /*renewAllowed*/ false);

    EXPECT_TRUE(result.IsLeader);
    EXPECT_EQ(LeaderRow().IncarnationId, IncarnationId_);
}

TEST_F(TDyntableLeaseTest, AnObserverNeverCapturesAnExpiredRow)
{
    // This is how a leader whose own lease ran out demotes itself instead of silently recapturing:
    // the work cycle stopped feeding the lease, so the leadership has to be re-contested.
    SetLeader(IncarnationId_, Now - TDuration::Seconds(1));
    auto transaction = MakeTransaction();

    auto result = Leases_.TryCaptureLeader(
        MakeClient(transaction),
        IncarnationId_,
        Address_,
        Ttl,
        /*captureAllowed*/ false,
        /*renewAllowed*/ false);

    EXPECT_FALSE(result.IsLeader);
    EXPECT_TRUE(ModificationLog().empty());
}

TEST_F(TDyntableLeaseTest, OurOwnLiveLeaseIsReportedWithoutAWrite)
{
    // The fenced work transactions keep the row fresh; a capture loop that wrote it too would
    // conflict with them over and over, which is exactly the livelock this avoids.
    SetLeader(IncarnationId_, Now + Ttl);
    auto transaction = MakeTransaction();

    auto result = Leases_.TryCaptureLeader(
        MakeClient(transaction),
        IncarnationId_,
        Address_,
        Ttl,
        /*captureAllowed*/ true,
        /*renewAllowed*/ false);

    EXPECT_TRUE(result.IsLeader);
    EXPECT_FALSE(result.Renewed);
    EXPECT_TRUE(ModificationLog().empty());
}

TEST_F(TDyntableLeaseTest, RecoveryRenewsOurOwnAgingLease)
{
    // During recovery no fenced transaction is created for a long while, so the lease would expire
    // under a perfectly healthy leader; this is the one mode that renews it in the background.
    SetLeader(IncarnationId_, Now + Ttl / 4);
    auto transaction = MakeTransaction();

    auto result = Leases_.TryCaptureLeader(
        MakeClient(transaction),
        IncarnationId_,
        Address_,
        Ttl,
        /*captureAllowed*/ true,
        /*renewAllowed*/ true);

    EXPECT_TRUE(result.IsLeader);
    EXPECT_TRUE(result.Renewed);
    EXPECT_EQ(LeaderRow().ExpirationInstant, Now + Ttl);
}

TEST_F(TDyntableLeaseTest, AFreshEnoughLeaseIsNotRenewedEvenDuringRecovery)
{
    SetLeader(IncarnationId_, Now + Ttl);
    auto transaction = MakeTransaction();

    auto result = Leases_.TryCaptureLeader(
        MakeClient(transaction),
        IncarnationId_,
        Address_,
        Ttl,
        /*captureAllowed*/ true,
        /*renewAllowed*/ true);

    EXPECT_TRUE(result.IsLeader);
    EXPECT_FALSE(result.Renewed);
    EXPECT_TRUE(ModificationLog().empty());
}

TEST_F(TDyntableLeaseTest, TouchingTheLeaderRewritesItAndReportsWhatWasLeft)
{
    auto remainder = TDuration::Minutes(3);
    SetLeader(IncarnationId_, Now + remainder);
    auto transaction = MakeTransaction();

    auto reported = Leases_.ValidateAndTouchLeader(transaction, IncarnationId_, Address_, Ttl);

    // The remainder is what tells the caller to commit an urgent prolongation of its own.
    EXPECT_EQ(reported, remainder);
    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{"write flow_control:leader_lease/"}));
    EXPECT_EQ(LeaderRow().ExpirationInstant, Now + Ttl);
}

TEST_F(TDyntableLeaseTest, TouchingTheLeaderFencesOutANonLeader)
{
    SetLeader(TIncarnationId(TGuid::Create()), Now + Ttl);
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchLeader(transaction, IncarnationId_, Address_, Ttl),
        "not the leader");
    EXPECT_TRUE(ModificationLog().empty());
}

TEST_F(TDyntableLeaseTest, TouchingTheLeaderFencesOutAnExpiredLeader)
{
    SetLeader(IncarnationId_, Now - TDuration::Seconds(1));
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchLeader(transaction, IncarnationId_, Address_, Ttl),
        "lease has expired");
}

TEST_F(TDyntableLeaseTest, TouchingTheLeaderFencesOutAnAbsentRow)
{
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchLeader(transaction, IncarnationId_, Address_, Ttl),
        "not the leader");
}

////////////////////////////////////////////////////////////////////////////////
// Granting and revoking, controller side.

TEST_F(TDyntableLeaseTest, TheDeadlineIsOneRowForTheWholePipeline)
{
    auto transaction = MakeTransaction();

    Leases_.TouchLeaseDeadline(transaction, Ttl);

    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{"write leases:/expiration"}));
    EXPECT_EQ(LeaseRow(DeadlineKey, ExpirationSubkey).ExpirationInstant, Now + Ttl);
}

TEST_F(TDyntableLeaseTest, AFreshPartitionGetsBothRowsAtOnce)
{
    auto transaction = MakeTransaction();

    Leases_.GrantPartitionLease(transaction, PartitionId_, JobId_);

    auto key = ToString(PartitionId_);
    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{
            Format("write leases:%v/existence", key),
            Format("write leases:%v/expiration", key),
                                 }));
    EXPECT_EQ(LeaseRow(key, ExistenceSubkey).JobId, JobId_);
    EXPECT_EQ(LeaseRow(key, ExpirationSubkey).JobId, JobId_);
}

TEST_F(TDyntableLeaseTest, TheGrantPhasesWriteOneRowEachInTheirOwnOrder)
{
    // Phase 1 takes the expiration row, which workers never write, so it cannot conflict — and
    // once it lands the incumbent fails its own check and starts nothing new. Phase 2 then takes
    // the existence row, where a conflict is possible but only once.
    auto key = ToString(PartitionId_);
    auto incumbentJobId = TJobId(TGuid::Create());
    SetPartitionLease(PartitionId_, incumbentJobId, incumbentJobId);

    auto phase1 = MakeTransaction();
    Leases_.GrantPartitionLeasePhase1(phase1, PartitionId_, JobId_);

    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{Format("write leases:%v/expiration", key)}));
    EXPECT_EQ(LeaseRow(key, ExpirationSubkey).JobId, JobId_);
    // The incumbent still owns the existence row, and the two no longer agree — which is what
    // makes its next validation fail.
    EXPECT_EQ(LeaseRow(key, ExistenceSubkey).JobId, incumbentJobId);

    Modifications_.clear();
    auto phase2 = MakeTransaction();
    Leases_.GrantPartitionLeasePhase2(phase2, PartitionId_, JobId_);

    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{Format("write leases:%v/existence", key)}));
    EXPECT_EQ(LeaseRow(key, ExistenceSubkey).JobId, JobId_);
}

TEST_F(TDyntableLeaseTest, TheRevocationPhasesDeleteTheSameRowsInTheSameOrder)
{
    auto key = ToString(PartitionId_);
    SetPartitionLease(PartitionId_, JobId_, JobId_);

    auto phase1 = MakeTransaction();
    Leases_.RevokePartitionLeasePhase1(phase1, PartitionId_);

    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{Format("delete leases:%v/expiration", key)}));

    Modifications_.clear();
    auto phase2 = MakeTransaction();
    Leases_.RevokePartitionLeasePhase2(phase2, PartitionId_);

    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{Format("delete leases:%v/existence", key)}));
    EXPECT_TRUE(LeaseRows_.empty());
}

TEST_F(TDyntableLeaseTest, RevokingAPartitionThatHoldsNothingLeavesNoTrace)
{
    // Deleting rather than blanking the rows is what lets a controller revoke a partition it knows
    // nothing about: a write would leave a pair of dead rows behind instead.
    auto transaction = MakeTransaction();

    Leases_.RevokePartitionLeasePhase1(transaction, PartitionId_);
    Leases_.RevokePartitionLeasePhase2(transaction, PartitionId_);

    EXPECT_TRUE(LeaseRows_.empty());
}

////////////////////////////////////////////////////////////////////////////////
// Validation, worker side.

TEST_F(TDyntableLeaseTest, AHeldLeaseIsConfirmedAndTheExistenceRowIsTouched)
{
    auto key = ToString(PartitionId_);
    SetPartitionLease(PartitionId_, JobId_, JobId_);
    SetDeadline(Now + Ttl);
    auto transaction = MakeTransaction();

    auto deadline = Leases_.ValidateAndTouchPartitionLease(transaction, PartitionId_, JobId_);

    EXPECT_EQ(deadline, Now + Ttl);
    // The dummy write is the whole point: it takes the row lock, so a revocation committing
    // concurrently conflicts with this transaction instead of silently overtaking it.
    EXPECT_EQ(ModificationLog(), (std::vector<std::string>{Format("write leases:%v/existence", key)}));
    EXPECT_EQ(LeaseRow(key, ExistenceSubkey).JobId, JobId_);
}

TEST_F(TDyntableLeaseTest, ALeaseWithOneRowGoneFencesTheWorkerOut)
{
    // Exactly the state phase 1 of a revocation leaves behind.
    SetLeaseRowsForRevokedPartition();
    SetDeadline(Now + Ttl);
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchPartitionLease(transaction, PartitionId_, JobId_),
        "absent or revoked");
    EXPECT_TRUE(ModificationLog().empty());
}

TEST_F(TDyntableLeaseTest, AnAbsentLeaseFencesTheWorkerOut)
{
    SetDeadline(Now + Ttl);
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchPartitionLease(transaction, PartitionId_, JobId_),
        "absent or revoked");
}

TEST_F(TDyntableLeaseTest, AnExpirationRowNamingAnotherJobFencesTheIncumbentOut)
{
    // The state between the two phases of a grant: the partition is being handed over, and the
    // incumbent must stop before the new job's second phase lands.
    SetPartitionLease(PartitionId_, JobId_, TJobId(TGuid::Create()));
    SetDeadline(Now + Ttl);
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchPartitionLease(transaction, PartitionId_, JobId_),
        "belongs to another job");
    EXPECT_TRUE(ModificationLog().empty());
}

TEST_F(TDyntableLeaseTest, AnExistenceRowNamingAnotherJobFencesTheWorkerOut)
{
    SetPartitionLease(PartitionId_, TJobId(TGuid::Create()), JobId_);
    SetDeadline(Now + Ttl);
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchPartitionLease(transaction, PartitionId_, JobId_),
        "belongs to another job");
}

TEST_F(TDyntableLeaseTest, APassedDeadlineFencesEveryWorkerOut)
{
    SetPartitionLease(PartitionId_, JobId_, JobId_);
    SetDeadline(Now - TDuration::Seconds(1));
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchPartitionLease(transaction, PartitionId_, JobId_),
        "leases have expired");
}

TEST_F(TDyntableLeaseTest, AMissingDeadlineRowFencesEveryWorkerOut)
{
    // A pipeline whose controller has never written the deadline has granted nothing either; a
    // missing row must not read as "no deadline, commit away".
    SetPartitionLease(PartitionId_, JobId_, JobId_);
    auto transaction = MakeTransaction();

    EXPECT_THROW_WITH_SUBSTRING(
        Leases_.ValidateAndTouchPartitionLease(transaction, PartitionId_, JobId_),
        "leases have expired");
}

TEST_F(TDyntableLeaseTest, ARememberedDeadlineKeepsTheHotPathOffTheSharedRow)
{
    // The deadline row is shared by the whole pipeline, so reading it on every commit would funnel
    // every partition through one row of one tablet. The row is deliberately absent here: a call
    // that still passes proves it was not read at all.
    SetPartitionLease(PartitionId_, JobId_, JobId_);
    auto transaction = MakeTransaction();

    auto deadline = Leases_.ValidateAndTouchPartitionLease(
        transaction,
        PartitionId_,
        JobId_,
        /*knownDeadline*/ Now + TDuration::Minutes(1));

    EXPECT_EQ(deadline, Now + TDuration::Minutes(1));
}

TEST_F(TDyntableLeaseTest, ARememberedDeadlineThatRanOutIsReRead)
{
    SetPartitionLease(PartitionId_, JobId_, JobId_);
    SetDeadline(Now + Ttl);
    auto transaction = MakeTransaction();

    auto deadline = Leases_.ValidateAndTouchPartitionLease(
        transaction,
        PartitionId_,
        JobId_,
        /*knownDeadline*/ Now - TDuration::Seconds(1));

    // The remembered instant is only ever conservative: the controller moves the deadline forward.
    EXPECT_EQ(deadline, Now + Ttl);
}

////////////////////////////////////////////////////////////////////////////////
// The one read of the table, at the start of a leadership.

TEST_F(TDyntableLeaseTest, ListingReportsEveryPartitionOnceAndTheDeadlineNever)
{
    auto otherPartitionId = TPartitionId(TGuid::Create());
    auto client = MakeSelectingClient({
        std::string(DeadlineKey),
        ToString(PartitionId_),
        ToString(PartitionId_),
        ToString(otherPartitionId),
    });

    auto partitionIds = WaitFor(Leases_.ListPartitionLeases(client))
        .ValueOrThrow();

    std::sort(partitionIds.begin(), partitionIds.end());
    std::vector expected{PartitionId_, otherPartitionId};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(partitionIds, expected);
}

TEST_F(TDyntableLeaseTest, ListingStepsOverAKeyItCannotParse)
{
    // This read is the only way a controller can take leadership at all, so a key that belongs to
    // nobody must not throw out of it — that would leave the pipeline unleadable for as long as
    // the row is there.
    auto client = MakeSelectingClient({"not-a-partition-id", ToString(PartitionId_)});

    auto partitionIds = WaitFor(Leases_.ListPartitionLeases(client))
        .ValueOrThrow();

    EXPECT_EQ(partitionIds, std::vector{PartitionId_});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
