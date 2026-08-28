#include "dyntable_lease.h"

#include "private.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/tablet_client/public.h>
#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = DyntableLeaseLogger;

////////////////////////////////////////////////////////////////////////////////

void TLeaderLeaseValue::Register(TRegistrar registrar)
{
    registrar.Parameter("incarnation_id", &TThis::IncarnationId)
        .Default();
    registrar.Parameter("address", &TThis::Address)
        .Default();
    registrar.Parameter("expiration_instant", &TThis::ExpirationInstant)
        .Default();
}

void TLeaseValue::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId)
        .Default();
    registrar.Parameter("expiration_instant", &TThis::ExpirationInstant)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr TStringBuf LeaderLeaseKey = "leader_lease";
//! The key of the pipeline-wide deadline row; no partition id can collide with it.
constexpr TStringBuf DeadlineKey = "";
constexpr TStringBuf ExistenceSubkey = "existence";
constexpr TStringBuf ExpirationSubkey = "expiration";

//! Shared plumbing of the row accessors below: accumulated lookup keys and row modifications,
//! the batched lookup of the `value` column and the flush. The accessors differ only in the key
//! columns of their tables.
class TRowAccessorBase
{
public:
    TNameTablePtr NameTable = New<TNameTable>();
    int KeyField = NameTable->GetIdOrRegisterName("key");
    int ValueField = NameTable->GetIdOrRegisterName("value");
    TRowBufferPtr RowBuffer = New<TRowBuffer>();

    std::vector<TLegacyKey> Keys;
    std::vector<TRowModification> Modifications;

    //! Reads the previously AddKey-ed rows via |clientBase| (a client or a transaction; the
    //! latter reads at the transaction start timestamp) and returns per-key YSON values,
    //! std::nullopt for missing rows.
    std::vector<std::optional<TYsonString>> Lookup(
        const IClientBasePtr& clientBase,
        const TYPath& path,
        std::optional<TDuration> timeout = {})
    {
        TLookupRowsOptions options;
        options.ColumnFilter = TColumnFilter({ValueField});
        options.KeepMissingRows = true;
        options.Timeout = timeout;

        auto range = MakeSharedRange(Keys, RowBuffer);
        auto result = WaitFor(clientBase->LookupRows(path, NameTable, range, options))
            .ValueOrThrow();

        std::vector<std::optional<TYsonString>> values;
        for (const auto& row : result.Rowset->GetRows()) {
            if (!row || row.GetCount() == 0 || row[0].Type == EValueType::Null) {
                values.push_back(std::nullopt);
            } else {
                values.push_back(TYsonString(TString(row[0].AsStringBuf())));
            }
        }
        YT_VERIFY(values.size() == Keys.size());
        return values;
    }

    void Flush(const ITransactionPtr& transaction, const TYPath& path)
    {
        YT_VERIFY(!Modifications.empty());
        transaction->ModifyRows(path, NameTable, MakeSharedRange(std::move(Modifications), RowBuffer));
        Modifications = {};
    }
};

//! Row accessor for the `leases` table: schema (hash, key, subkey, value).
class TLeaseRowAccessor
    : public TRowAccessorBase
{
public:
    int SubkeyField = NameTable->GetIdOrRegisterName("subkey");

    void AddKey(TStringBuf key, TStringBuf subkey)
    {
        auto row = RowBuffer->AllocateUnversioned(2);
        row[0] = RowBuffer->CaptureValue(MakeUnversionedStringValue(key, KeyField));
        row[1] = RowBuffer->CaptureValue(MakeUnversionedStringValue(subkey, SubkeyField));
        Keys.push_back(row);
    }

    void AddWrite(TStringBuf key, TStringBuf subkey, const TYsonString& value)
    {
        auto row = RowBuffer->AllocateUnversioned(3);
        row[0] = RowBuffer->CaptureValue(MakeUnversionedStringValue(key, KeyField));
        row[1] = RowBuffer->CaptureValue(MakeUnversionedStringValue(subkey, SubkeyField));
        row[2] = RowBuffer->CaptureValue(MakeUnversionedAnyValue(value.AsStringBuf(), ValueField));
        Modifications.push_back(NRowModifications::TWriteRow(row));
    }

    void AddDelete(TStringBuf key, TStringBuf subkey)
    {
        auto row = RowBuffer->AllocateUnversioned(2);
        row[0] = RowBuffer->CaptureValue(MakeUnversionedStringValue(key, KeyField));
        row[1] = RowBuffer->CaptureValue(MakeUnversionedStringValue(subkey, SubkeyField));
        Modifications.push_back(NRowModifications::TDeleteRow(row));
    }
};

//! Row accessor for the `flow_control` table: schema (key, value) — no subkey.
class TControlRowAccessor
    : public TRowAccessorBase
{
public:
    void AddKey(TStringBuf key)
    {
        auto row = RowBuffer->AllocateUnversioned(1);
        row[0] = RowBuffer->CaptureValue(MakeUnversionedStringValue(key, KeyField));
        Keys.push_back(row);
    }

    void AddWrite(TStringBuf key, const TYsonString& value)
    {
        auto row = RowBuffer->AllocateUnversioned(2);
        row[0] = RowBuffer->CaptureValue(MakeUnversionedStringValue(key, KeyField));
        row[1] = RowBuffer->CaptureValue(MakeUnversionedAnyValue(value.AsStringBuf(), ValueField));
        Modifications.push_back(NRowModifications::TWriteRow(row));
    }
};

TInstant TransactionStartInstant(const ITransactionPtr& transaction)
{
    return TimestampToInstant(transaction->GetStartTimestamp()).first;
}

template <class T>
std::optional<T> ParseValue(const std::optional<TYsonString>& value)
{
    if (!value) {
        return std::nullopt;
    }
    return ConvertTo<T>(*value);
}

TYsonString MakeOwnerValue(const TJobId& jobId)
{
    TLeaseValue value;
    value.JobId = jobId;
    return ConvertToYsonString(value);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TDyntableLeases::TDyntableLeases(TYPath flowControlTablePath, TYPath leasesTablePath)
    : FlowControlPath_(std::move(flowControlTablePath))
    , Path_(std::move(leasesTablePath))
{ }

TLeaderAttemptResult TDyntableLeases::TryCaptureLeader(
    const IClientPtr& client,
    const TIncarnationId& incarnationId,
    const std::string& address,
    TDuration ttl,
    bool captureAllowed,
    bool renewAllowed) const
{
    ITransactionPtr transaction;
    std::optional<TLeaderLeaseValue> leader;
    try {
        TTransactionStartOptions options;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", "Flow: leader lease capture");
        options.Attributes = std::move(attributes);
        transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, options))
            .ValueOrThrow();

        TControlRowAccessor accessor;
        accessor.AddKey(LeaderLeaseKey);
        leader = ParseValue<TLeaderLeaseValue>(accessor.Lookup(transaction, FlowControlPath_)[0]);

        auto now = TransactionStartInstant(transaction);
        bool mine = leader && leader->IncarnationId == incarnationId;
        if (mine && leader->ExpirationInstant >= now &&
            (!renewAllowed || leader->ExpirationInstant >= now + ttl / 2))
        {
            // Ours and alive; the fenced work transactions keep it fresh, nothing to write.
            // In the recovery mode (|renewAllowed|) an aging lease falls through to the write.
            YT_UNUSED_FUTURE(transaction->Abort());
            return {.IsLeader = true, .CurrentLeader = std::move(leader)};
        }
        if (leader && !mine && leader->ExpirationInstant >= now) {
            // A live foreign leader; do not touch the row.
            YT_UNUSED_FUTURE(transaction->Abort());
            return {.IsLeader = false, .CurrentLeader = std::move(leader)};
        }
        if (!(mine && leader->ExpirationInstant >= now) && !captureAllowed) {
            // The row is absent or expired — capturable by anyone, including its previous
            // owner, but this caller only observes.
            YT_UNUSED_FUTURE(transaction->Abort());
            return {.IsLeader = false, .CurrentLeader = std::move(leader)};
        }

        TLeaderLeaseValue value;
        value.IncarnationId = incarnationId;
        value.Address = address;
        value.ExpirationInstant = now + ttl;
        accessor.AddWrite(LeaderLeaseKey, ConvertToYsonString(value));
        accessor.Flush(transaction, FlowControlPath_);
    } catch (const std::exception& ex) {
        if (transaction) {
            YT_UNUSED_FUTURE(transaction->Abort());
        }
        return {.IsLeader = false, .CurrentLeader = std::move(leader), .Error = TError(ex)};
    }

    // A commit failure means we lost a race (or the table is unavailable) — either way we do not
    // hold the leadership; the error is reported for diagnostics only.
    auto error = WaitFor(transaction->Commit());
    if (!error.IsOK()) {
        return {.IsLeader = false, .CurrentLeader = std::move(leader), .Error = TError(error)};
    }
    return {.IsLeader = true, .Renewed = true, .CurrentLeader = std::move(leader)};
}

TDuration TDyntableLeases::ValidateAndTouchLeader(
    const ITransactionPtr& transaction,
    const TIncarnationId& incarnationId,
    const std::string& address,
    TDuration ttl) const
{
    auto leader = ValidateLeaderImpl(transaction, incarnationId);
    auto remaining = leader.ExpirationInstant - TransactionStartInstant(transaction);

    TLeaderLeaseValue value;
    value.IncarnationId = incarnationId;
    value.Address = address;
    value.ExpirationInstant = TransactionStartInstant(transaction) + ttl;
    TControlRowAccessor accessor;
    accessor.AddWrite(LeaderLeaseKey, ConvertToYsonString(value));
    accessor.Flush(transaction, FlowControlPath_);

    return remaining;
}

TLeaderLeaseValue TDyntableLeases::ValidateLeaderImpl(
    const ITransactionPtr& transaction,
    const TIncarnationId& incarnationId) const
{
    TControlRowAccessor accessor;
    accessor.AddKey(LeaderLeaseKey);
    auto leader = ParseValue<TLeaderLeaseValue>(accessor.Lookup(transaction, FlowControlPath_)[0]);

    auto now = TransactionStartInstant(transaction);
    if (!leader || leader->IncarnationId != incarnationId) {
        THROW_ERROR_EXCEPTION("Controller is not the leader")
            .With("self_incarnation_id", incarnationId)
            .With("leader_incarnation_id", leader ? leader->IncarnationId : TIncarnationId())
            .With("leader_address", leader ? leader->Address : std::string());
    }
    if (leader->ExpirationInstant < now) {
        THROW_ERROR_EXCEPTION("Controller leader lease has expired")
            .With("expiration_instant", leader->ExpirationInstant)
            .With("transaction_start_instant", now);
    }
    return *leader;
}

TFuture<std::optional<TLeaderLeaseValue>> TDyntableLeases::ReadLeader(const IClientPtr& client) const
{
    return BIND([client, path = FlowControlPath_] {
        TControlRowAccessor accessor;
        accessor.AddKey(LeaderLeaseKey);
        return ParseValue<TLeaderLeaseValue>(accessor.Lookup(client, path)[0]);
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TDyntableLeases::TouchLeaseDeadline(
    const ITransactionPtr& transaction,
    TDuration ttl) const
{
    TLeaseValue value;
    value.ExpirationInstant = TransactionStartInstant(transaction) + ttl;

    TLeaseRowAccessor accessor;
    accessor.AddWrite(DeadlineKey, ExpirationSubkey, ConvertToYsonString(value));
    accessor.Flush(transaction, Path_);
}

void TDyntableLeases::GrantPartitionLease(
    const ITransactionPtr& transaction,
    const TPartitionId& partitionId,
    const TJobId& jobId) const
{
    auto value = MakeOwnerValue(jobId);

    TLeaseRowAccessor accessor;
    auto key = ToString(partitionId);
    accessor.AddWrite(key, ExistenceSubkey, value);
    accessor.AddWrite(key, ExpirationSubkey, value);
    accessor.Flush(transaction, Path_);
}

void TDyntableLeases::GrantPartitionLeasePhase1(
    const ITransactionPtr& transaction,
    const TPartitionId& partitionId,
    const TJobId& jobId) const
{
    TLeaseRowAccessor accessor;
    accessor.AddWrite(ToString(partitionId), ExpirationSubkey, MakeOwnerValue(jobId));
    accessor.Flush(transaction, Path_);
}

void TDyntableLeases::GrantPartitionLeasePhase2(
    const ITransactionPtr& transaction,
    const TPartitionId& partitionId,
    const TJobId& jobId) const
{
    TLeaseRowAccessor accessor;
    accessor.AddWrite(ToString(partitionId), ExistenceSubkey, MakeOwnerValue(jobId));
    accessor.Flush(transaction, Path_);
}

void TDyntableLeases::RevokePartitionLeasePhase1(
    const ITransactionPtr& transaction,
    const TPartitionId& partitionId) const
{
    TLeaseRowAccessor accessor;
    accessor.AddDelete(ToString(partitionId), ExpirationSubkey);
    accessor.Flush(transaction, Path_);
}

void TDyntableLeases::RevokePartitionLeasePhase2(
    const ITransactionPtr& transaction,
    const TPartitionId& partitionId) const
{
    TLeaseRowAccessor accessor;
    accessor.AddDelete(ToString(partitionId), ExistenceSubkey);
    accessor.Flush(transaction, Path_);
}

TFuture<std::vector<TPartitionId>> TDyntableLeases::ListPartitionLeases(
    const IClientPtr& client) const
{
    TSelectRowsOptions options;
    // Read at the sync-last-committed timestamp: it is always at or ahead of the replica's
    // retained timestamp, so a fresh flush cannot leave the scan below the retention horizon
    // (which the async-last-committed replication-progress timestamp can, failing with code
    // 313). This only nominates cleanup candidates; every lease mutation revalidates inside
    // its own transaction, so waiting on the odd in-flight worker commit is acceptable here.
    options.Timestamp = SyncLastCommittedTimestamp;
    return client->SelectRows(Format("key FROM [%v]", Path_), options)
        .Apply(BIND([] (const TSelectRowsResult& result) {
            THashSet<TPartitionId> partitionIds;
            std::vector<TString> unparsedKeys;
            auto schema = result.Rowset->GetSchema();
            auto keyIndex = schema->GetColumnIndexOrThrow("key");
            for (const auto& row : result.Rowset->GetRows()) {
                auto key = row[keyIndex].AsStringBuf();
                if (key == DeadlineKey) {
                    continue;
                }
                // A key that is neither the deadline nor a partition id belongs to nobody this
                // protocol knows about. Skipping it keeps one stray row from throwing out of the
                // only read that lets a controller take leadership at all, which would leave the
                // pipeline unleadable for as long as the row is there.
                TGuid partitionId;
                if (!TGuid::FromString(key, &partitionId)) {
                    unparsedKeys.push_back(TString(key));
                    continue;
                }
                partitionIds.insert(TPartitionId(partitionId));
            }
            if (!unparsedKeys.empty()) {
                YT_LOG_WARNING("Ignoring unrecognized keys of the leases table (Count: %v, Sample: %v)",
                    std::ssize(unparsedKeys),
                    unparsedKeys.front());
            }
            return std::vector<TPartitionId>(partitionIds.begin(), partitionIds.end());
        }));
}

TInstant TDyntableLeases::ValidateAndTouchPartitionLease(
    const ITransactionPtr& transaction,
    const TPartitionId& partitionId,
    const TJobId& jobId,
    TInstant knownDeadline,
    std::optional<TDuration> lookupTimeout) const
{
    auto key = ToString(partitionId);
    auto now = TransactionStartInstant(transaction);
    // See the header: the shared deadline row is read only once the remembered deadline has run
    // out, which keeps this hot path off it for a whole ttl per partition.
    bool readDeadline = knownDeadline <= now;

    TLeaseRowAccessor accessor;
    accessor.AddKey(key, ExistenceSubkey);
    accessor.AddKey(key, ExpirationSubkey);
    if (readDeadline) {
        accessor.AddKey(DeadlineKey, ExpirationSubkey);
    }
    auto values = accessor.Lookup(transaction, Path_, lookupTimeout);
    auto existence = ParseValue<TLeaseValue>(values[0]);
    auto expiration = ParseValue<TLeaseValue>(values[1]);
    auto deadline = knownDeadline;
    if (readDeadline) {
        auto deadlineValue = ParseValue<TLeaseValue>(values[2]);
        deadline = deadlineValue ? deadlineValue->ExpirationInstant : TInstant::Zero();
    }

    if (!existence || !expiration) {
        THROW_ERROR_EXCEPTION("Partition lease is absent or revoked")
            .With("partition_id", partitionId)
            .With("has_existence", existence.has_value())
            .With("has_expiration", expiration.has_value());
    }
    // Both rows must name this job: a revocation in progress has already rewritten one of them,
    // and a reassignment has rewritten both.
    if (existence->JobId != jobId || expiration->JobId != jobId) {
        THROW_ERROR_EXCEPTION("Partition lease belongs to another job")
            .With("partition_id", partitionId)
            .With("lease_job_id", existence->JobId)
            .With("revoked_job_id", expiration->JobId)
            .With("job_id", jobId);
    }
    if (deadline < now) {
        THROW_ERROR_EXCEPTION("Partition leases have expired")
            .With("partition_id", partitionId)
            .With("expiration_instant", deadline)
            .With("transaction_start_instant", now);
    }

    // The dummy write guarantees that a concurrent revocation (phase 2 deletes this row)
    // conflicts with this transaction's commit. Dyntable writes are blind: the tablet never
    // compares the written value with the current one, so even a byte-identical write takes the
    // row write lock and conflicts with a concurrent delete.
    accessor.AddWrite(key, ExistenceSubkey, ConvertToYsonString(*existence));
    accessor.Flush(transaction, Path_);

    return deadline;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
