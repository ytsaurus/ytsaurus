#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Leases over plain dynamic table rows.
//!
//! The only transactional primitive used is the write-write conflict of overlapping tablet
//! transactions, so the protocol behaves identically over regular and chaos replicated tables.
//! Wall-clock instants are always derived from transaction start timestamps (the cluster clock),
//! never from local clocks of the participants.
//!
//! The leader lease lives in the pipeline's `flow_control` table (key = string, value = YSON):
//!   key = "leader_lease"           -> TLeaderLeaseValue
//!
//! Partition leases live in the pipeline's `leases` table (key = (key, subkey), value = YSON):
//!   (<partition_id>, "existence")  -> the job owning the lease
//!   (<partition_id>, "expiration") -> the job owning the lease
//!   ("",             "expiration") -> the deadline shared by every lease of the pipeline
//!
//! Both rows of a partition name its owner, and a worker is fenced by the pair: the two rows are
//! what makes revocation two-phase (see below), not two different facts.
//!
//! The deadline is pipeline-wide and lives in a single row. It states one thing — "the controller
//! is alive and in control" — which is one fact for the whole pipeline, not one per partition:
//! per-partition deadlines made the controller rewrite every lease row several times per ttl,
//! which on a large pipeline is tens of thousands of writes per minute carrying no information.
//!
//! Leadership and lease safety come from every fenced transaction re-reading its row (at the
//! transaction start timestamp) and writing it back: any concurrent capture/revoke also writes
//! the same row, so at most one of the conflicting transactions commits. The deadline only gates
//! LIVENESS decisions (how long a worker may keep committing once the controller stops writing
//! the table at all); it never substitutes for the transactional check.
//!
//! The leader lease is fed exclusively by the leader's fenced transactions — there is no
//! background renewal. This makes the lease a watchdog for the whole work cycle: a controller
//! that stops committing stops prolonging, and once the lease expires a replica captures
//! leadership. When a fenced transaction finds the remaining lease short, it is committed
//! immediately as a pure prolongation and the work is fenced by a fresh transaction instead.
//!
//! Partition lease revocation is two-phase: after phase 1 commits the worker can no longer START
//! a new transaction, after phase 2 it can no longer COMMIT a previously started one. Reassigning
//! a partition needs no phases at all — a grant writes both rows at once and conflicts with the
//! superseded worker on the "existence" row. Either way the rows always name the one job that may
//! be running the partition, which is what keeps two jobs of one partition from ever overlapping.

////////////////////////////////////////////////////////////////////////////////

struct TLeaderLeaseValue
    : public NYTree::TYsonStructLite
{
    TIncarnationId IncarnationId;
    std::string Address;
    TInstant ExpirationInstant;

    REGISTER_YSON_STRUCT_LITE(TLeaderLeaseValue);

    static void Register(TRegistrar registrar);
};

//! The value of every row of the `leases` table. A partition row carries the owning job and
//! leaves the instant zero; the pipeline-wide deadline row carries the instant and leaves the job
//! null. One struct for both keeps the table self-describing and the accessors trivial.
struct TLeaseValue
    : public NYTree::TYsonStructLite
{
    TJobId JobId;
    TInstant ExpirationInstant;

    REGISTER_YSON_STRUCT_LITE(TLeaseValue);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! The outcome of a single leader capture/renew attempt.
struct TLeaderAttemptResult
{
    //! Whether the caller holds the leadership after the attempt.
    bool IsLeader = false;
    //! Whether the attempt committed a write (a capture or a recovery-time renewal).
    bool Renewed = false;
    //! The current leader row (pre-write view); null if the row was absent.
    std::optional<TLeaderLeaseValue> CurrentLeader;
    //! Why the attempt did not end in leadership (lost race, unreachable table, ...); OK when
    //! IsLeader or when leadership is simply held by a live foreign leader.
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

//! Stateless protocol helper bound to the pipeline's lease tables.
//! |flowControlTablePath| is the `flow_control` table that holds the leader lease row;
//! |leasesTablePath| is the `leases` table that holds partition lease rows.
class TDyntableLeases
{
public:
    TDyntableLeases(NYPath::TYPath flowControlTablePath, NYPath::TYPath leasesTablePath);

    // Leader lease.

    //! One capture attempt in its own tablet transaction: when the leader row is absent or
    //! expired (anyone's, including our own after a stalled cycle), writes {|incarnationId|,
    //! |address|, now + ttl}. When the row is ours and unexpired, just reports leadership: the
    //! lease is prolonged by the fenced work transactions (see #ValidateAndTouchLeader), never
    //! from here — a leader whose work cycle stalls must stop feeding the lease and lose it.
    //! |captureAllowed| = false turns the attempt into a pure observation; the current leader
    //! uses it so that its own expired lease demotes it instead of being silently recaptured.
    //! |renewAllowed| = true restores the background renewal of our own live-but-aging lease —
    //! the recovery-time mode, when long read-only phases create no fenced transactions.
    //! A commit conflict simply means somebody else won the race — reported as not-a-leader,
    //! not as an error.
    TLeaderAttemptResult TryCaptureLeader(
        const NApi::IClientPtr& client,
        const TIncarnationId& incarnationId,
        const std::string& address,
        TDuration ttl,
        bool captureAllowed,
        bool renewAllowed) const;

    //! Fences a transaction with the leadership check: verifies (at the transaction start
    //! timestamp) that the leader row belongs to |incarnationId| and is not expired, and writes the row
    //! back with a refreshed expiration. Throws if the caller is not the leader; the write makes any
    //! concurrent capture conflict with this transaction's commit.
    //! Returns how much of the pre-refresh lease was left at the transaction start, so the caller
    //! can commit an urgent prolongation separately when the remainder runs low.
    TDuration ValidateAndTouchLeader(
        const NApi::ITransactionPtr& transaction,
        const TIncarnationId& incarnationId,
        const std::string& address,
        TDuration ttl) const;

    //! Reads the current leader row outside of any transaction (for announcing/monitoring).
    TFuture<std::optional<TLeaderLeaseValue>> ReadLeader(const NApi::IClientPtr& client) const;

    // Partition leases, controller side. All calls only add row modifications to |transaction|;
    // the caller owns fencing (ValidateAndTouchLeader) and the commit.

    //! Rewrites the pipeline-wide deadline row as |ttl| from the transaction start. Every lease
    //! of the pipeline dies at that instant unless the controller writes the row again, so this
    //! is what keeps the whole fleet alive; the controller is its only writer.
    void TouchLeaseDeadline(
        const NApi::ITransactionPtr& transaction,
        TDuration ttl) const;

    // Granting and revoking are the same two-phase shape, and the phases divide the same way in
    // both: phase 1 touches the "expiration" row, which workers never write, so it can conflict
    // with nobody — and once it commits, no worker passes its lease check, so none of them can
    // start another transaction. Phase 2 touches the "existence" row, which every worker commit
    // dummy-writes, so it can conflict — but at most once per partition, because phase 1 already
    // shut the door. That bound is what makes halving a conflicted chunk converge.
    //
    // A partition that holds no lease has nobody to shut out, so granting it needs no phases at
    // all — see #GrantPartitionLease.

    //! Writes both lease rows of a partition that holds no lease yet, naming |jobId| as the owner.
    //! Safe in one transaction precisely because there is no incumbent: nothing else writes those
    //! rows, so this cannot conflict. Do NOT use it to move a partition from one job to another —
    //! the incumbent worker keeps committing until this lands, so the conflict is unbounded and
    //! the grant can lose the race indefinitely. Use the two phases below for that.
    void GrantPartitionLease(
        const NApi::ITransactionPtr& transaction,
        const TPartitionId& partitionId,
        const TJobId& jobId) const;

    //! Phase 1 of moving a lease to |jobId|: writes the "expiration" row with the new owner. The
    //! incumbent then fails its lease check (the two rows no longer agree) and starts nothing new.
    void GrantPartitionLeasePhase1(
        const NApi::ITransactionPtr& transaction,
        const TPartitionId& partitionId,
        const TJobId& jobId) const;

    //! Phase 2 of moving a lease to |jobId|: writes the "existence" row with the new owner, which
    //! completes the handover and fences whatever transaction the incumbent still had in flight.
    void GrantPartitionLeasePhase2(
        const NApi::ITransactionPtr& transaction,
        const TPartitionId& partitionId,
        const TJobId& jobId) const;

    //! Phase 1 of the revocation: deletes the "expiration" row.
    //! Deleting rather than rewriting the row with no owner is what lets the controller revoke a
    //! partition it knows nothing about: a delete of an absent row is a no-op, so revoking a
    //! partition that never held a lease costs nothing and leaves no trace, while a write would
    //! create a pair of dead rows for it.
    void RevokePartitionLeasePhase1(
        const NApi::ITransactionPtr& transaction,
        const TPartitionId& partitionId) const;

    //! Phase 2 of the revocation: deletes the "existence" row.
    void RevokePartitionLeasePhase2(
        const NApi::ITransactionPtr& transaction,
        const TPartitionId& partitionId) const;

    //! Lists every partition that has a row in the table. Read exactly once, when a controller
    //! takes leadership, to find what its predecessor left behind; from then on the live layout
    //! says which partitions may hold a lease and the table is never scanned again.
    //! The pipeline-wide deadline row is not a partition and is skipped.
    TFuture<std::vector<TPartitionId>> ListPartitionLeases(
        const NApi::IClientPtr& client) const;

    // Partition leases, worker side.

    //! Fences a worker transaction with the lease check: verifies (at the transaction start
    //! timestamp) that both rows of the partition name this job and that the pipeline-wide
    //! deadline has not passed, then dummy-writes the "existence" row so that any concurrent
    //! revocation conflicts with this transaction's commit. Throws if the lease is absent,
    //! foreign or expired.
    //! |lookupTimeout| bounds the row read, so a slow lookup fails just this commit attempt.
    //!
    //! |knownDeadline| is a deadline this caller read earlier; while it has not run out the
    //! deadline row is not read at all, and the returned value is meant to be passed back on the
    //! next call. The deadline row is shared by the whole pipeline, so reading it on every commit
    //! would funnel every partition through one row of one tablet; remembering it is safe because
    //! the controller only ever moves the deadline forward, which makes a remembered instant
    //! conservative — it can only stop this worker earlier than strictly necessary. (Lowering the
    //! configured ttl of a running pipeline is the one thing that moves it back, and then a
    //! worker keeps the longer deadline until it runs out.)
    TInstant ValidateAndTouchPartitionLease(
        const NApi::ITransactionPtr& transaction,
        const TPartitionId& partitionId,
        const TJobId& jobId,
        TInstant knownDeadline = TInstant::Zero(),
        std::optional<TDuration> lookupTimeout = {}) const;

private:
    const NYPath::TYPath FlowControlPath_;
    const NYPath::TYPath Path_;

    TLeaderLeaseValue ValidateLeaderImpl(
        const NApi::ITransactionPtr& transaction,
        const TIncarnationId& incarnationId) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
