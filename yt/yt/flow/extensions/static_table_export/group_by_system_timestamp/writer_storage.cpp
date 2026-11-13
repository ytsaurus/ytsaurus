#include "writer_storage.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/table_writer.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <numeric>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr i64 MaxWriteFrameDataWeight = 64_MB;
//! With the idle close of a whole stream it stays below the one-minute limit of the proxy for an upload without data.
constexpr auto MaxSurplusWriterIdleTime = TDuration::Seconds(15);
constexpr auto ControlTransactionTimeout = TDuration::Minutes(1);

std::string MakePartitionKey(int partition)
{
    return Format("p_%v", partition);
}

//! Releases the locks of a failed control transaction before its timeout.
auto AbortOnExit(const ITransactionPtr& transaction)
{
    return Finally([transaction] {
        YT_UNUSED_FUTURE(transaction->Abort());
    });
}

bool IsGateUnavailable(const TError& error)
{
    return error.FindMatching(NYTree::EErrorCode::ResolveError) ||
        error.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TStaticTableExportPart::Register(TRegistrar registrar)
{
    registrar.Parameter("first_offset", &TThis::FirstOffset);
    registrar.Parameter("next_offset", &TThis::NextOffset);
}

void TStaticTableExportCursor::Register(TRegistrar registrar)
{
    registrar.Parameter("generation", &TThis::Generation)
        .Default();
    registrar.Parameter("next_offset", &TThis::NextOffset)
        .Default(0);
    registrar.Parameter("highest_window", &TThis::HighestWindow)
        .Default();
    registrar.Parameter("pending_ranges", &TThis::PendingRanges)
        .Default();
}

void TStaticTableExportProgress::Register(TRegistrar registrar)
{
    registrar.Parameter("format_version", &TThis::FormatVersion);
    registrar.Parameter("owner", &TThis::Owner);
    registrar.Parameter("source_identity", &TThis::SourceIdentity);
    registrar.Parameter("partition_count", &TThis::PartitionCount);
    registrar.Parameter("schema_fingerprint", &TThis::SchemaFingerprint);
    registrar.Parameter("closed_through", &TThis::ClosedThrough);
    registrar.Parameter("published_offsets", &TThis::PublishedOffsets);
}

////////////////////////////////////////////////////////////////////////////////

bool IsStaticTableExportCursorAt(const TStaticTableExportCursor& cursor, i64 offset)
{
    // A cursor that never committed accepts any first offset: the source may start past zero.
    return cursor.NextOffset == offset || (!cursor.HighestWindow && cursor.NextOffset <= offset);
}

EStaticTableExportCommitOutcome ClassifyStaticTableExportCommit(
    ui64 window,
    std::span<const TStaticTableExportDelta> deltas,
    const TStaticTableExportCursors& cursors,
    std::vector<int>* fenced)
{
    bool committed = true;
    bool notCommitted = true;
    for (const auto& delta : deltas) {
        const auto& cursor = GetOrCrash(cursors, delta.Partition);
        // A newer owner may have rewritten the same offsets, so its cursor says nothing about this commit.
        if (cursor->Generation != delta.Generation) {
            fenced->push_back(delta.Partition);
            continue;
        }
        committed &= cursor->NextOffset == delta.NextOffset && cursor->HighestWindow == window;
        notCommitted &= IsStaticTableExportCursorAt(*cursor, delta.FirstOffset);
    }
    if (committed && std::ssize(*fenced) < std::ssize(deltas)) {
        return EStaticTableExportCommitOutcome::Committed;
    }
    return notCommitted ? EStaticTableExportCommitOutcome::NotCommitted : EStaticTableExportCommitOutcome::Unknown;
}

////////////////////////////////////////////////////////////////////////////////

TStaticTableExportWindowStream::TStaticTableExportWindowStream(ui64 window, ITransactionPtr transaction)
    : Window_(window)
    , Transaction_(std::move(transaction))
{ }

TStaticTableExportWindowStream::~TStaticTableExportWindowStream()
{
    if (!Finished_) {
        YT_UNUSED_FUTURE(Transaction_->Abort());
    }
}

////////////////////////////////////////////////////////////////////////////////

TStaticTableExportWriterStorage::TStaticTableExportWriterStorage(
    IClientPtr client,
    TStaticTableExportParametersPtr parameters,
    TTableSchemaPtr schema,
    INodePtr owner,
    std::string sourceIdentity,
    TTableWriterConfigPtr tableWriterConfig,
    int tableWriterCount,
    const NProfiling::TProfiler& profiler)
    : Client_(std::move(client))
    , Parameters_(std::move(parameters))
    , Schema_(std::move(schema))
    , SchemaFingerprint_(FarmFingerprint(ConvertToYsonString(Schema_, EYsonFormat::Binary).AsStringBuf()))
    , Owner_(std::move(owner))
    , SourceIdentity_(std::move(sourceIdentity))
    , TableWriterConfig_(std::move(tableWriterConfig))
    , TableWriterCount_(tableWriterCount)
    , CommitTimer_(profiler.Timer("/commit_duration"))
    , PublicationTimer_(profiler.Timer("/publication_duration"))
    , PublicationConflicts_(profiler.Counter("/publication_retries"))
{ }

NYPath::TYPath TStaticTableExportWriterStorage::GetRootPath() const
{
    return Parameters_->OutputDirectory.GetPath() + "/_export_state";
}

NYPath::TYPath TStaticTableExportWriterStorage::GetPartitionsPath() const
{
    return GetRootPath() + "/partitions";
}

NYPath::TYPath TStaticTableExportWriterStorage::GetGatePath(ui64 window) const
{
    return Format("%v/windows/%v", GetRootPath(), window);
}

void TStaticTableExportWriterStorage::ValidateProgress(const TStaticTableExportProgressPtr& progress) const
{
    THROW_ERROR_EXCEPTION_UNLESS(progress->FormatVersion == StaticTableExportFormatVersion,
        "Static table export state has unsupported format version %v, expected %v",
        progress->FormatVersion,
        StaticTableExportFormatVersion);
    THROW_ERROR_EXCEPTION_UNLESS(AreNodesEqual(progress->Owner, Owner_) && progress->SourceIdentity == SourceIdentity_,
        "Static table export state belongs to another export")
        .With("output_directory", Parameters_->OutputDirectory.GetPath());
    THROW_ERROR_EXCEPTION_UNLESS(std::ssize(progress->PublishedOffsets) == progress->PartitionCount,
        "Static table export state is corrupted");
}

TStaticTableExportProgressPtr TStaticTableExportWriterStorage::ReadProgress(const IClientBasePtr& client) const
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Leader;
    auto progressOrError = WaitFor(client->GetNode(GetRootPath() + "/@progress", options));
    if (progressOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        return nullptr;
    }
    auto progress = ConvertTo<TStaticTableExportProgressPtr>(progressOrError.ValueOrThrow());
    ValidateProgress(progress);
    return progress;
}

TStaticTableExportProgressPtr TStaticTableExportWriterStorage::ReadProgress()
{
    return ReadProgress(Client_);
}

TStaticTableExportCursors TStaticTableExportWriterStorage::ReadCursors(
    const IClientBasePtr& client,
    const std::vector<int>& partitions) const
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Leader;
    std::vector<std::string> keys;
    for (int partition : partitions) {
        keys.push_back(MakePartitionKey(partition));
    }
    options.Attributes = TAttributeFilter(std::move(keys));
    const auto node = ConvertToNode(WaitFor(client->GetNode(GetPartitionsPath(), options))
            .ValueOrThrow());
    TStaticTableExportCursors cursors;
    for (int partition : partitions) {
        auto cursor = node->Attributes().Find<TStaticTableExportCursorPtr>(MakePartitionKey(partition));
        cursors.emplace(partition, cursor ? std::move(cursor) : New<TStaticTableExportCursor>());
    }
    return cursors;
}

void TStaticTableExportWriterStorage::LockPartitions(
    const ITransactionPtr& transaction,
    std::vector<int> partitions,
    TInstant deadline,
    TDuration backoff) const
{
    // Ascending key order keeps concurrent lockers acyclic.
    std::sort(partitions.begin(), partitions.end());
    for (int partition : partitions) {
        TLockNodeOptions options;
        options.AttributeKey = MakePartitionKey(partition);
        while (true) {
            auto lockResultOrError = WaitFor(transaction->LockNode(GetPartitionsPath(), NCypressClient::ELockMode::Shared, options));
            if (lockResultOrError.IsOK()) {
                break;
            }
            if (!lockResultOrError.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict) ||
                TInstant::Now() + backoff > deadline)
            {
                THROW_ERROR_EXCEPTION("Failed to lock static table export partition %v",
                    partition)
                    .With(TError(lockResultOrError));
            }
            TDelayedExecutor::WaitForDuration(backoff);
        }
    }
}

void TStaticTableExportWriterStorage::SaveCursors(const ITransactionPtr& transaction, const TStaticTableExportCursors& cursors) const
{
    if (cursors.empty()) {
        return;
    }
    auto attributes = CreateEphemeralAttributes();
    for (const auto& [partition, cursor] : cursors) {
        attributes->Set(MakePartitionKey(partition), cursor);
    }
    WaitFor(transaction->MultisetAttributesNode(GetPartitionsPath() + "/@", attributes->ToMap()))
        .ThrowOnError();
}

ITransactionPtr TStaticTableExportWriterStorage::StartTransaction(TDuration timeout, std::optional<TInstant> deadline) const
{
    TTransactionStartOptions options;
    options.Timeout = timeout;
    options.Deadline = deadline;
    return WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Master, options))
        .ValueOrThrow();
}

std::pair<TStaticTableExportProgressPtr, TStaticTableExportCursors> TStaticTableExportWriterStorage::Claim(
    std::span<const TStaticTableExportClaim> claims,
    TDuration timeout)
{
    std::vector<int> partitions;
    for (const auto& claim : claims) {
        partitions.push_back(claim.Partition);
    }
    // The partition cursors exist only after bootstrap.
    if (!ReadProgress(Client_)) {
        return {};
    }
    const auto transaction = StartTransaction(timeout);
    auto abortGuard = AbortOnExit(transaction);
    LockPartitions(transaction, partitions, TInstant::Now() + timeout, TDuration::MilliSeconds(100));
    auto progress = ReadProgress(transaction);
    THROW_ERROR_EXCEPTION_UNLESS(progress, "Static table export state disappeared");
    for (int partition : partitions) {
        THROW_ERROR_EXCEPTION_UNLESS(partition >= 0 && partition < progress->PartitionCount,
            "Static table export source topology changed")
            .With("partition", partition);
    }
    auto cursors = ReadCursors(transaction, partitions);
    TStaticTableExportCursors updates;
    for (const auto& claim : claims) {
        const auto& cursor = GetOrCrash(cursors, claim.Partition);
        // Only a strictly newer job takes ownership; an older one stays fenced.
        if (cursor->Generation < claim.Generation) {
            cursor->Generation = claim.Generation;
            updates[claim.Partition] = cursor;
        }
    }
    SaveCursors(transaction, updates);
    abortGuard.Release();
    WaitFor(transaction->Commit())
        .ThrowOnError();
    return {std::move(progress), std::move(cursors)};
}

TStaticTableExportCursors TStaticTableExportWriterStorage::ReadCursors(const std::vector<int>& partitions, TDuration timeout)
{
    const auto transaction = StartTransaction(timeout);
    auto abortGuard = AbortOnExit(transaction);
    LockPartitions(transaction, partitions, TInstant::Now() + timeout, TDuration::MilliSeconds(100));
    return ReadCursors(transaction, partitions);
}

std::pair<TStaticTableExportWindowStreamPtr, TStaticTableExportProgressPtr> TStaticTableExportWriterStorage::OpenStream(
    ui64 window,
    TDuration timeout,
    TInstant deadline)
{
    auto stream = New<TStaticTableExportWindowStream>(window, StartTransaction(timeout, deadline));
    // The shared gate lock is held until commit, so the publisher cannot move the table under a writer.
    auto lockResultOrError = WaitFor(stream->Transaction_->LockNode(GetGatePath(window), NCypressClient::ELockMode::Shared));
    if (!lockResultOrError.IsOK()) {
        if (!IsGateUnavailable(lockResultOrError)) {
            lockResultOrError.ThrowOnError();
        }
        return {nullptr, ReadProgress()};
    }
    // Read after the lock: an earlier read could miss a publication that completed before it.
    auto progress = ReadProgress(stream->Transaction_);
    THROW_ERROR_EXCEPTION_UNLESS(progress, "Static table export state disappeared");
    // A writer of another schema waits until the publisher migrates the gates or the writer restarts.
    if (window <= progress->ClosedThrough || !HasCurrentSchema(progress)) {
        return {nullptr, std::move(progress)};
    }
    return {std::move(stream), std::move(progress)};
}

void TStaticTableExportWriterStorage::WriteStream(
    const TStaticTableExportWindowStreamPtr& stream,
    const TNameTablePtr& nameTable,
    std::span<const TSharedRange<TUnversionedRow>> rowRanges,
    TDuration ioTimeout,
    bool backlogged)
{
    YT_VERIFY(!stream->Finished_);
    std::vector<i64> rangeDataWeights;
    for (const auto& rows : rowRanges) {
        auto& rangeDataWeight = rangeDataWeights.emplace_back();
        for (auto row : rows) {
            rangeDataWeight += GetDataWeight(row);
        }
    }
    // A backlog means the writers fall behind, so the stream adds one; a stream that keeps up writes large chunks.
    const auto writerCount = std::min<i64>(
        std::max<i64>(std::ssize(stream->Writers_), 1) + (backlogged && !stream->Writers_.empty() ? 1 : 0),
        TableWriterCount_);
    if (std::ssize(stream->Writers_) < writerCount) {
        NYPath::TRichYPath path(GetGatePath(stream->Window_) + "/table");
        path.SetAppend(true);
        TTableWriterOptions options;
        options.Config = TableWriterConfig_;
        std::vector<TFuture<ITableWriterPtr>> writers;
        for (auto index = std::ssize(stream->Writers_); index < writerCount; ++index) {
            writers.push_back(stream->Transaction_->CreateTableWriter(path, options));
        }
        auto created = WaitFor(AllSucceeded(std::move(writers)).WithTimeout(ioTimeout))
            .ValueOrThrow();
        for (auto& writer : created) {
            stream->Writers_.push_back(std::move(writer));
            stream->IdMappings_.emplace_back();
            stream->WriterDataTimes_.push_back(TInstant::Now());
        }
    }

    // The longest idle writers get the first messages, then each goes to the least loaded one.
    std::vector<int> order(stream->Writers_.size());
    std::iota(order.begin(), order.end(), 0);
    std::ranges::sort(order, {}, [&] (int index) {
        return stream->WriterDataTimes_[index];
    });
    std::vector<std::vector<TSharedRange<TUnversionedRow>>> shares(stream->Writers_.size());
    std::vector<i64> shareDataWeights(stream->Writers_.size());
    for (int index = 0; index < std::ssize(rowRanges); ++index) {
        const int writer = index < std::ssize(order)
            ? order[index]
            : std::ranges::min_element(shareDataWeights) - shareDataWeights.begin();
        shareDataWeights[writer] += rangeDataWeights[index];
        shares[writer].push_back(rowRanges[index]);
    }

    // The proxy drops an upload without data for a minute, so a writer the load no longer needs is closed in time.
    const auto now = TInstant::Now();
    std::vector<TFuture<void>> closes;
    std::vector<ITableWriterPtr> closing;
    for (int index = std::ssize(shares) - 1; index >= 0; --index) {
        if (!shares[index].empty()) {
            stream->WriterDataTimes_[index] = now;
        } else if (now >= stream->WriterDataTimes_[index] + MaxSurplusWriterIdleTime) {
            closes.push_back(stream->Writers_[index]->Close().WithTimeout(ioTimeout));
            closing.push_back(stream->Writers_[index]);
            stream->Writers_.erase(stream->Writers_.begin() + index);
            stream->IdMappings_.erase(stream->IdMappings_.begin() + index);
            stream->WriterDataTimes_.erase(stream->WriterDataTimes_.begin() + index);
            shares.erase(shares.begin() + index);
        }
    }

    // The closes run with the writes, so a slow chunk finish does not idle the kept writers.
    auto writes = std::move(closes);
    for (int index = 0; index < std::ssize(shares); ++index) {
        if (shares[index].empty()) {
            continue;
        }
        writes.push_back(BIND([stream, index, nameTable, rows = std::move(shares[index]), ioTimeout] {
            WriteRows(stream->Writers_[index], &stream->IdMappings_[index], nameTable, rows, ioTimeout);
        })
                .AsyncVia(GetCurrentInvoker())
                .Run());
    }
    // A failed write leaves the others running on the stream, so all of them finish before the error is thrown.
    const auto results = WaitFor(AllSet(std::move(writes)))
        .ValueOrThrow();
    for (const auto& result : results) {
        result.ThrowOnError();
    }
}

void TStaticTableExportWriterStorage::WriteRows(
    const ITableWriterPtr& writer,
    std::vector<int>* idMapping,
    const TNameTablePtr& nameTable,
    std::span<const TSharedRange<TUnversionedRow>> rowRanges,
    TDuration ioTimeout)
{
    const auto& writerNameTable = writer->GetNameTable();
    for (int columnId = std::ssize(*idMapping); columnId < nameTable->GetSize(); ++columnId) {
        idMapping->push_back(writerNameTable->GetIdOrRegisterName(nameTable->GetName(columnId)));
    }

    // Only cell headers are copied to remap ids; |rowRanges| own the values until the writer is ready.
    const auto buffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> frame;
    i64 frameDataWeight = 0;
    auto flush = [&] {
        if (frame.empty()) {
            return;
        }
        if (!writer->Write(frame)) {
            WaitFor(writer->GetReadyEvent().WithTimeout(ioTimeout))
                .ThrowOnError();
        }
        frame.clear();
        buffer->Clear();
        frameDataWeight = 0;
    };
    for (const auto& rows : rowRanges) {
        for (auto row : rows) {
            auto captured = buffer->CaptureRow(row, /*captureValues*/ false);
            for (auto& value : captured) {
                value.Id = (*idMapping)[value.Id];
            }
            frame.push_back(captured);
            frameDataWeight += GetDataWeight(row);
            if (frameDataWeight >= MaxWriteFrameDataWeight) {
                flush();
            }
        }
    }
    flush();
}

void TStaticTableExportWriterStorage::CloseStream(const TStaticTableExportWindowStreamPtr& stream, TDuration ioTimeout)
{
    if (stream->Writers_.empty()) {
        return;
    }
    std::vector<TFuture<void>> closes;
    for (const auto& writer : stream->Writers_) {
        closes.push_back(writer->Close().WithTimeout(ioTimeout));
    }
    WaitFor(AllSucceeded(std::move(closes)))
        .ThrowOnError();
    // A later write of the same transaction appends through new writers.
    stream->Writers_.clear();
    stream->IdMappings_.clear();
    stream->WriterDataTimes_.clear();
}

TStaticTableExportCursors TStaticTableExportWriterStorage::CommitStream(
    const TStaticTableExportWindowStreamPtr& stream,
    std::span<const TStaticTableExportDelta> deltas,
    ui64 closedThrough,
    TDuration lockBackoff)
{
    NProfiling::TEventTimerGuard timer(CommitTimer_);
    const auto& transaction = stream->Transaction_;
    std::vector<int> partitions;
    for (const auto& delta : deltas) {
        partitions.push_back(delta.Partition);
    }
    LockPartitions(transaction, partitions, TInstant::Now() + ControlTransactionTimeout, lockBackoff);
    auto cursors = ReadCursors(transaction, partitions);
    const auto window = stream->Window_;
    for (const auto& delta : deltas) {
        const auto& cursor = GetOrCrash(cursors, delta.Partition);
        THROW_ERROR_EXCEPTION_UNLESS(cursor->Generation == delta.Generation,
            "Static table export partition %v is owned by another job",
            delta.Partition)
            .With("generation", delta.Generation)
            .With("owner_generation", cursor->Generation);
        THROW_ERROR_EXCEPTION_UNLESS(IsStaticTableExportCursorAt(*cursor, delta.FirstOffset) && cursor->HighestWindow.value_or(0) <= window,
            "Static table export stream is not the next durable prefix of partition %v",
            delta.Partition)
            .With("first_offset", delta.FirstOffset)
            .With("durable_next_offset", cursor->NextOffset)
            .With("window", window)
            .With("highest_window", cursor->HighestWindow);
        auto& pending = cursor->PendingRanges;
        pending.erase(pending.begin(), pending.upper_bound(closedThrough));
        auto& range = pending[window];
        if (!range) {
            range = New<TStaticTableExportPart>();
            range->FirstOffset = delta.FirstOffset;
        }
        range->NextOffset = delta.NextOffset;
        cursor->NextOffset = delta.NextOffset;
        cursor->HighestWindow = window;
    }
    SaveCursors(transaction, cursors);
    WaitFor(transaction->Commit())
        .ThrowOnError();
    stream->Finished_ = true;
    return cursors;
}

void TStaticTableExportWriterStorage::AbortStream(const TStaticTableExportWindowStreamPtr& stream)
{
    if (!std::exchange(stream->Finished_, true)) {
        YT_UNUSED_FUTURE(stream->Transaction_->Abort());
    }
}

void TStaticTableExportWriterStorage::CreateGate(const ITransactionPtr& transaction, ui64 window) const
{
    const auto path = GetGatePath(window);
    WaitFor(transaction->CreateNode(path, NObjectClient::EObjectType::MapNode))
        .ThrowOnError();
    TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("schema", Schema_);
    options.Attributes->Set("compression_codec", Parameters_->CompressionCodec);
    options.Attributes->Set("optimize_for", Parameters_->OptimizeFor);
    options.Attributes->Set("erasure_codec", NErasure::ECodec::None);
    options.Attributes->Set("replication_factor", 3);
    options.Attributes->Set("chunk_merger_mode", "none");
    WaitFor(transaction->CreateNode(path + "/table", NObjectClient::EObjectType::Table, options))
        .ThrowOnError();
}

TStaticTableExportProgressPtr TStaticTableExportWriterStorage::Bootstrap(int partitionCount, ui64 closedThrough)
{
    const auto transaction = StartTransaction(ControlTransactionTimeout);
    auto abortGuard = AbortOnExit(transaction);
    TCreateNodeOptions options;
    options.Recursive = true;
    for (const auto& path : {GetRootPath(), GetPartitionsPath(), GetRootPath() + "/windows"}) {
        // A concurrent bootstrap conflicts on the root and leaves the existing state intact.
        WaitFor(transaction->CreateNode(path, NObjectClient::EObjectType::MapNode, options))
            .ThrowOnError();
    }
    const auto period = Parameters_->WindowPeriod.Seconds();
    for (int index = 1; index <= Parameters_->MaxUnpublishedWindowCount; ++index) {
        CreateGate(transaction, closedThrough + index * period);
    }
    auto progress = New<TStaticTableExportProgress>();
    progress->FormatVersion = StaticTableExportFormatVersion;
    progress->Owner = Owner_;
    progress->SourceIdentity = SourceIdentity_;
    progress->PartitionCount = partitionCount;
    progress->SchemaFingerprint = SchemaFingerprint_;
    progress->ClosedThrough = closedThrough;
    progress->PublishedOffsets.resize(partitionCount);
    WaitFor(transaction->SetNode(GetRootPath() + "/@progress", ConvertToYsonString(progress)))
        .ThrowOnError();
    abortGuard.Release();
    WaitFor(transaction->Commit())
        .ThrowOnError();
    return progress;
}

bool TStaticTableExportWriterStorage::HasCurrentSchema(const TStaticTableExportProgressPtr& progress) const
{
    return progress->SchemaFingerprint == SchemaFingerprint_;
}

bool TStaticTableExportWriterStorage::MigrateSchema()
{
    const auto period = Parameters_->WindowPeriod.Seconds();
    auto progress = ReadProgress();
    THROW_ERROR_EXCEPTION_UNLESS(progress, "Static table export is not bootstrapped");
    const auto transaction = StartTransaction(ControlTransactionTimeout);
    auto abortGuard = AbortOnExit(transaction);
    // Writers of the current schema open no gate before the migration, so only writers of an older one hold locks.
    for (int index = 1; index <= Parameters_->MaxUnpublishedWindowCount; ++index) {
        auto lockResultOrError = WaitFor(transaction->LockNode(GetGatePath(progress->ClosedThrough + index * period), NCypressClient::ELockMode::Exclusive));
        if (lockResultOrError.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
            PublicationConflicts_.Increment();
            return false;
        }
        lockResultOrError.ThrowOnError();
    }
    const auto closedThrough = progress->ClosedThrough;
    progress = ReadProgress(transaction);
    if (progress->ClosedThrough != closedThrough) {
        return false;
    }
    for (int index = 1; index <= Parameters_->MaxUnpublishedWindowCount; ++index) {
        const auto window = closedThrough + index * period;
        // A written gate accepts only a compatible change, like a new optional column of a strict schema.
        TAlterTableOptions options;
        options.TransactionId = transaction->GetId();
        options.Schema = *Schema_;
        auto alterError = WaitFor(Client_->AlterTable(GetGatePath(window) + "/table", options));
        THROW_ERROR_EXCEPTION_IF_FAILED(alterError,
            "Static table export cannot change the schema of window %v",
            window);
    }
    progress->SchemaFingerprint = SchemaFingerprint_;
    WaitFor(transaction->SetNode(GetRootPath() + "/@progress", ConvertToYsonString(progress)))
        .ThrowOnError();
    abortGuard.Release();
    WaitFor(transaction->Commit())
        .ThrowOnError();
    return true;
}

EStaticTableExportPublishResult TStaticTableExportWriterStorage::PublishNext(
    const TBuildStaticTableExportPublication& buildPublication,
    bool tailOnly,
    std::optional<TDuration> tableTtl)
{
    const auto period = Parameters_->WindowPeriod.Seconds();
    auto progress = ReadProgress();
    THROW_ERROR_EXCEPTION_UNLESS(progress, "Static table export is not bootstrapped");
    const auto window = progress->ClosedThrough + period;
    const auto gate = GetGatePath(window);

    const auto transaction = StartTransaction(ControlTransactionTimeout);
    auto abortGuard = AbortOnExit(transaction);
    // Never wait for writers here: a waiting lock would stall the controller and still admit new shared lockers.
    auto lockResultOrError = WaitFor(transaction->LockNode(gate, NCypressClient::ELockMode::Exclusive));
    if (lockResultOrError.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict)) {
        PublicationConflicts_.Increment();
        return EStaticTableExportPublishResult::Busy;
    }
    lockResultOrError.ThrowOnError();
    progress = ReadProgress(transaction);
    // A gate created now must have the schema of the others.
    if (progress->ClosedThrough + period != window || !HasCurrentSchema(progress)) {
        return EStaticTableExportPublishResult::Busy;
    }

    std::vector<int> partitions(progress->PartitionCount);
    std::iota(partitions.begin(), partitions.end(), 0);
    const auto cursors = ReadCursors(transaction, partitions);
    if (tailOnly && std::none_of(cursors.begin(), cursors.end(), [&] (const auto& item) {
            return item.second->HighestWindow.value_or(0) >= window;
        }))
    {
        return EStaticTableExportPublishResult::EmptyTail;
    }

    NProfiling::TEventTimerGuard timer(PublicationTimer_);
    std::vector<TStaticTableExportRange> ranges;
    for (int partition : partitions) {
        const auto& cursor = GetOrCrash(cursors, partition);
        auto& published = progress->PublishedOffsets[partition];
        TStaticTableExportRange range{.Partition = partition, .FirstOffset = published, .NextOffset = published};
        if (auto it = cursor->PendingRanges.find(window); it != cursor->PendingRanges.end()) {
            // Nothing of a partition is published before its first range, which starts at its first exported offset.
            THROW_ERROR_EXCEPTION_UNLESS((it->second->FirstOffset == published || published == 0) && it->second->NextOffset <= cursor->NextOffset,
                "Non-contiguous static table export range of partition %v in window %v",
                partition,
                window)
                .With("published_offset", published)
                .With("first_offset", it->second->FirstOffset);
            range.FirstOffset = it->second->FirstOffset;
            range.NextOffset = it->second->NextOffset;
        }
        published = range.NextOffset;
        ranges.push_back(range);
    }

    const auto publication = buildPublication(window, ranges);
    THROW_ERROR_EXCEPTION_UNLESS(!publication.Name.empty() && publication.Name.find('/') == std::string::npos,
        "Static table export publication name must be a nonempty path leaf")
        .With("name", publication.Name);
    const auto destination = Parameters_->OutputDirectory.GetPath() + "/" + publication.Name;
    WaitFor(transaction->MoveNode(gate + "/table", destination))
        .ThrowOnError();
    auto attributes = publication.Attributes ? publication.Attributes->Clone() : CreateEphemeralAttributes();
    attributes->Set("window_start", window);
    attributes->Set("window_end", window + period);
    if (tableTtl) {
        attributes->Set("expiration_time", TInstant::Now() + *tableTtl);
    }
    WaitFor(transaction->MultisetAttributesNode(destination + "/@", attributes->ToMap()))
        .ThrowOnError();
    WaitFor(transaction->RemoveNode(gate))
        .ThrowOnError();
    CreateGate(transaction, window + Parameters_->MaxUnpublishedWindowCount * period);
    progress->ClosedThrough = window;
    WaitFor(transaction->SetNode(GetRootPath() + "/@progress", ConvertToYsonString(progress)))
        .ThrowOnError();
    abortGuard.Release();
    WaitFor(transaction->Commit())
        .ThrowOnError();
    return EStaticTableExportPublishResult::Published;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
