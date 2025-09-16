package tech.ytsaurus.client;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.AbortJob;
import tech.ytsaurus.client.request.AbortOperation;
import tech.ytsaurus.client.request.AbortQuery;
import tech.ytsaurus.client.request.AbortTransaction;
import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.AbstractModifyRowsRequest;
import tech.ytsaurus.client.request.AlterQuery;
import tech.ytsaurus.client.request.AlterTable;
import tech.ytsaurus.client.request.AlterTableReplica;
import tech.ytsaurus.client.request.BuildSnapshot;
import tech.ytsaurus.client.request.CheckClusterLiveness;
import tech.ytsaurus.client.request.CommitTransaction;
import tech.ytsaurus.client.request.CompleteOperation;
import tech.ytsaurus.client.request.CreateObject;
import tech.ytsaurus.client.request.CreateShuffleReader;
import tech.ytsaurus.client.request.CreateShuffleWriter;
import tech.ytsaurus.client.request.CreateTablePartitionReader;
import tech.ytsaurus.client.request.FinishDistributedWriteSession;
import tech.ytsaurus.client.request.FlowExecute;
import tech.ytsaurus.client.request.FlowExecuteResult;
import tech.ytsaurus.client.request.FreezeTable;
import tech.ytsaurus.client.request.GcCollect;
import tech.ytsaurus.client.request.GenerateTimestamps;
import tech.ytsaurus.client.request.GetFlowView;
import tech.ytsaurus.client.request.GetFlowViewResult;
import tech.ytsaurus.client.request.GetInSyncReplicas;
import tech.ytsaurus.client.request.GetJob;
import tech.ytsaurus.client.request.GetJobStderr;
import tech.ytsaurus.client.request.GetJobStderrResult;
import tech.ytsaurus.client.request.GetOperation;
import tech.ytsaurus.client.request.GetPipelineDynamicSpec;
import tech.ytsaurus.client.request.GetPipelineDynamicSpecResult;
import tech.ytsaurus.client.request.GetPipelineSpec;
import tech.ytsaurus.client.request.GetPipelineSpecResult;
import tech.ytsaurus.client.request.GetPipelineState;
import tech.ytsaurus.client.request.GetPipelineStateResult;
import tech.ytsaurus.client.request.GetQuery;
import tech.ytsaurus.client.request.GetQueryResult;
import tech.ytsaurus.client.request.GetTablePivotKeys;
import tech.ytsaurus.client.request.GetTabletInfos;
import tech.ytsaurus.client.request.ListJobs;
import tech.ytsaurus.client.request.ListJobsResult;
import tech.ytsaurus.client.request.ListQueries;
import tech.ytsaurus.client.request.ListQueriesResult;
import tech.ytsaurus.client.request.ListQueueConsumerRegistrations;
import tech.ytsaurus.client.request.ListQueueConsumerRegistrationsResult;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.PatchOperationSpec;
import tech.ytsaurus.client.request.PausePipeline;
import tech.ytsaurus.client.request.PingDistributedWriteSession;
import tech.ytsaurus.client.request.PingTransaction;
import tech.ytsaurus.client.request.PullConsumer;
import tech.ytsaurus.client.request.PullQueue;
import tech.ytsaurus.client.request.Query;
import tech.ytsaurus.client.request.QueryResult;
import tech.ytsaurus.client.request.ReadQueryResult;
import tech.ytsaurus.client.request.RegisterQueueConsumer;
import tech.ytsaurus.client.request.RemountTable;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.client.request.ResumeOperation;
import tech.ytsaurus.client.request.SetPipelineDynamicSpec;
import tech.ytsaurus.client.request.SetPipelineDynamicSpecResult;
import tech.ytsaurus.client.request.SetPipelineSpec;
import tech.ytsaurus.client.request.SetPipelineSpecResult;
import tech.ytsaurus.client.request.ShuffleHandle;
import tech.ytsaurus.client.request.StartDistributedWriteSession;
import tech.ytsaurus.client.request.StartPipeline;
import tech.ytsaurus.client.request.StartQuery;
import tech.ytsaurus.client.request.StartShuffle;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.StopPipeline;
import tech.ytsaurus.client.request.SuspendOperation;
import tech.ytsaurus.client.request.TabletInfo;
import tech.ytsaurus.client.request.TrimTable;
import tech.ytsaurus.client.request.UnfreezeTable;
import tech.ytsaurus.client.request.UnmountTable;
import tech.ytsaurus.client.request.UpdateOperationParameters;
import tech.ytsaurus.client.request.WriteTableFragment;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.EAtomicity;
import tech.ytsaurus.rpcproxy.ETableReplicaMode;
import tech.ytsaurus.ysontree.YTreeNode;

public interface ApiServiceClient extends TransactionalClient {
    CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction);

    /**
     * @deprecated prefer to use {@link #startTransaction(StartTransaction)}
     */
    @Deprecated
    default CompletableFuture<ApiServiceTransaction> startTransaction(
            StartTransaction.BuilderBase<?> startTransaction) {
        return startTransaction(startTransaction.build());
    }

    /**
     * @deprecated prefer to use {@link #startTransaction(StartTransaction)}
     */
    @Deprecated
    default CompletableFuture<ApiServiceTransaction> startTransaction(ApiServiceTransactionOptions options) {
        return startTransaction(options.toStartTransaction());
    }

    CompletableFuture<Void> pingTransaction(PingTransaction req);

    default CompletableFuture<Void> pingTransaction(GUID id) {
        return pingTransaction(new PingTransaction(id));
    }

    CompletableFuture<Void> commitTransaction(CommitTransaction req);

    default CompletableFuture<Void> commitTransaction(GUID id) {
        return commitTransaction(new CommitTransaction(id));
    }

    CompletableFuture<Void> abortTransaction(AbortTransaction req);

    default CompletableFuture<Void> abortTransaction(GUID id) {
        return abortTransaction(new AbortTransaction(id));
    }

    CompletableFuture<List<YTreeNode>> getTablePivotKeys(GetTablePivotKeys req);

    CompletableFuture<GUID> createObject(CreateObject req);

    CompletableFuture<Void> checkClusterLiveness(CheckClusterLiveness req);

    <T> CompletableFuture<Void> lookupRows(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    );

    default <T> CompletableFuture<Void> lookupRows(
            AbstractLookupRowsRequest.Builder<?, ?> request,
            YTreeRowSerializer<T> serializer,
            ConsumerSource<T> consumer
    ) {
        return lookupRows(request.build(), serializer, consumer);
    }

    @Deprecated
    default CompletableFuture<UnversionedRowset> lookupRows(
            LookupRowsRequest.BuilderBase<?> request, YtTimestamp timestamp) {
        return lookupRows(request.setTimestamp(timestamp));
    }

    @Deprecated
    default CompletableFuture<VersionedRowset> versionedLookupRows(
            LookupRowsRequest.BuilderBase<?> request, YtTimestamp timestamp) {
        return versionedLookupRows(request.setTimestamp(timestamp));
    }

    CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest<?, ?> request);

    default CompletableFuture<Void> modifyRows(GUID transactionId, AbstractModifyRowsRequest.Builder<?, ?> request) {
        return modifyRows(transactionId, (AbstractModifyRowsRequest<?, ?>) request.build());
    }

    CompletableFuture<Long> buildSnapshot(BuildSnapshot req);

    CompletableFuture<Void> gcCollect(GcCollect req);

    default CompletableFuture<Void> gcCollect(GUID cellId) {
        return gcCollect(new GcCollect(cellId));
    }

    /**
     * Mount table.
     * <p>
     * This method doesn't wait until tablets become mounted.
     *
     * @see MountTable
     * @see CompoundClient#mountTableAndWaitTablets(MountTable)
     */
    CompletableFuture<Void> mountTable(MountTable req);

    /**
     * @deprecated prefer to use {@link #mountTable(MountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> mountTable(MountTable.BuilderBase<?> req) {
        return mountTable(req.build());
    }

    /**
     * Unmount table.
     * <p>
     * This method doesn't wait until tablets become unmounted.
     *
     * @see UnmountTable
     * @see CompoundClient#unmountTableAndWaitTablets(UnmountTable)
     */
    CompletableFuture<Void> unmountTable(UnmountTable req);

    /**
     * @deprecated prefer to use {@link #unmountTable(UnmountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unmountTable(UnmountTable.BuilderBase<?> req) {
        return unmountTable(req.build());
    }

    default CompletableFuture<Void> remountTable(String path) {
        return remountTable(RemountTable.builder().setPath(path).build());
    }

    CompletableFuture<Void> remountTable(RemountTable req);

    /**
     * @deprecated prefer to use {@link #remountTable(RemountTable)}
     */
    @Deprecated
    default CompletableFuture<Void> remountTable(RemountTable.BuilderBase<?> req) {
        return remountTable(req.build());
    }

    default CompletableFuture<Void> freezeTable(String path) {
        return freezeTable(path, null);
    }

    default CompletableFuture<Void> freezeTable(String path, @Nullable Duration requestTimeout) {
        return freezeTable(FreezeTable.builder().setPath(path).setTimeout(requestTimeout).build());
    }

    CompletableFuture<Void> freezeTable(FreezeTable req);

    /**
     * @deprecated prefer to use {@link #freezeTable(FreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> freezeTable(FreezeTable.BuilderBase<?> req) {
        return freezeTable(req.build());
    }

    default CompletableFuture<Void> unfreezeTable(String path) {
        return unfreezeTable(path, null);
    }

    default CompletableFuture<Void> unfreezeTable(String path, @Nullable Duration requestTimeout) {
        return unfreezeTable(UnfreezeTable.builder().setPath(path).setTimeout(requestTimeout).build());
    }

    default CompletableFuture<Void> unfreezeTable(FreezeTable req) {
        UnfreezeTable.Builder unfreezeReqBuilder = UnfreezeTable.builder().setPath(req.getPath());
        if (req.getTimeout().isPresent()) {
            unfreezeReqBuilder.setTimeout(req.getTimeout().get());
        }
        return unfreezeTable(unfreezeReqBuilder.build());
    }

    /**
     * @deprecated prefer to use {@link #unfreezeTable(FreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unfreezeTable(FreezeTable.BuilderBase<?> req) {
        return unfreezeTable(req.build());
    }

    CompletableFuture<Void> unfreezeTable(UnfreezeTable req);

    /**
     * @deprecated prefer to use {@link #unfreezeTable(UnfreezeTable)}
     */
    @Deprecated
    default CompletableFuture<Void> unfreezeTable(UnfreezeTable.BuilderBase<?> req) {
        return unfreezeTable(req.build());
    }

    CompletableFuture<List<GUID>> getInSyncReplicas(GetInSyncReplicas request, YtTimestamp timestamp);

    default CompletableFuture<List<GUID>> getInSyncReplicas(
            String path,
            YtTimestamp timestamp,
            TableSchema schema,
            Iterable<? extends List<?>> keys
    ) {
        return getInSyncReplicas(new GetInSyncReplicas(path, schema, keys), timestamp);
    }

    CompletableFuture<List<TabletInfo>> getTabletInfos(GetTabletInfos req);

    default CompletableFuture<List<TabletInfo>> getTabletInfos(String path, List<Integer> tabletIndices) {
        return getTabletInfos(GetTabletInfos.builder().setPath(path).setTabletIndexes(tabletIndices).build());
    }

    CompletableFuture<YtTimestamp> generateTimestamps(GenerateTimestamps req);

    default CompletableFuture<YtTimestamp> generateTimestamps(int count) {
        GenerateTimestamps req = new GenerateTimestamps(count);
        return generateTimestamps(req);
    }

    default CompletableFuture<YtTimestamp> generateTimestamps() {
        return generateTimestamps(1);
    }

    CompletableFuture<Void> reshardTable(ReshardTable req);

    /**
     * @deprecated prefer to use {@link #reshardTable(ReshardTable)}
     */
    @Deprecated
    default CompletableFuture<Void> reshardTable(ReshardTable.BuilderBase<?> req) {
        return reshardTable(req.build());
    }

    default CompletableFuture<Void> trimTable(String path, int tableIndex, long trimmedRowCount) {
        TrimTable req = new TrimTable(path, tableIndex, trimmedRowCount);
        return trimTable(req);
    }

    CompletableFuture<Void> trimTable(TrimTable req);

    CompletableFuture<Void> alterTable(AlterTable req);

    /**
     * @deprecated prefer to use {@link #alterTable(AlterTable)}
     */
    @Deprecated
    default CompletableFuture<Void> alterTable(AlterTable.BuilderBase<?> req) {
        return alterTable(req.build());
    }

    CompletableFuture<Void> alterTableReplica(
            GUID replicaId,
            boolean enabled,
            ETableReplicaMode mode,
            boolean preserveTimestamp,
            EAtomicity atomicity
    );

    CompletableFuture<Void> alterTableReplica(AlterTableReplica req);

    /**
     * Reads a batch of rows from a given partition of a given queue, starting at (at least) the given offset.
     * Requires the user to have read-access to the specified queue.
     * There is no guarantee that `PullQueue#rowBatchReadOptions#maxRowCount` rows will be returned even if they are
     * in the queue.
     * <p>
     *
     * @param req PullQueue request.
     * @return CompletableFuture that completes with {@link QueueRowset}.
     */
    CompletableFuture<QueueRowset> pullQueue(PullQueue req);

    /**
     * Same as {@link ApiServiceClient#pullQueue(PullQueue)}, but requires user to have read-access to the consumer
     * and the consumer being registered for the given queue.
     * There is no guarantee that `PullConsumer#rowBatchReadOptions#maxRowCount` rows will be returned even if they
     * are in the queue.
     * <p>
     *
     * @param req PullConsumer request.
     * @return CompletableFuture that completes with {@link QueueRowset}.
     */
    CompletableFuture<QueueRowset> pullConsumer(PullConsumer req);

    CompletableFuture<Void> registerQueueConsumer(RegisterQueueConsumer req);

    /**
     * Request to list queue consumer registrations.
     *
     * @return list of queue consumer registrations.
     * @see ListQueueConsumerRegistrations
     * @see ListQueueConsumerRegistrationsResult
     */
    CompletableFuture<ListQueueConsumerRegistrationsResult> listQueueConsumerRegistrations(
            ListQueueConsumerRegistrations req
    );

    /**
     * Request to start query.
     *
     * @return query id.
     * @see StartQuery
     */
    CompletableFuture<GUID> startQuery(StartQuery req);

    /**
     * Request to abort query.
     *
     * @see AbortQuery
     */
    CompletableFuture<Void> abortQuery(AbortQuery req);

    /**
     * Request to get query result.
     *
     * @return metadata about query result.
     * @see GetQueryResult
     */
    CompletableFuture<QueryResult> getQueryResult(GetQueryResult req);

    /**
     * Request to read query result.
     *
     * @return query result.
     * @see ReadQueryResult
     */
    CompletableFuture<UnversionedRowset> readQueryResult(ReadQueryResult req);

    /**
     * Request to get information about query.
     *
     * @return query.
     * @see GetQuery
     */
    CompletableFuture<Query> getQuery(GetQuery req);

    /**
     * Request to get a list of queries by specified filters.
     *
     * @return list of queries.
     * @see ListQueries
     */
    CompletableFuture<ListQueriesResult> listQueries(ListQueries req);

    /**
     * Request to alter query.
     *
     * @see AlterQuery
     */
    CompletableFuture<Void> alterQuery(AlterQuery req);

    CompletableFuture<YTreeNode> getOperation(GetOperation req);

    /**
     * @deprecated prefer to use {@link #getOperation(GetOperation)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> getOperation(GetOperation.BuilderBase<?> req) {
        return getOperation(req.build());
    }

    /**
     * Request to abort operation.
     * <p>
     * Operation will be finished in erroneous aborted state.
     * <p>
     *
     * @see AbortOperation
     * @see <a href="https://ytsaurus.tech/docs/en/api/commands#abort_job">
     * abort_job documentation
     * </a>
     */
    CompletableFuture<Void> abortOperation(AbortOperation req);

    CompletableFuture<Void> completeOperation(CompleteOperation req);

    CompletableFuture<Void> suspendOperation(SuspendOperation req);

    CompletableFuture<Void> resumeOperation(ResumeOperation req);

    /**
     * Starts a new shuffle under a transaction specified in StartShuffle request.
     *
     * @param req Shuffle parameters including parent transaction id and number of partitions.
     * @return Shuffle descriptor that will be used in createShuffleWriter and createShuffleReader.
     */
    CompletableFuture<ShuffleHandle> startShuffle(StartShuffle req);

    /**
     * Creates ShuffleDataWriter for writing shuffle rows.
     *
     * @param req A request to create Shuffle writer containing shuffle descriptor.
     * @return ShuffleDataWriter that can be used to write shuffle rows.
     */
    CompletableFuture<AsyncWriter<UnversionedRow>> createShuffleWriter(CreateShuffleWriter req);

    /**
     * Creates ShuffleDataReader for reading shuffle rows of specified shuffle partition.
     *
     * @param req A request to create Shuffle reader containing shuffle descriptor and a partition number to read.
     * @return ShuffleDataReader that can be used to read shuffle rows.
     */
    CompletableFuture<AsyncReader<UnversionedRow>> createShuffleReader(CreateShuffleReader req);

    /**
     * Creates TablePartitionReader for reading partition rows.
     *
     * @param req A request to create Partition reader containing cookie and additional reading configs.
     * @return TablePartitionReader that can be used to read partition rows.
     */
    <T> CompletableFuture<AsyncReader<T>> createTablePartitionReader(CreateTablePartitionReader<T> req);

    /**
     * Starts distributed write session.
     *
     * @param req StartDistributedWriteSession request
     * @return DistributedWriteHandle that pings distributed write session and contains DistributedWriteSession id and
     * a list of DistributedWriteCookie that must be passed to WriteTableFragment method.
     */
    CompletableFuture<DistributedWriteHandle> startDistributedWriteSession(StartDistributedWriteSession req);

    /**
     * Pings active distributed write session.
     *
     * @param req Distributed write session request that contain distributed write session id.
     */
    CompletableFuture<Void> pingDistributedWriteSession(PingDistributedWriteSession req);

    /**
     * Finishes distributed write session.
     *
     * @param req Distributed write session finish request with all distributed write fragment resultd
     */
    CompletableFuture<Void> finishDistributedWriteSession(FinishDistributedWriteSession req);

    /**
     * Creates an AsyncFragmentWriter for an active distribute wrote session.
     *
     * @param req Write table fragment request with DistributedWriteCookie
     * @return An AsyncFragmentWriter that must be used to write data.
     */
    <T> CompletableFuture<AsyncFragmentWriter<T>> writeTableFragment(WriteTableFragment<T> req);


    /**
     * @deprecated prefer to use {@link #resumeOperation(ResumeOperation)}
     */
    @Deprecated
    default CompletableFuture<Void> resumeOperation(ResumeOperation.BuilderBase<?> req) {
        return resumeOperation(req.build());
    }

    CompletableFuture<YTreeNode> getJob(GetJob req);

    /**
     * @deprecated prefer to use {@link #getJob(GetJob)}
     */
    @Deprecated
    default CompletableFuture<YTreeNode> getJob(GetJob.BuilderBase<?> req) {
        return getJob(req.build());
    }

    /**
     * Request to abort job.
     * <p>
     * Job will be aborted. In the future scheduler will restart this job.
     * <p>
     *
     * @see <a href="https://ytsaurus.tech/docs/en/api/commands#abort_job">
     * abort_job documentation
     * </a>
     */
    CompletableFuture<Void> abortJob(AbortJob req);

    CompletableFuture<ListJobsResult> listJobs(ListJobs req);

    CompletableFuture<GetJobStderrResult> getJobStderr(GetJobStderr req);

    CompletableFuture<Void> updateOperationParameters(UpdateOperationParameters req);

    /**
     * Patches operation specification for running operation.
     */
    CompletableFuture<Void> patchOperationSpec(PatchOperationSpec req);

    /**
     * @deprecated prefer to use {@link #updateOperationParameters(UpdateOperationParameters)}
     */
    @Deprecated
    default CompletableFuture<Void> updateOperationParameters(
            UpdateOperationParameters.BuilderBase<?> req) {
        return updateOperationParameters(req.build());
    }

    /**
     * Retrieves a Flow pipeline spec for a given pipeline path.
     * <p>
     *
     * @param req a request containing the pipeline path.
     * @return A {@link CompletableFuture} that completes with {@link GetPipelineSpecResult}, containing the pipeline
     * spec and pipeline version.
     */
    CompletableFuture<GetPipelineSpecResult> getPipelineSpec(GetPipelineSpec req);

    /**
     * Sets the Flow pipeline spec for a given pipeline path.
     * <p>
     *
     * @param req A request containing the pipeline path, pipeline spec, expected pipeline version and force flag.
     * @return A {@link CompletableFuture} that completes with {@link SetPipelineSpecResult}, containing the new
     * pipeline version.
     * @throws RuntimeException If the expectedVersion is provided and does not equal the received version.
     */
    CompletableFuture<SetPipelineSpecResult> setPipelineSpec(SetPipelineSpec req);

    /**
     * Retrieves a Flow pipeline dynamic spec for a given pipeline path.
     * <p>
     *
     * @param req A request containing the pipeline path.
     * @return A {@link CompletableFuture} that completes with {@link GetPipelineDynamicSpecResult}, containing the
     * pipeline dynamic spec and pipeline version.
     */
    CompletableFuture<GetPipelineDynamicSpecResult> getPipelineDynamicSpec(GetPipelineDynamicSpec req);

    /**
     * Sets the Flow pipeline dynamic spec for a given pipeline path.
     * <p>
     *
     * @param req A request containing the pipeline path, pipeline dynamic spec, and expected pipeline version.
     * @return A {@link CompletableFuture} that completes with {@link SetPipelineDynamicSpecResult}, containing the
     * new pipeline version.
     * @throws RuntimeException If the expectedVersion is provided and does not equal the received version.
     */
    CompletableFuture<SetPipelineDynamicSpecResult> setPipelineDynamicSpec(SetPipelineDynamicSpec req);

    /**
     * Starts a Flow pipeline for a given pipeline path.
     * <p>
     *
     * @param req The request containing the pipeline path.
     * @return A {@link CompletableFuture} that completes when the pipeline has been successfully started.
     */
    CompletableFuture<Void> startPipeline(StartPipeline req);

    /**
     * Stops a Flow pipeline for a given pipeline path.
     * <p>
     *
     * @param req the request containing the pipeline path.
     * @return A {@link CompletableFuture} that completes when the pipeline has been successfully stopped.
     */
    CompletableFuture<Void> stopPipeline(StopPipeline req);

    /**
     * Pauses a Flow pipeline for a given pipeline path.
     * <p>
     *
     * @param req The request containing the pipeline path.
     * @return A {@link CompletableFuture} that completes when the pipeline has been successfully paused.
     */
    CompletableFuture<Void> pausePipeline(PausePipeline req);

    /**
     * Retrieves the current state of a Flow pipeline for a given pipeline path.
     * <p>
     *
     * @param req The request object containing pipeline path.
     * @return A {@link CompletableFuture} that completes with the {@link GetPipelineStateResult}.
     */
    CompletableFuture<GetPipelineStateResult> getPipelineState(GetPipelineState req);

    /**
     * Retrieves the current Flow view for a given request.
     * <p>
     *
     * @param req the request object containing pipeline path, view path and cached flag.
     * @return A {@link CompletableFuture} that completes with the GetFlowViewResult.
     */
    CompletableFuture<GetFlowViewResult> getFlowView(GetFlowView req);

    /**
     * A universal method for executing arbitrary Flow commands for a given pipeline path.
     * <p>
     *
     * @param req The request containing pipeline path, Flow command and arguments as a {@link YTreeNode}.
     * @return A {@link CompletableFuture} that completes with the {@link FlowExecuteResult}, containing command
     * execution result as a {@link YTreeNode}.
     */
    CompletableFuture<FlowExecuteResult> flowExecute(FlowExecute req);

}
