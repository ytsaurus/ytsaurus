#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/MergeTree/MergeTask.h>

#include <memory>
#include <fmt/format.h>

#include <Common/logger_useful.h>
#include <Common/ActionBlocker.h>
#include <Core/Settings.h>
#include <Common/ProfileEvents.h>
#include <Processors/Transforms/CheckSortedTransform.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Compression/CompressedWriteBuffer.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <IO/IReadableWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/TTLCalcTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace ProfileEvents
{
    extern const Event Merge;
    extern const Event MergedColumns;
    extern const Event GatheredColumns;
    extern const Event MergeTotalMilliseconds;
    extern const Event MergeExecuteMilliseconds;
    extern const Event MergeHorizontalStageExecuteMilliseconds;
    extern const Event MergeVerticalStageExecuteMilliseconds;
    extern const Event MergeProjectionStageExecuteMilliseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

static ColumnsStatistics getStatisticsForColumns(
    const NamesAndTypesList & columns_to_read,
    const StorageMetadataPtr & metadata_snapshot)
{
    ColumnsStatistics all_statistics;
    const auto & all_columns = metadata_snapshot->getColumns();

    for (const auto & column : columns_to_read)
    {
        const auto * desc = all_columns.tryGet(column.name);
        if (desc && !desc->statistics.empty())
        {
            auto statistics = MergeTreeStatisticsFactory::instance().get(desc->statistics);
            all_statistics.push_back(std::move(statistics));
        }
    }
    return all_statistics;
}

static void addMissedColumnsToSerializationInfos(
    size_t num_rows_in_parts,
    const Names & part_columns,
    const ColumnsDescription & storage_columns,
    const SerializationInfo::Settings & info_settings,
    SerializationInfoByName & new_infos)
{
    NameSet part_columns_set(part_columns.begin(), part_columns.end());

    for (const auto & column : storage_columns)
    {
        if (part_columns_set.contains(column.name))
            continue;

        if (column.default_desc.kind != ColumnDefaultKind::Default)
            continue;

        if (column.default_desc.expression)
            continue;

        auto new_info = column.type->createSerializationInfo(info_settings);
        new_info->addDefaults(num_rows_in_parts);
        new_infos.emplace(column.name, std::move(new_info));
    }
}

/// PK columns are sorted and merged, ordinary columns are gathered using info from merge step
void MergeTask::ExecuteAndFinalizeHorizontalPart::extractMergingAndGatheringColumns() const
{
    const auto & sorting_key_expr = global_ctx->metadata_snapshot->getSortingKey().expression;
    Names sort_key_columns_vec = sorting_key_expr->getRequiredColumns();

    std::set<String> key_columns(sort_key_columns_vec.cbegin(), sort_key_columns_vec.cend());

    /// Force sign column for Collapsing mode
    if (ctx->merging_params.mode == MergeTreeData::MergingParams::Collapsing)
        key_columns.emplace(ctx->merging_params.sign_column);

    /// Force version column for Replacing mode
    if (ctx->merging_params.mode == MergeTreeData::MergingParams::Replacing)
    {
        key_columns.emplace(ctx->merging_params.is_deleted_column);
        key_columns.emplace(ctx->merging_params.version_column);
    }

    /// Force sign column for VersionedCollapsing mode. Version is already in primary key.
    if (ctx->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing)
        key_columns.emplace(ctx->merging_params.sign_column);

    /// Force to merge at least one column in case of empty key
    if (key_columns.empty())
        key_columns.emplace(global_ctx->storage_columns.front().name);

    const auto & skip_indexes = global_ctx->metadata_snapshot->getSecondaryIndices();

    for (const auto & index : skip_indexes)
    {
        auto index_columns = index.expression->getRequiredColumns();

        /// Calculate indexes that depend only on one column on vertical
        /// stage and other indexes on horizonatal stage of merge.
        if (index_columns.size() == 1)
        {
            const auto & column_name = index_columns.front();
            global_ctx->skip_indexes_by_column[column_name].push_back(index);
        }
        else
        {
            std::ranges::copy(index_columns, std::inserter(key_columns, key_columns.end()));
            global_ctx->merging_skip_indexes.push_back(index);
        }
    }

    /// TODO: also force "summing" and "aggregating" columns to make Horizontal merge only for such columns

    for (const auto & column : global_ctx->storage_columns)
    {
        if (key_columns.contains(column.name))
        {
            global_ctx->merging_columns.emplace_back(column);

            /// If column is in horizontal stage we need to calculate its indexes on horizontal stage as well
            auto it = global_ctx->skip_indexes_by_column.find(column.name);
            if (it != global_ctx->skip_indexes_by_column.end())
            {
                for (auto & index : it->second)
                    global_ctx->merging_skip_indexes.push_back(std::move(index));

                global_ctx->skip_indexes_by_column.erase(it);
            }
        }
        else
        {
            global_ctx->gathering_columns.emplace_back(column);
        }
    }
}

bool MergeTask::ExecuteAndFinalizeHorizontalPart::prepare()
{
    ProfileEvents::increment(ProfileEvents::Merge);

    String local_tmp_prefix;
    if (global_ctx->need_prefix)
    {
        // projection parts have different prefix and suffix compared to normal parts.
        // E.g. `proj_a.proj` for a normal projection merge and `proj_a.tmp_proj` for a projection materialization merge.
        local_tmp_prefix = global_ctx->parent_part ? "" : "tmp_merge_";
    }
    const String local_tmp_suffix = global_ctx->parent_part ? ctx->suffix : "";

    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

    /// We don't want to perform merge assigned with TTL as normal merge, so
    /// throw exception
    if (isTTLMergeType(global_ctx->future_part->merge_type) && global_ctx->ttl_merges_blocker->isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts with TTL");

    LOG_DEBUG(ctx->log, "Merging {} parts: from {} to {} into {} with storage {}",
        global_ctx->future_part->parts.size(),
        global_ctx->future_part->parts.front()->name,
        global_ctx->future_part->parts.back()->name,
        global_ctx->future_part->part_format.part_type.toString(),
        global_ctx->future_part->part_format.storage_type.toString());

    if (global_ctx->deduplicate)
    {
        if (global_ctx->deduplicate_by_columns.empty())
            LOG_DEBUG(ctx->log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(ctx->log, "DEDUPLICATE BY ('{}')", fmt::join(global_ctx->deduplicate_by_columns, "', '"));
    }

    ctx->disk = global_ctx->space_reservation->getDisk();
    auto local_tmp_part_basename = local_tmp_prefix + global_ctx->future_part->name + local_tmp_suffix;

    std::optional<MergeTreeDataPartBuilder> builder;
    if (global_ctx->parent_part)
    {
        auto data_part_storage = global_ctx->parent_part->getDataPartStorage().getProjection(local_tmp_part_basename,  /* use parent transaction */ false);
        builder.emplace(*global_ctx->data, global_ctx->future_part->name, data_part_storage);
        builder->withParentPart(global_ctx->parent_part);
    }
    else
    {
        auto local_single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + global_ctx->future_part->name, ctx->disk, 0);
        builder.emplace(global_ctx->data->getDataPartBuilder(global_ctx->future_part->name, local_single_disk_volume, local_tmp_part_basename));
        builder->withPartStorageType(global_ctx->future_part->part_format.storage_type);
    }

    builder->withPartInfo(global_ctx->future_part->part_info);
    builder->withPartType(global_ctx->future_part->part_format.part_type);

    global_ctx->new_data_part = std::move(*builder).build();
    auto data_part_storage = global_ctx->new_data_part->getDataPartStoragePtr();

    if (data_part_storage->exists())
        throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Directory {} already exists", data_part_storage->getFullPath());

    data_part_storage->beginTransaction();
    /// Background temp dirs cleaner will not touch tmp projection directory because
    /// it's located inside part's directory
    if (!global_ctx->parent_part)
        global_ctx->temporary_directory_lock = global_ctx->data->getTemporaryPartDirectoryHolder(local_tmp_part_basename);

    global_ctx->storage_columns = global_ctx->metadata_snapshot->getColumns().getAllPhysical();

    auto object_columns = MergeTreeData::getConcreteObjectColumns(global_ctx->future_part->parts, global_ctx->metadata_snapshot->getColumns());
    extendObjectColumns(global_ctx->storage_columns, object_columns, false);
    global_ctx->storage_snapshot = std::make_shared<StorageSnapshot>(*global_ctx->data, global_ctx->metadata_snapshot, std::move(object_columns));

    extractMergingAndGatheringColumns();

    global_ctx->new_data_part->uuid = global_ctx->future_part->uuid;
    global_ctx->new_data_part->partition.assign(global_ctx->future_part->getPartition());
    global_ctx->new_data_part->is_temp = global_ctx->parent_part == nullptr;

    /// In case of replicated merge tree with zero copy replication
    /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
    /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
    global_ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;

    ctx->need_remove_expired_values = false;
    ctx->force_ttl = false;

    if (enabledBlockNumberColumn(global_ctx))
        addGatheringColumn(global_ctx, BlockNumberColumn::name, BlockNumberColumn::type);

    if (enabledBlockOffsetColumn(global_ctx))
        addGatheringColumn(global_ctx, BlockOffsetColumn::name, BlockOffsetColumn::type);

    SerializationInfo::Settings info_settings =
    {
        .ratio_of_defaults_for_sparse = global_ctx->data->getSettings()->ratio_of_defaults_for_sparse_serialization,
        .choose_kind = true,
    };

    SerializationInfoByName infos(global_ctx->storage_columns, info_settings);

    for (const auto & part : global_ctx->future_part->parts)
    {
        global_ctx->new_data_part->ttl_infos.update(part->ttl_infos);
        if (global_ctx->metadata_snapshot->hasAnyTTL() && !part->checkAllTTLCalculated(global_ctx->metadata_snapshot))
        {
            LOG_INFO(ctx->log, "Some TTL values were not calculated for part {}. Will calculate them forcefully during merge.", part->name);
            ctx->need_remove_expired_values = true;
            ctx->force_ttl = true;
        }

        if (!info_settings.isAlwaysDefault())
        {
            auto part_infos = part->getSerializationInfos();

            addMissedColumnsToSerializationInfos(
                part->rows_count,
                part->getColumns().getNames(),
                global_ctx->metadata_snapshot->getColumns(),
                info_settings,
                part_infos);

            infos.add(part_infos);
        }
    }

    const auto & local_part_min_ttl = global_ctx->new_data_part->ttl_infos.part_min_ttl;
    if (local_part_min_ttl && local_part_min_ttl <= global_ctx->time_of_merge)
        ctx->need_remove_expired_values = true;

    global_ctx->new_data_part->setColumns(global_ctx->storage_columns, infos, global_ctx->metadata_snapshot->getMetadataVersion());

    if (ctx->need_remove_expired_values && global_ctx->ttl_merges_blocker->isCancelled())
    {
        LOG_INFO(ctx->log, "Part {} has values with expired TTL, but merges with TTL are cancelled.", global_ctx->new_data_part->name);
        ctx->need_remove_expired_values = false;
    }

    ctx->sum_input_rows_upper_bound = global_ctx->merge_list_element_ptr->total_rows_count;
    ctx->sum_compressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_compressed;

    global_ctx->chosen_merge_algorithm = chooseMergeAlgorithm();
    global_ctx->merge_list_element_ptr->merge_algorithm.store(global_ctx->chosen_merge_algorithm, std::memory_order_relaxed);

    LOG_DEBUG(ctx->log, "Selected MergeAlgorithm: {}", toString(global_ctx->chosen_merge_algorithm));

    /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
    /// (which is locked in data.getTotalActiveSizeInBytes())
    /// (which is locked in shared mode when input streams are created) and when inserting new data
    /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
    /// deadlock is impossible.
    ctx->compression_codec = global_ctx->data->getCompressionCodecForPart(
        global_ctx->merge_list_element_ptr->total_size_bytes_compressed, global_ctx->new_data_part->ttl_infos, global_ctx->time_of_merge);

    ctx->tmp_disk = std::make_unique<TemporaryDataOnDisk>(global_ctx->context->getTempDataOnDisk());

    switch (global_ctx->chosen_merge_algorithm)
    {
        case MergeAlgorithm::Horizontal:
        {
            global_ctx->merging_columns = global_ctx->storage_columns;
            global_ctx->merging_skip_indexes = global_ctx->metadata_snapshot->getSecondaryIndices();
            global_ctx->gathering_columns.clear();
            global_ctx->skip_indexes_by_column.clear();
            break;
        }
        case MergeAlgorithm::Vertical:
        {
            ctx->rows_sources_uncompressed_write_buf = ctx->tmp_disk->createRawStream();
            ctx->rows_sources_write_buf = std::make_unique<CompressedWriteBuffer>(*ctx->rows_sources_uncompressed_write_buf);

            std::map<String, UInt64> local_merged_column_to_size;
            for (const auto & part : global_ctx->future_part->parts)
                part->accumulateColumnSizes(local_merged_column_to_size);

            ctx->column_sizes = ColumnSizeEstimator(
                std::move(local_merged_column_to_size),
                global_ctx->merging_columns,
                global_ctx->gathering_columns);

            break;
        }
        default :
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge algorithm must be chosen");
    }

    /// If merge is vertical we cannot calculate it
    ctx->blocks_are_granules_size = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Vertical);

    /// Merged stream will be created and available as merged_stream variable
    createMergedStream();

    /// Skip fully expired columns manually, since in case of
    /// need_remove_expired_values is not set, TTLTransform will not be used,
    /// and columns that had been removed by TTL (via TTLColumnAlgorithm) will
    /// be added again with default values.
    ///
    /// Also note, that it is better to do this here, since in other places it
    /// will be too late (i.e. they will be written, and we will burn CPU/disk
    /// resources for this).
    if (!ctx->need_remove_expired_values)
    {
        auto part_serialization_infos = global_ctx->new_data_part->getSerializationInfos();

        NameSet columns_to_remove;
        for (auto & [column_name, ttl] : global_ctx->new_data_part->ttl_infos.columns_ttl)
        {
            if (ttl.finished())
            {
                global_ctx->new_data_part->expired_columns.insert(column_name);
                LOG_TRACE(ctx->log, "Adding expired column {} for part {}", column_name, global_ctx->new_data_part->name);
                columns_to_remove.insert(column_name);
                part_serialization_infos.erase(column_name);
            }
        }

        if (!columns_to_remove.empty())
        {
            global_ctx->gathering_columns = global_ctx->gathering_columns.eraseNames(columns_to_remove);
            global_ctx->merging_columns = global_ctx->merging_columns.eraseNames(columns_to_remove);
            global_ctx->storage_columns = global_ctx->storage_columns.eraseNames(columns_to_remove);

            global_ctx->new_data_part->setColumns(
                global_ctx->storage_columns,
                part_serialization_infos,
                global_ctx->metadata_snapshot->getMetadataVersion());
        }
    }

    global_ctx->to = std::make_shared<MergedBlockOutputStream>(
        global_ctx->new_data_part,
        global_ctx->metadata_snapshot,
        global_ctx->merging_columns,
        MergeTreeIndexFactory::instance().getMany(global_ctx->merging_skip_indexes),
        getStatisticsForColumns(global_ctx->merging_columns, global_ctx->metadata_snapshot),
        ctx->compression_codec,
        global_ctx->txn ? global_ctx->txn->tid : Tx::PrehistoricTID,
        /*reset_columns=*/ true,
        ctx->blocks_are_granules_size,
        global_ctx->context->getWriteSettings());

    global_ctx->rows_written = 0;
    ctx->initial_reservation = global_ctx->space_reservation ? global_ctx->space_reservation->getSize() : 0;

    ctx->is_cancelled = [merges_blocker = global_ctx->merges_blocker,
        ttl_merges_blocker = global_ctx->ttl_merges_blocker,
        need_remove = ctx->need_remove_expired_values,
        merge_list_element = global_ctx->merge_list_element_ptr]() -> bool
    {
        return merges_blocker->isCancelled()
            || (need_remove && ttl_merges_blocker->isCancelled())
            || merge_list_element->is_cancelled.load(std::memory_order_relaxed);
    };

    /// This is the end of preparation. Execution will be per block.
    return false;
}

bool MergeTask::enabledBlockNumberColumn(GlobalRuntimeContextPtr global_ctx)
{
    return global_ctx->data->getSettings()->enable_block_number_column && global_ctx->metadata_snapshot->getGroupByTTLs().empty();
}

bool MergeTask::enabledBlockOffsetColumn(GlobalRuntimeContextPtr global_ctx)
{
    return global_ctx->data->getSettings()->enable_block_offset_column && global_ctx->metadata_snapshot->getGroupByTTLs().empty();
}

void MergeTask::addGatheringColumn(GlobalRuntimeContextPtr global_ctx, const String & name, const DataTypePtr & type)
{
    if (global_ctx->storage_columns.contains(name))
        return;

    global_ctx->storage_columns.emplace_back(name, type);
    global_ctx->gathering_columns.emplace_back(name, type);
}


MergeTask::StageRuntimeContextPtr MergeTask::ExecuteAndFinalizeHorizontalPart::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeHorizontalStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    auto new_ctx = std::make_shared<VerticalMergeRuntimeContext>();

    new_ctx->rows_sources_write_buf = std::move(ctx->rows_sources_write_buf);
    new_ctx->rows_sources_uncompressed_write_buf = std::move(ctx->rows_sources_uncompressed_write_buf);
    new_ctx->column_sizes = std::move(ctx->column_sizes);
    new_ctx->compression_codec = std::move(ctx->compression_codec);
    new_ctx->tmp_disk = std::move(ctx->tmp_disk);
    new_ctx->it_name_and_type = std::move(ctx->it_name_and_type);
    new_ctx->read_with_direct_io = std::move(ctx->read_with_direct_io);
    new_ctx->need_sync = std::move(ctx->need_sync);

    ctx.reset();
    return new_ctx;
}

MergeTask::StageRuntimeContextPtr MergeTask::VerticalMergeStage::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeVerticalStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    auto new_ctx = std::make_shared<MergeProjectionsRuntimeContext>();
    new_ctx->need_sync = std::move(ctx->need_sync);

    ctx.reset();
    return new_ctx;
}


bool MergeTask::ExecuteAndFinalizeHorizontalPart::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}


bool MergeTask::ExecuteAndFinalizeHorizontalPart::executeImpl()
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    UInt64 step_time_ms = global_ctx->data->getSettings()->background_task_preferred_step_execution_time_ms.totalMilliseconds();

    do
    {
        Block block;

        if (ctx->is_cancelled() || !global_ctx->merging_executor->pull(block))
        {
            finalize();
            return false;
        }

        global_ctx->rows_written += block.rows();
        const_cast<MergedBlockOutputStream &>(*global_ctx->to).write(block);

        UInt64 result_rows = 0;
        UInt64 result_bytes = 0;
        global_ctx->merged_pipeline.tryGetResultRowsAndBytes(result_rows, result_bytes);
        global_ctx->merge_list_element_ptr->rows_written = result_rows;
        global_ctx->merge_list_element_ptr->bytes_written_uncompressed = result_bytes;

        /// Reservation updates is not performed yet, during the merge it may lead to higher free space requirements
        if (global_ctx->space_reservation && ctx->sum_input_rows_upper_bound)
        {
            /// The same progress from merge_entry could be used for both algorithms (it should be more accurate)
            /// But now we are using inaccurate row-based estimation in Horizontal case for backward compatibility
            Float64 progress = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Horizontal)
                ? std::min(1., 1. * global_ctx->rows_written / ctx->sum_input_rows_upper_bound)
                : std::min(1., global_ctx->merge_list_element_ptr->progress.load(std::memory_order_relaxed));

            global_ctx->space_reservation->update(static_cast<size_t>((1. - progress) * ctx->initial_reservation));
        }
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}

void MergeTask::ExecuteAndFinalizeHorizontalPart::finalize() const
{
    global_ctx->merging_executor.reset();
    global_ctx->merged_pipeline.reset();

    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

    if (ctx->need_remove_expired_values && global_ctx->ttl_merges_blocker->isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts with expired TTL");

    const size_t sum_compressed_bytes_upper_bound = global_ctx->merge_list_element_ptr->total_size_bytes_compressed;
    ctx->need_sync = needSyncPart(ctx->sum_input_rows_upper_bound, sum_compressed_bytes_upper_bound, *global_ctx->data->getSettings());
}

bool MergeTask::VerticalMergeStage::prepareVerticalMergeForAllColumns() const
{
    /// No need to execute this part if it is horizontal merge.
    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    size_t sum_input_rows_exact = global_ctx->merge_list_element_ptr->rows_read;
    size_t input_rows_filtered = *global_ctx->input_rows_filtered;
    global_ctx->merge_list_element_ptr->columns_written = global_ctx->merging_columns.size();
    global_ctx->merge_list_element_ptr->progress.store(ctx->column_sizes->keyColumnsWeight(), std::memory_order_relaxed);

    /// Ensure data has written to disk.
    ctx->rows_sources_write_buf->finalize();
    ctx->rows_sources_uncompressed_write_buf->finalize();
    ctx->rows_sources_uncompressed_write_buf->finalize();

    size_t rows_sources_count = ctx->rows_sources_write_buf->count();
    /// In special case, when there is only one source part, and no rows were skipped, we may have
    /// skipped writing rows_sources file. Otherwise rows_sources_count must be equal to the total
    /// number of input rows.
    if ((rows_sources_count > 0 || global_ctx->future_part->parts.size() > 1) && sum_input_rows_exact != rows_sources_count + input_rows_filtered)
        throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Number of rows in source parts ({}) excluding filtered rows ({}) differs from number "
                        "of bytes written to rows_sources file ({}). It is a bug.",
                        sum_input_rows_exact, input_rows_filtered, rows_sources_count);

    /// TemporaryDataOnDisk::createRawStream returns WriteBufferFromFile implementing IReadableWriteBuffer
    /// and we expect to get ReadBufferFromFile here.
    /// So, it's relatively safe to use dynamic_cast here and downcast to ReadBufferFromFile.
    auto * wbuf_readable = dynamic_cast<IReadableWriteBuffer *>(ctx->rows_sources_uncompressed_write_buf.get());
    std::unique_ptr<ReadBuffer> reread_buf = wbuf_readable ? wbuf_readable->tryGetReadBuffer() : nullptr;
    if (!reread_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read temporary file {}", ctx->rows_sources_uncompressed_write_buf->getFileName());

    auto * reread_buffer_raw = dynamic_cast<ReadBufferFromFileBase *>(reread_buf.get());
    if (!reread_buffer_raw)
    {
        const auto & reread_buf_ref = *reread_buf;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ReadBufferFromFileBase, but got {}", demangle(typeid(reread_buf_ref).name()));
    }
    /// Move ownership from std::unique_ptr<ReadBuffer> to std::unique_ptr<ReadBufferFromFile> for CompressedReadBufferFromFile.
    /// First, release ownership from unique_ptr to base type.
    reread_buf.release(); /// NOLINT(bugprone-unused-return-value,hicpp-ignored-remove-result): we already have the pointer value in `reread_buffer_raw`

    /// Then, move ownership to unique_ptr to concrete type.
    std::unique_ptr<ReadBufferFromFileBase> reread_buffer_from_file(reread_buffer_raw);

    /// CompressedReadBufferFromFile expects std::unique_ptr<ReadBufferFromFile> as argument.
    ctx->rows_sources_read_buf = std::make_unique<CompressedReadBufferFromFile>(std::move(reread_buffer_from_file));
    ctx->it_name_and_type = global_ctx->gathering_columns.cbegin();

    const auto & settings = global_ctx->context->getSettingsRef();

    size_t max_delayed_streams = 0;
    if (global_ctx->new_data_part->getDataPartStorage().supportParallelWrite())
    {
        if (settings.max_insert_delayed_streams_for_parallel_write.changed)
            max_delayed_streams = settings.max_insert_delayed_streams_for_parallel_write;
        else
            max_delayed_streams = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
    }

    ctx->max_delayed_streams = max_delayed_streams;

    bool all_parts_on_remote_disks = std::ranges::all_of(global_ctx->future_part->parts, [](const auto & part) { return part->isStoredOnRemoteDisk(); });
    ctx->use_prefetch = all_parts_on_remote_disks && global_ctx->data->getSettings()->vertical_merge_remote_filesystem_prefetch;

    if (ctx->use_prefetch && ctx->it_name_and_type != global_ctx->gathering_columns.end())
        ctx->prepared_pipe = createPipeForReadingOneColumn(ctx->it_name_and_type->name);

    return false;
}

Pipe MergeTask::VerticalMergeStage::createPipeForReadingOneColumn(const String & column_name) const
{
    Pipes pipes;
    for (size_t part_num = 0; part_num < global_ctx->future_part->parts.size(); ++part_num)
    {
        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge,
            *global_ctx->data,
            global_ctx->storage_snapshot,
            global_ctx->future_part->parts[part_num],
            Names{column_name},
            /*mark_ranges=*/ {},
            global_ctx->input_rows_filtered,
            /*apply_deleted_mask=*/ true,
            ctx->read_with_direct_io,
            ctx->use_prefetch);

        pipes.emplace_back(std::move(pipe));
    }

    return Pipe::unitePipes(std::move(pipes));
}

void MergeTask::VerticalMergeStage::prepareVerticalMergeForOneColumn() const
{
    const auto & column_name = ctx->it_name_and_type->name;

    ctx->progress_before = global_ctx->merge_list_element_ptr->progress.load(std::memory_order_relaxed);
    global_ctx->column_progress = std::make_unique<MergeStageProgress>(ctx->progress_before, ctx->column_sizes->columnWeight(column_name));

    Pipe pipe;
    if (ctx->prepared_pipe)
    {
        pipe = std::move(*ctx->prepared_pipe);

        auto next_column_it = std::next(ctx->it_name_and_type);
        if (next_column_it != global_ctx->gathering_columns.end())
            ctx->prepared_pipe = createPipeForReadingOneColumn(next_column_it->name);
    }
    else
    {
        pipe = createPipeForReadingOneColumn(column_name);
    }

    ctx->rows_sources_read_buf->seek(0, 0);
    bool is_result_sparse = global_ctx->new_data_part->getSerialization(column_name)->getKind() == ISerialization::Kind::SPARSE;

    const auto data_settings = global_ctx->data->getSettings();
    auto transform = std::make_unique<ColumnGathererTransform>(
        pipe.getHeader(),
        pipe.numOutputPorts(),
        *ctx->rows_sources_read_buf,
        data_settings->merge_max_block_size,
        data_settings->merge_max_block_size_bytes,
        is_result_sparse);

    pipe.addTransform(std::move(transform));

    MergeTreeIndices indexes_to_recalc;
    auto indexes_it = global_ctx->skip_indexes_by_column.find(column_name);

    if (indexes_it != global_ctx->skip_indexes_by_column.end())
    {
        indexes_to_recalc = MergeTreeIndexFactory::instance().getMany(indexes_it->second);

        pipe.addTransform(std::make_shared<ExpressionTransform>(
            pipe.getHeader(),
            indexes_it->second.getSingleExpressionForIndices(global_ctx->metadata_snapshot->getColumns(),
            global_ctx->data->getContext())));

        pipe.addTransform(std::make_shared<MaterializingTransform>(pipe.getHeader()));
    }

    ctx->column_parts_pipeline = QueryPipeline(std::move(pipe));

    /// Dereference unique_ptr
    ctx->column_parts_pipeline.setProgressCallback(MergeProgressCallback(
        global_ctx->merge_list_element_ptr,
        global_ctx->watch_prev_elapsed,
        *global_ctx->column_progress));

    /// Is calculated inside MergeProgressCallback.
    ctx->column_parts_pipeline.disableProfileEventUpdate();
    ctx->executor = std::make_unique<PullingPipelineExecutor>(ctx->column_parts_pipeline);
    NamesAndTypesList columns_list = {*ctx->it_name_and_type};

    ctx->column_to = std::make_unique<MergedColumnOnlyOutputStream>(
        global_ctx->new_data_part,
        global_ctx->metadata_snapshot,
        columns_list,
        ctx->compression_codec,
        indexes_to_recalc,
        getStatisticsForColumns(columns_list, global_ctx->metadata_snapshot),
        &global_ctx->written_offset_columns,
        global_ctx->to->getIndexGranularity());

    ctx->column_elems_written = 0;
}


bool MergeTask::VerticalMergeStage::executeVerticalMergeForOneColumn() const
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    UInt64 step_time_ms = global_ctx->data->getSettings()->background_task_preferred_step_execution_time_ms.totalMilliseconds();

    do
    {
        Block block;

        if (global_ctx->merges_blocker->isCancelled()
            || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed)
            || !ctx->executor->pull(block))
            return false;

        ctx->column_elems_written += block.rows();
        ctx->column_to->write(block);
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}


void MergeTask::VerticalMergeStage::finalizeVerticalMergeForOneColumn() const
{
    const String & column_name = ctx->it_name_and_type->name;
    if (global_ctx->merges_blocker->isCancelled() || global_ctx->merge_list_element_ptr->is_cancelled.load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

    ctx->executor.reset();
    auto changed_checksums = ctx->column_to->fillChecksums(global_ctx->new_data_part, global_ctx->checksums_gathered_columns);
    global_ctx->checksums_gathered_columns.add(std::move(changed_checksums));
    const auto & columns_sample = ctx->column_to->getColumnsSample().getColumnsWithTypeAndName();
    global_ctx->gathered_columns_samples.insert(global_ctx->gathered_columns_samples.end(), columns_sample.begin(), columns_sample.end());

    ctx->delayed_streams.emplace_back(std::move(ctx->column_to));

    while (ctx->delayed_streams.size() > ctx->max_delayed_streams)
    {
        ctx->delayed_streams.front()->finish(ctx->need_sync);
        ctx->delayed_streams.pop_front();
    }

    if (global_ctx->rows_written != ctx->column_elems_written)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Written {} elements of column {}, but {} rows of PK columns",
                        toString(ctx->column_elems_written), column_name, toString(global_ctx->rows_written));
    }

    UInt64 rows = 0;
    UInt64 bytes = 0;
    ctx->column_parts_pipeline.tryGetResultRowsAndBytes(rows, bytes);

    /// NOTE: 'progress' is modified by single thread, but it may be concurrently read from MergeListElement::getInfo() (StorageSystemMerges).

    global_ctx->merge_list_element_ptr->columns_written += 1;
    global_ctx->merge_list_element_ptr->bytes_written_uncompressed += bytes;
    global_ctx->merge_list_element_ptr->progress.store(ctx->progress_before + ctx->column_sizes->columnWeight(column_name), std::memory_order_relaxed);

    /// This is the external loop increment.
    ++ctx->it_name_and_type;
}


bool MergeTask::VerticalMergeStage::finalizeVerticalMergeForAllColumns() const
{
    for (auto & stream : ctx->delayed_streams)
        stream->finish(ctx->need_sync);

    return false;
}


bool MergeTask::MergeProjectionsStage::mergeMinMaxIndexAndPrepareProjections() const
{
    for (const auto & part : global_ctx->future_part->parts)
    {
        /// Skip empty parts,
        /// (that can be created in StorageReplicatedMergeTree::createEmptyPartInsteadOfLost())
        /// since they can incorrectly set min,
        /// that will be changed after one more merge/OPTIMIZE.
        if (!part->isEmpty())
            global_ctx->new_data_part->minmax_idx->merge(*part->minmax_idx);
    }

    /// Print overall profiling info. NOTE: it may duplicates previous messages
    {
        ProfileEvents::increment(ProfileEvents::MergedColumns, global_ctx->merging_columns.size());
        ProfileEvents::increment(ProfileEvents::GatheredColumns, global_ctx->gathering_columns.size());

        double elapsed_seconds = global_ctx->merge_list_element_ptr->watch.elapsedSeconds();
        LOG_DEBUG(ctx->log,
            "Merge sorted {} rows, containing {} columns ({} merged, {} gathered) in {} sec., {} rows/sec., {}/sec.",
            global_ctx->merge_list_element_ptr->rows_read,
            global_ctx->storage_columns.size(),
            global_ctx->merging_columns.size(),
            global_ctx->gathering_columns.size(),
            elapsed_seconds,
            global_ctx->merge_list_element_ptr->rows_read / elapsed_seconds,
            ReadableSize(global_ctx->merge_list_element_ptr->bytes_read_uncompressed / elapsed_seconds));
    }


    const auto mode = global_ctx->data->getSettings()->deduplicate_merge_projection_mode;
    /// Under throw mode, we still choose to drop projections due to backward compatibility since some
    /// users might have projections before this change.
    if (global_ctx->data->merging_params.mode != MergeTreeData::MergingParams::Ordinary
        && (mode == DeduplicateMergeProjectionMode::THROW || mode == DeduplicateMergeProjectionMode::DROP))
    {
        ctx->projections_iterator = ctx->tasks_for_projections.begin();
        return false;
    }

    const auto & projections = global_ctx->metadata_snapshot->getProjections();

    for (const auto & projection : projections)
    {
        MergeTreeData::DataPartsVector projection_parts;
        for (const auto & part : global_ctx->future_part->parts)
        {
            auto actual_projection_parts = part->getProjectionParts();
            auto it = actual_projection_parts.find(projection.name);
            if (it != actual_projection_parts.end() && !it->second->is_broken)
                projection_parts.push_back(it->second);
        }
        if (projection_parts.size() < global_ctx->future_part->parts.size())
        {
            LOG_DEBUG(ctx->log, "Projection {} is not merged because some parts don't have it", projection.name);
            continue;
        }

        LOG_DEBUG(
            ctx->log,
            "Selected {} projection_parts from {} to {}",
            projection_parts.size(),
            projection_parts.front()->name,
            projection_parts.back()->name);

        auto projection_future_part = std::make_shared<FutureMergedMutatedPart>();
        projection_future_part->assign(std::move(projection_parts));
        projection_future_part->name = projection.name;
        // TODO (ab): path in future_part is only for merge process introspection, which is not available for merges of projection parts.
        // Let's comment this out to avoid code inconsistency and add it back after we implement projection merge introspection.
        // projection_future_part->path = global_ctx->future_part->path + "/" + projection.name + ".proj/";
        projection_future_part->part_info = {"all", 0, 0, 0};

        MergeTreeData::MergingParams projection_merging_params;
        projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
        if (projection.type == ProjectionDescription::Type::Aggregate)
            projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

        ctx->tasks_for_projections.emplace_back(std::make_shared<MergeTask>(
            projection_future_part,
            projection.metadata,
            global_ctx->merge_entry,
            std::make_unique<MergeListElement>((*global_ctx->merge_entry)->table_id, projection_future_part, global_ctx->context),
            global_ctx->time_of_merge,
            global_ctx->context,
            global_ctx->space_reservation,
            global_ctx->deduplicate,
            global_ctx->deduplicate_by_columns,
            global_ctx->cleanup,
            projection_merging_params,
            global_ctx->need_prefix,
            global_ctx->new_data_part.get(),
            ".proj",
            NO_TRANSACTION_PTR,
            global_ctx->data,
            global_ctx->mutator,
            global_ctx->merges_blocker,
            global_ctx->ttl_merges_blocker));
    }

    /// We will iterate through projections and execute them
    ctx->projections_iterator = ctx->tasks_for_projections.begin();

    return false;
}


bool MergeTask::MergeProjectionsStage::executeProjections() const
{
    if (ctx->projections_iterator == ctx->tasks_for_projections.end())
        return false;

    if ((*ctx->projections_iterator)->execute())
        return true;

    ++ctx->projections_iterator;
    return true;
}


bool MergeTask::MergeProjectionsStage::finalizeProjectionsAndWholeMerge() const
{
    for (const auto & task : ctx->tasks_for_projections)
    {
        auto part = task->getFuture().get();
        global_ctx->new_data_part->addProjectionPart(part->name, std::move(part));
    }

    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        global_ctx->to->finalizePart(global_ctx->new_data_part, ctx->need_sync);
    else
        global_ctx->to->finalizePart(global_ctx->new_data_part, ctx->need_sync, &global_ctx->storage_columns, &global_ctx->checksums_gathered_columns, &global_ctx->gathered_columns_samples);

    global_ctx->new_data_part->getDataPartStorage().precommitTransaction();
    global_ctx->promise.set_value(global_ctx->new_data_part);

    return false;
}

MergeTask::StageRuntimeContextPtr MergeTask::MergeProjectionsStage::getContextForNextStage()
{
    /// Do not increment for projection stage because time is already accounted in main task.
    /// The projection stage has its own empty projection stage which may add a drift of several milliseconds.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(ProfileEvents::MergeExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
        ProfileEvents::increment(ProfileEvents::MergeProjectionStageExecuteMilliseconds, ctx->elapsed_execute_ns / 1000000UL);
    }

    return nullptr;
}

bool MergeTask::VerticalMergeStage::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}

bool MergeTask::MergeProjectionsStage::execute()
{
    chassert(subtasks_iterator != subtasks.end());

    Stopwatch watch;
    bool res = (this->**subtasks_iterator)();
    ctx->elapsed_execute_ns += watch.elapsedNanoseconds();

    if (res)
        return res;

    /// Move to the next subtask in an array of subtasks
    ++subtasks_iterator;
    return subtasks_iterator != subtasks.end();
}


bool MergeTask::VerticalMergeStage::executeVerticalMergeForAllColumns() const
{
    /// No need to execute this part if it is horizontal merge.
    if (global_ctx->chosen_merge_algorithm != MergeAlgorithm::Vertical)
        return false;

    /// This is the external cycle condition
    if (ctx->it_name_and_type == global_ctx->gathering_columns.end())
        return false;

    switch (ctx->vertical_merge_one_column_state)
    {
        case VerticalMergeRuntimeContext::State::NEED_PREPARE:
        {
            prepareVerticalMergeForOneColumn();
            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_EXECUTE;
            return true;
        }
        case VerticalMergeRuntimeContext::State::NEED_EXECUTE:
        {
            if (executeVerticalMergeForOneColumn())
                return true;

            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_FINISH;
            return true;
        }
        case VerticalMergeRuntimeContext::State::NEED_FINISH:
        {
            finalizeVerticalMergeForOneColumn();
            ctx->vertical_merge_one_column_state = VerticalMergeRuntimeContext::State::NEED_PREPARE;
            return true;
        }
    }
    return false;
}


bool MergeTask::execute()
{
    chassert(stages_iterator != stages.end());
    const auto & current_stage = *stages_iterator;

    if (current_stage->execute())
        return true;

    /// Stage is finished, need to initialize context for the next stage and update profile events.

    UInt64 current_elapsed_ms = global_ctx->merge_list_element_ptr->watch.elapsedMilliseconds();
    UInt64 stage_elapsed_ms = current_elapsed_ms - global_ctx->prev_elapsed_ms;
    global_ctx->prev_elapsed_ms = current_elapsed_ms;

    auto next_stage_context = current_stage->getContextForNextStage();

    /// Do not increment for projection stage because time is already accounted in main task.
    if (global_ctx->parent_part == nullptr)
    {
        ProfileEvents::increment(current_stage->getTotalTimeProfileEvent(), stage_elapsed_ms);
        ProfileEvents::increment(ProfileEvents::MergeTotalMilliseconds, stage_elapsed_ms);
    }

    /// Move to the next stage in an array of stages
    ++stages_iterator;
    if (stages_iterator == stages.end())
        return false;

    (*stages_iterator)->setRuntimeContext(std::move(next_stage_context), global_ctx);
    return true;
}


void MergeTask::ExecuteAndFinalizeHorizontalPart::createMergedStream()
{
    /** Read from all parts, merge and write into a new one.
      * In passing, we calculate expression for sorting.
      */
    Pipes pipes;
    global_ctx->watch_prev_elapsed = 0;

    /// We count total amount of bytes in parts
    /// and use direct_io + aio if there is more than min_merge_bytes_to_use_direct_io
    ctx->read_with_direct_io = false;
    const auto data_settings = global_ctx->data->getSettings();
    if (data_settings->min_merge_bytes_to_use_direct_io != 0)
    {
        size_t total_size = 0;
        for (const auto & part : global_ctx->future_part->parts)
        {
            total_size += part->getBytesOnDisk();
            if (total_size >= data_settings->min_merge_bytes_to_use_direct_io)
            {
                LOG_DEBUG(ctx->log, "Will merge parts reading files in O_DIRECT");
                ctx->read_with_direct_io = true;

                break;
            }
        }
    }

    /// Using unique_ptr, because MergeStageProgress has no default constructor
    global_ctx->horizontal_stage_progress = std::make_unique<MergeStageProgress>(
        ctx->column_sizes ? ctx->column_sizes->keyColumnsWeight() : 1.0);

    for (const auto & part : global_ctx->future_part->parts)
    {
        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge,
            *global_ctx->data,
            global_ctx->storage_snapshot,
            part,
            global_ctx->merging_columns.getNames(),
            /*mark_ranges=*/ {},
            global_ctx->input_rows_filtered,
            /*apply_deleted_mask=*/ true,
            ctx->read_with_direct_io,
            /*prefetch=*/ false);

        if (global_ctx->metadata_snapshot->hasSortingKey())
        {
            pipe.addSimpleTransform([this](const Block & header)
            {
                return std::make_shared<ExpressionTransform>(header, global_ctx->metadata_snapshot->getSortingKey().expression);
            });
        }

        pipes.emplace_back(std::move(pipe));
    }


    Names sort_columns = global_ctx->metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    sort_description.compile_sort_description = global_ctx->data->getContext()->getSettingsRef().compile_sort_description;
    sort_description.min_count_to_compile_sort_description = global_ctx->data->getContext()->getSettingsRef().min_count_to_compile_sort_description;

    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    Names partition_key_columns = global_ctx->metadata_snapshot->getPartitionKey().column_names;

    Block header = pipes.at(0).getHeader();
    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(sort_columns[i], 1, 1);

#ifndef NDEBUG
    if (!sort_description.empty())
    {
        for (size_t i = 0; i < pipes.size(); ++i)
        {
            auto & pipe = pipes[i];
            pipe.addSimpleTransform([&](const Block & header_)
            {
                auto transform = std::make_shared<CheckSortedTransform>(header_, sort_description);
                transform->setDescription(global_ctx->future_part->parts[i]->name);
                return transform;
            });
        }
    }
#endif

    /// The order of the streams is important: when the key is matched, the elements go in the order of the source stream number.
    /// In the merged part, the lines with the same key must be in the ascending order of the identifier of original part,
    ///  that is going in insertion order.
    ProcessorPtr merged_transform;

    /// If merge is vertical we cannot calculate it
    ctx->blocks_are_granules_size = (global_ctx->chosen_merge_algorithm == MergeAlgorithm::Vertical);

    /// There is no sense to have the block size bigger than one granule for merge operations.
    const UInt64 merge_block_size_rows = data_settings->merge_max_block_size;
    const UInt64 merge_block_size_bytes = data_settings->merge_max_block_size_bytes;

    switch (ctx->merging_params.mode)
    {
        case MergeTreeData::MergingParams::Ordinary:
            merged_transform = std::make_shared<MergingSortedTransform>(
                header,
                pipes.size(),
                sort_description,
                merge_block_size_rows,
                merge_block_size_bytes,
                SortingQueueStrategy::Default,
                /* limit_= */0,
                /* always_read_till_end_= */false,
                ctx->rows_sources_write_buf.get(),
                ctx->blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Collapsing:
            merged_transform = std::make_shared<CollapsingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.sign_column, false,
                merge_block_size_rows, merge_block_size_bytes, ctx->rows_sources_write_buf.get(), ctx->blocks_are_granules_size);
            break;

        case MergeTreeData::MergingParams::Summing:
            merged_transform = std::make_shared<SummingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.columns_to_sum, partition_key_columns, merge_block_size_rows, merge_block_size_bytes);
            break;

        case MergeTreeData::MergingParams::Aggregating:
            merged_transform = std::make_shared<AggregatingSortedTransform>(header, pipes.size(), sort_description, merge_block_size_rows, merge_block_size_bytes);
            break;

        case MergeTreeData::MergingParams::Replacing:
            if (global_ctx->cleanup && !data_settings->allow_experimental_replacing_merge_with_cleanup)
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Experimental merges with CLEANUP are not allowed");

            merged_transform = std::make_shared<ReplacingSortedTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.is_deleted_column, ctx->merging_params.version_column,
                merge_block_size_rows, merge_block_size_bytes, ctx->rows_sources_write_buf.get(), ctx->blocks_are_granules_size,
                global_ctx->cleanup);
            break;

        case MergeTreeData::MergingParams::Graphite:
            merged_transform = std::make_shared<GraphiteRollupSortedTransform>(
                header, pipes.size(), sort_description, merge_block_size_rows, merge_block_size_bytes,
                ctx->merging_params.graphite_params, global_ctx->time_of_merge);
            break;

        case MergeTreeData::MergingParams::VersionedCollapsing:
            merged_transform = std::make_shared<VersionedCollapsingTransform>(
                header, pipes.size(), sort_description, ctx->merging_params.sign_column,
                merge_block_size_rows, merge_block_size_bytes, ctx->rows_sources_write_buf.get(), ctx->blocks_are_granules_size);
            break;
    }

    auto builder = std::make_unique<QueryPipelineBuilder>();
    builder->init(Pipe::unitePipes(std::move(pipes)));
    builder->addTransform(std::move(merged_transform));

#ifndef NDEBUG
    if (!sort_description.empty())
    {
        builder->addSimpleTransform([&](const Block & header_)
        {
            auto transform = std::make_shared<CheckSortedTransform>(header_, sort_description);
            return transform;
        });
    }
#endif

    if (global_ctx->deduplicate)
    {
        const auto & virtuals = *global_ctx->data->getVirtualsPtr();

        /// We don't want to deduplicate by virtual persistent column.
        /// If deduplicate_by_columns is empty, add all columns except virtuals.
        if (global_ctx->deduplicate_by_columns.empty())
        {
            for (const auto & column : global_ctx->merging_columns)
            {
                if (virtuals.tryGet(column.name, VirtualsKind::Persistent))
                    continue;

                global_ctx->deduplicate_by_columns.emplace_back(column.name);
            }
        }

        if (DistinctSortedTransform::isApplicable(header, sort_description, global_ctx->deduplicate_by_columns))
            builder->addTransform(std::make_shared<DistinctSortedTransform>(
                builder->getHeader(), sort_description, SizeLimits(), 0 /*limit_hint*/, global_ctx->deduplicate_by_columns));
        else
            builder->addTransform(std::make_shared<DistinctTransform>(
                builder->getHeader(), SizeLimits(), 0 /*limit_hint*/, global_ctx->deduplicate_by_columns));
    }

    PreparedSets::Subqueries subqueries;

    if (ctx->need_remove_expired_values)
    {
        auto transform = std::make_shared<TTLTransform>(global_ctx->context, builder->getHeader(), *global_ctx->data, global_ctx->metadata_snapshot, global_ctx->new_data_part, global_ctx->time_of_merge, ctx->force_ttl);
        subqueries = transform->getSubqueries();
        builder->addTransform(std::move(transform));
    }

    if (!global_ctx->merging_skip_indexes.empty())
    {
        builder->addTransform(std::make_shared<ExpressionTransform>(
            builder->getHeader(),
            global_ctx->merging_skip_indexes.getSingleExpressionForIndices(global_ctx->metadata_snapshot->getColumns(),
            global_ctx->data->getContext())));

        builder->addTransform(std::make_shared<MaterializingTransform>(builder->getHeader()));
    }

    if (!subqueries.empty())
        builder = addCreatingSetsTransform(std::move(builder), std::move(subqueries), global_ctx->context);

    global_ctx->merged_pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    /// Dereference unique_ptr and pass horizontal_stage_progress by reference
    global_ctx->merged_pipeline.setProgressCallback(MergeProgressCallback(global_ctx->merge_list_element_ptr, global_ctx->watch_prev_elapsed, *global_ctx->horizontal_stage_progress));
    /// Is calculated inside MergeProgressCallback.
    global_ctx->merged_pipeline.disableProfileEventUpdate();

    global_ctx->merging_executor = std::make_unique<PullingPipelineExecutor>(global_ctx->merged_pipeline);
}


MergeAlgorithm MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm() const
{
    const size_t total_rows_count = global_ctx->merge_list_element_ptr->total_rows_count;
    const size_t total_size_bytes_uncompressed = global_ctx->merge_list_element_ptr->total_size_bytes_uncompressed;
    const auto data_settings = global_ctx->data->getSettings();

    if (global_ctx->deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data_settings->enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;
    if (ctx->need_remove_expired_values)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->future_part->part_format.part_type != MergeTreeDataPartType::Wide)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->future_part->part_format.storage_type != MergeTreeDataPartStorageType::Full)
        return MergeAlgorithm::Horizontal;
    if (global_ctx->cleanup)
        return MergeAlgorithm::Horizontal;

    if (!data_settings->allow_vertical_merges_from_compact_to_wide_parts)
    {
        for (const auto & part : global_ctx->future_part->parts)
        {
            if (!isWidePart(part))
                return MergeAlgorithm::Horizontal;
        }
    }

    bool is_supported_storage =
        ctx->merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        ctx->merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = global_ctx->gathering_columns.size() >= data_settings->vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = total_rows_count >= data_settings->vertical_merge_algorithm_min_rows_to_activate;

    bool enough_total_bytes = total_size_bytes_uncompressed >= data_settings->vertical_merge_algorithm_min_bytes_to_activate;

    bool no_parts_overflow = global_ctx->future_part->parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_total_bytes && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    return merge_alg;
}

}
