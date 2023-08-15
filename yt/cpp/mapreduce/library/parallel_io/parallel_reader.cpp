#include "parallel_reader.h"

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/util/batch.h>
#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

i64 GetRowCount(const TRichYPath& path)
{
    Y_ENSURE(path.GetRanges());
    i64 rowCount = 0;
    for (const auto& range : *path.GetRanges()) {
        Y_ENSURE(range.LowerLimit_.RowIndex_.Defined(), "Lower limit must be specified as row index");
        Y_ENSURE(range.UpperLimit_.RowIndex_.Defined(), "Upper limit must be specified as row index");
        rowCount += *range.UpperLimit_.RowIndex_ - *range.LowerLimit_.RowIndex_;
    }
    return rowCount;
}

TTableSlicer::TTableSlicer(TRichYPath path, i64 batchSize)
    : Path_(std::move(path))
    , BatchSize_(batchSize)
{ }

void TTableSlicer::Next()
{
    Offset_ += BatchSize_;
    if (GetLowerLimit() + Offset_ >= GetUpperLimit()) {
        ++RangeIndex_;
        Offset_ = 0;
    }
}

bool TTableSlicer::IsValid() const
{
    return Path_.GetRanges().Defined() && RangeIndex_ < static_cast<i64>(Path_.GetRanges()->size());
}

TReadRange TTableSlicer::GetRange() const
{
    Y_VERIFY(IsValid());
    auto begin = GetLowerLimit() + Offset_;
    auto end = ::Min(GetUpperLimit(), begin + BatchSize_);
    return TReadRange::FromRowIndices(begin, end);
}

i64 TTableSlicer::GetLowerLimit() const
{
    return *Path_.GetRange(RangeIndex_).LowerLimit_.RowIndex_;
}

i64 TTableSlicer::GetUpperLimit() const
{
    return *Path_.GetRange(RangeIndex_).UpperLimit_.RowIndex_;
}

////////////////////////////////////////////////////////////////////////////////


template <typename TResult>
TVector<TResult> CollectUniqueRawPathVectorCallResultsForRichPathVector(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    const std::function<TVector<TResult>(const IClientBasePtr&, const TVector<TYPath>&)> &fn)
{
    THashSet<TYPath> uniqueRawPathsSet;
    for (const auto& path : paths) {
        uniqueRawPathsSet.emplace(path.Path_);
    }
    TVector<TYPath> rawPaths(uniqueRawPathsSet.begin(), uniqueRawPathsSet.end());

    auto valuesList = fn(client, rawPaths);
    Y_VERIFY(valuesList.size() == rawPaths.size());
    THashMap<TYPath, TResult> rawPathToResult;
    rawPathToResult.reserve(rawPaths.size());
    for (int i = 0; i < static_cast<int>(rawPaths.size()); i++) {
        rawPathToResult.emplace(rawPaths[i], std::move(valuesList[i]));
    }

    TVector<TResult> result;
    result.reserve(paths.size());

    for (const auto& path : paths) {
        result.emplace_back(rawPathToResult.at(path.Path_));
    }
    return result;
}


std::pair<IClientBasePtr, TVector<TRichYPath>> CreateRangeReaderClientAndPaths(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    bool createTransaction)
{
    auto lockPaths = [&] (const IClientBasePtr& client, TVector<TRichYPath> paths) {
        auto locks = CollectUniqueRawPathVectorCallResultsForRichPathVector<ILockPtr>(
            client, paths, [] ( const auto& client, const auto& rawPaths) {
                return BatchTransform(client, rawPaths, [] (const TBatchRequestPtr& batch, const TYPath& path) {
                    return batch->Lock(path, ELockMode::LM_SNAPSHOT);
                });
            }
        );

        TVector<TRichYPath> result = std::move(paths);
        for (int i = 0; i < static_cast<int>(paths.size()); ++i) {
            result[i].Path("#" + GetGuidAsString(locks[i]->GetLockedNodeId()));
        }
        return result;
    };

    auto getMissingRanges = [&] (const IClientBasePtr& client, TVector<TRichYPath> paths) {
        auto rowCounts = BatchTransform(client, paths, [] (const TBatchRequestPtr& batch, const TRichYPath& path) {
            if (path.GetRanges().Empty()) {
                return batch->Get(path.Path_ + "/@row_count");
            }
            for (const auto& range : *path.GetRanges()) {
                Y_ENSURE(range.LowerLimit_.RowIndex_.Defined(), "Lower limit must be specified as row index");
                Y_ENSURE(range.UpperLimit_.RowIndex_.Defined(), "Upper limit must be specified as row index");
            }
            return NThreading::MakeFuture(TNode());
        });
        for (int i = 0; i < static_cast<int>(paths.size()); ++i) {
            if (paths[i].GetRanges().Empty()) {
                paths[i].AddRange(TReadRange::FromRowIndices(0, rowCounts[i].AsInt64()));
            }
        }
        return paths;
    };

    auto rangeReaderPaths = BatchTransform(client, paths, std::mem_fn(&IBatchRequest::CanonizeYPath));

    IClientBasePtr rangeReaderClient;
    if (createTransaction) {
        rangeReaderClient = client->StartTransaction();
        rangeReaderPaths = lockPaths(rangeReaderClient, std::move(rangeReaderPaths));
    } else {
        rangeReaderClient = client;
    }

    rangeReaderPaths = getMissingRanges(rangeReaderClient, std::move(rangeReaderPaths));

    return {std::move(rangeReaderClient), std::move(rangeReaderPaths)};
}


i64 EstimateTableRowWeight(const IClientBasePtr& client, const TVector<TRichYPath>& paths)
{
    TVector<i64> dataWeights;
    dataWeights.reserve(paths.size());
    TVector<i64> rowCounts;
    rowCounts.reserve(paths.size());

    /// ------------------- @row_count and @data_weight --------------------------
    {
        auto attributes = CollectUniqueRawPathVectorCallResultsForRichPathVector<TNode>(
            client, paths, [] ( const auto& client, const auto& rawPaths) {
                return BatchTransform(client, rawPaths, [] (const TBatchRequestPtr& batch, const TYPath& path) {
                    return batch->Get(path + "/@", TGetOptions().AttributeFilter(TAttributeFilter().AddAttribute("data_weight").AddAttribute("row_count")));
                });
            }
        );

        for (const auto& node : attributes) {
            dataWeights.emplace_back(node.ChildConvertTo<i64>("data_weight"));
            rowCounts.emplace_back(node.ChildConvertTo<i64>("row_count"));
        }
    }

    /// ------------------- Columnar statistics --------------------------
    {
        THashMap<TYPath, THashSet<TString>> tableToColumnsSet;
        for (const auto& path : paths) {
            if (path.Columns_) {
                for (const auto& column : path.Columns_->Parts_) {
                    tableToColumnsSet[path.Path_].emplace(column);
                }
            }
        }

        TVector<TRichYPath> pathsForColumnStatistics;
        for (const auto& [path, columnsSet] : tableToColumnsSet) {
            TRichYPath richPath(path);
            richPath.Columns(TVector<TString>(columnsSet.begin(), columnsSet.end()));
            pathsForColumnStatistics.emplace_back(richPath);
        }

        if (!pathsForColumnStatistics.empty()) {
            THashMap<TString, TTableColumnarStatistics> columnarStatistics;
            auto columnarStatsResponse = client->GetTableColumnarStatistics(pathsForColumnStatistics);
            for (int i = 0; i < static_cast<int>(columnarStatsResponse.size()); i++) {
                columnarStatistics.emplace(pathsForColumnStatistics[i].Path_, columnarStatsResponse[i]);
            }

            for (int i = 0; i < static_cast<int>(paths.size()); ++i) {
                const auto& path = paths[i];
                if (!path.Columns_) {
                    continue;
                }
                const auto& colStats = columnarStatistics.at(path.Path_);
                i64 dataWeight = 0;
                for (const auto& column : path.Columns_->Parts_) {
                    dataWeight += colStats.ColumnDataWeight.at(column);
                }
                dataWeights[i] = dataWeight;
            }
        }
    }

    // --------------------- Row weight estimation ---------------------
    i64 rowWeight = 1;
    for (int i = 0; i < static_cast<int>(paths.size()); ++i) {
        auto dataWeight = dataWeights[i];
        auto rowCount = Max(rowCounts[i], static_cast<i64>(1));
        rowWeight = Max(rowWeight, dataWeight / rowCount);
    }
    return rowWeight;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
