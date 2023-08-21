#include "parallel_reader.h"

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/util/batch.h>

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

std::pair<IClientBasePtr, TVector<TRichYPath>> CreateRangeReaderClientAndPaths(
    const IClientBasePtr& client,
    const TVector<TRichYPath>& paths,
    bool createTransaction)
{
    auto lockPaths = [&] (const IClientBasePtr& client, TVector<TRichYPath> paths) {
        auto locks = BatchTransform(client, paths, [] (const TBatchRequestPtr& batch, const TRichYPath& path) {
            return batch->Lock(path.Path_, ELockMode::LM_SNAPSHOT);
        });
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
    TVector<TRichYPath> pathsForColumnStatistics;

    auto dataWeights = BatchTransform(client, paths, [&] (const TBatchRequestPtr& batch, const TRichYPath& path) {
        if (path.Columns_) {
            auto pathWithoutRanges = path;
            pathWithoutRanges.ResetRanges();
            pathsForColumnStatistics.push_back(pathWithoutRanges);
            return NThreading::MakeFuture(TNode(0));
        } else {
            return batch->Get(path.Path_ + "/@data_weight");
        }
    });

    if (!pathsForColumnStatistics.empty()) {
        auto columnarStatistics = client->GetTableColumnarStatistics(pathsForColumnStatistics);
        auto statisticsIt = columnarStatistics.cbegin();
        for (int i = 0; i < static_cast<int>(paths.size()); ++i) {
            const auto& path = paths[i];
            if (!path.Columns_) {
                continue;
            }
            Y_VERIFY(statisticsIt != columnarStatistics.cend());
            i64 dataWeight = 0;
            for (const auto& [columnName, columnWeight] : statisticsIt->ColumnDataWeight) {
                dataWeight += columnWeight;
            }
            dataWeights[i] = dataWeight;
            ++statisticsIt;
        }
    }

    auto rowCounts = BatchTransform(client, paths, [&] (const TBatchRequestPtr& batch, const TRichYPath& path) {
        return batch->Get(path.Path_ + "/@row_count");
    });

    i64 rowWeight = 1;
    for (int i = 0; i < static_cast<int>(paths.size()); ++i) {
        auto dataWeight = dataWeights[i].AsInt64();
        auto rowCount = Max(rowCounts[i].AsInt64(), static_cast<i64>(1));
        rowWeight = Max(rowWeight, dataWeight / rowCount);
    }
    return rowWeight;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
