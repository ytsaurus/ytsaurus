#pragma once
#include <contrib/ydb/library/accessor/accessor.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>

namespace NKikimr::NArrow::NSplitter {

class TSimpleSerializationStat {
public:
    enum class EOrigination : ui32 {
        Statistic = 0,
        Loader,
        Mix
    };

protected:
    ui64 SerializedBytes = 0;
    ui64 RecordsCount = 0;
    ui64 RawBytes = 0;
    YDB_ACCESSOR(EOrigination, Origination, EOrigination::Statistic);

public:
    TSimpleSerializationStat() = default;
    TSimpleSerializationStat(const ui64 bytes, const ui64 recordsCount, const ui64 rawBytes);

    std::vector<i64> SplitRecords(
        const ui32 recordsCount, const ui32 expectedRecordsCount, const ui32 expectedColumnPageSize, const ui32 maxBlobSize);

    TString DebugString() const {
        return TStringBuilder() << "{"
                                << "serialized_bytes=" << SerializedBytes << ";"
                                << "records=" << RecordsCount << ";"
                                << "raw_bytes=" << RawBytes << ";"
                                << "origination=" << ::ToString(Origination) << ";"
                                << "}";
    };

    double GetSerializedBytesPerRecord() const {
        AFL_VERIFY(RecordsCount);
        return 1.0 * SerializedBytes / RecordsCount;
    }
    double GetRawBytesPerRecord() const {
        AFL_VERIFY(RecordsCount);
        return 1.0 * RawBytes / RecordsCount;
    }

    ui64 GetSerializedBytes() const {
        return SerializedBytes;
    }
    ui64 GetRecordsCount() const {
        return RecordsCount;
    }
    ui64 GetRawBytes() const {
        return RawBytes;
    }

    void AddStat(const TSimpleSerializationStat& stat) {
        if (stat.GetOrigination() != GetOrigination()) {
            Origination = EOrigination::Mix;
        }
        SerializedBytes += stat.SerializedBytes;
        RecordsCount += stat.RecordsCount;
        RawBytes += stat.RawBytes;
    }

    void RemoveStat(const TSimpleSerializationStat& stat) {
        Y_ABORT_UNLESS(SerializedBytes >= stat.SerializedBytes);
        SerializedBytes -= stat.SerializedBytes;
        Y_ABORT_UNLESS(RecordsCount >= stat.RecordsCount);
        RecordsCount -= stat.RecordsCount;
        Y_ABORT_UNLESS(RawBytes >= stat.RawBytes);
        RawBytes -= stat.RawBytes;
    }
};

class TBatchSerializationStat {
protected:
    double SerializedBytesPerRecord = 0;
    double RawBytesPerRecord = 0;
public:
    TBatchSerializationStat() = default;
    TBatchSerializationStat(const ui64 bytes, const ui64 recordsCount, const ui64 rawBytes) {
        Y_ABORT_UNLESS(recordsCount);
        SerializedBytesPerRecord = 1.0 * bytes / recordsCount;
        RawBytesPerRecord = 1.0 * rawBytes / recordsCount;
    }

    ui64 PredictPackedSize(const ui32 recordsCount) const {
        return SerializedBytesPerRecord * recordsCount;
    }

    TString DebugString() const {
        return TStringBuilder() << "{sbpr=" << SerializedBytesPerRecord << ";rbpr=" << RawBytesPerRecord << "}";
    }

    TBatchSerializationStat(const TSimpleSerializationStat& simple) {
        SerializedBytesPerRecord = simple.GetSerializedBytesPerRecord();
        RawBytesPerRecord = simple.GetRawBytesPerRecord();
    }

    void Merge(const TSimpleSerializationStat& item) {
        SerializedBytesPerRecord += item.GetSerializedBytesPerRecord();
        RawBytesPerRecord += item.GetRawBytesPerRecord();
    }

    std::vector<i64> SplitRecordsForBlobSize(const i64 recordsCount, const ui64 blobSize) const;

    std::optional<ui64> PredictOptimalPackRecordsCount(const ui64 recordsCount, const ui64 blobSize) const {
        if (!SerializedBytesPerRecord) {
            return {};
        }
        const ui64 fullSize = 1.0 * recordsCount * SerializedBytesPerRecord;
        if (fullSize < blobSize) {
            return recordsCount;
        } else {
            return std::floor(1.0 * blobSize / SerializedBytesPerRecord);
        }
    }

    std::optional<ui64> PredictOptimalSplitFactor(const ui64 recordsCount, const ui64 blobSize) const {
        if (!SerializedBytesPerRecord) {
            return {};
        }
        const ui64 fullSize = 1.0 * recordsCount * SerializedBytesPerRecord;
        if (fullSize < blobSize) {
            return 1;
        } else {
            return std::floor(1.0 * fullSize / blobSize);
        }
    }
};

class TColumnSerializationStat: public TSimpleSerializationStat {
private:
    using TBase = TSimpleSerializationStat;
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY_DEF(std::string, ColumnName);

public:
    TColumnSerializationStat(const ui32 columnId, const std::string& columnName)
        : ColumnId(columnId)
        , ColumnName(columnName) {
    }

    double GetPackedRecordSize() const {
        return (double)SerializedBytes / RecordsCount;
    }

    TColumnSerializationStat RecalcForRecordsCount(const ui64 recordsCount) const {
        TColumnSerializationStat result(ColumnId, ColumnName);
        result.Merge(TSimpleSerializationStat(
            1.0 * SerializedBytes / RecordsCount * recordsCount, recordsCount, 1.0 * RawBytes / RecordsCount * recordsCount));
        return result;
    }

    TString DebugString() const {
        return TStringBuilder() << "{id=" << ColumnId << ";name=" << ColumnName << ";details=" << TBase::DebugString() << "}";
    }

    void Merge(const TSimpleSerializationStat& item) {
        SerializedBytes += item.GetSerializedBytes();
        RawBytes += item.GetRawBytes();
        RecordsCount += item.GetRecordsCount();
    }
};

class TSerializationStats {
private:
    std::deque<TColumnSerializationStat> ColumnStat;
    std::map<ui32, TColumnSerializationStat*> StatsByColumnId;
    std::map<std::string, TColumnSerializationStat*> StatsByColumnName;

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "{columns=";
        for (auto&& i : ColumnStat) {
            sb << i.DebugString();
        }
        sb << ";}";
        return sb;
    }

    void Merge(const TSerializationStats& item) {
        for (auto&& i : item.ColumnStat) {
            AddStat(i);
        }
    }

    void AddStat(const TColumnSerializationStat& info) {
        auto it = StatsByColumnId.find(info.GetColumnId());
        if (it == StatsByColumnId.end()) {
            ColumnStat.emplace_back(info);
            AFL_VERIFY(StatsByColumnId.emplace(info.GetColumnId(), &ColumnStat.back()).second)("column_id", info.GetColumnId())(
                                                                      "column_name", info.GetColumnName());
            AFL_VERIFY(StatsByColumnName.emplace(info.GetColumnName(), &ColumnStat.back()).second)("column_id", info.GetColumnId())(
                                                                          "column_name", info.GetColumnName());
        } else {
            it->second->Merge(info);
        }
    }

    std::optional<TColumnSerializationStat> GetColumnInfo(const ui32 columnId) const {
        auto it = StatsByColumnId.find(columnId);
        if (it == StatsByColumnId.end()) {
            return {};
        } else {
            return *it->second;
        }
    }

    std::optional<TColumnSerializationStat> GetColumnInfo(const std::string& columnName) const {
        auto it = StatsByColumnName.find(columnName);
        if (it == StatsByColumnName.end()) {
            return {};
        } else {
            return *it->second;
        }
    }

    std::optional<TBatchSerializationStat> GetStatsForRecordBatch(const std::shared_ptr<arrow::RecordBatch>& rb) const;
    std::optional<TBatchSerializationStat> GetStatsForRecordBatch(const std::shared_ptr<arrow::Schema>& schema) const;

    template <class TContainer>
    std::optional<TBatchSerializationStat> GetStatsForColumns(const TContainer& ids, const bool fieldsRequired) const {
        std::optional<TBatchSerializationStat> result;
        for (auto&& i : ids) {
            auto columnInfo = GetColumnInfo(i);
            if (!columnInfo || columnInfo->GetRecordsCount() == 0) {
                if (fieldsRequired) {
                    return {};
                } else {
                    continue;
                }
            } else if (!result) {
                result = TBatchSerializationStat();
            }
            result->Merge(*columnInfo);
        }
        return result;
    }
};
}   // namespace NKikimr::NArrow::NSplitter
