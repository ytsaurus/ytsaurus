#pragma once

#include "stats.h"

#include <contrib/ydb/core/formats/arrow/common/container.h>

#include <contrib/ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TColumnsData {
private:
    TDictStats Stats;
    YDB_READONLY_DEF(std::shared_ptr<TGeneralContainer>, Records);

public:
    std::shared_ptr<IChunkedArray> GetPathAccessor(const std::string_view path) const {
        auto idx = Stats.GetKeyIndexOptional(path);
        if (!idx) {
            return nullptr;
        } else {
            return Records->GetColumnVerified(*idx);
        }
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("stats", Stats.DebugJson());
        result.InsertValue("records", Records->DebugJson());
        return result;
    }

    TColumnsData ApplyFilter(const TColumnFilter& filter) const;

    TColumnsData Slice(const ui32 offset, const ui32 count) const;

    static TColumnsData BuildEmpty(const ui32 recordsCount) {
        return TColumnsData(TDictStats::BuildEmpty(), std::make_shared<TGeneralContainer>(recordsCount));
    }

    ui64 GetRawSize() const {
        return Records->GetRawSizeVerified();
    }

    class TIterator {
    private:
        ui32 KeyIndex;
        std::shared_ptr<IChunkedArray> GlobalChunkedArray;
        const arrow::StringArray* CurrentArrayData;
        std::optional<IChunkedArray::TFullChunkedArrayAddress> FullArrayAddress;
        std::optional<IChunkedArray::TFullDataAddress> ChunkAddress;
        ui32 CurrentIndex = 0;

        void InitArrays();

    public:
        TIterator(const ui32 keyIndex, const std::shared_ptr<IChunkedArray>& chunkedArray)
            : KeyIndex(keyIndex)
            , GlobalChunkedArray(chunkedArray) {
            InitArrays();
        }

        ui32 GetCurrentRecordIndex() const {
            return CurrentIndex;
        }

        ui32 GetKeyIndex() const {
            return KeyIndex;
        }

        std::string_view GetValue() const {
            auto view = CurrentArrayData->GetView(ChunkAddress->GetAddress().GetLocalIndex(CurrentIndex));
            return std::string_view(view.data(), view.size());
        }

        bool HasValue() const {
            return !CurrentArrayData->IsNull(ChunkAddress->GetAddress().GetLocalIndex(CurrentIndex));
        }

        bool IsValid() const {
            return CurrentIndex < GlobalChunkedArray->GetRecordsCount();
        }

        bool SkipRecordTo(const ui32 recordIndex) {
            if (recordIndex <= CurrentIndex) {
                return true;
            }
            AFL_VERIFY(IsValid());
            AFL_VERIFY(ChunkAddress->GetAddress().Contains(CurrentIndex));
            CurrentIndex = recordIndex;
            for (; CurrentIndex < ChunkAddress->GetAddress().GetGlobalFinishPosition(); ++CurrentIndex) {
                if (CurrentArrayData->IsNull(CurrentIndex - ChunkAddress->GetAddress().GetGlobalStartPosition())) {
                    continue;
                }
                return true;
            }
            InitArrays();
            return IsValid();
        }

        bool Next() {
            AFL_VERIFY(IsValid());
            AFL_VERIFY(ChunkAddress->GetAddress().Contains(CurrentIndex));
            ++CurrentIndex;
            for (; CurrentIndex < ChunkAddress->GetAddress().GetGlobalFinishPosition(); ++CurrentIndex) {
                if (CurrentArrayData->IsNull(CurrentIndex - ChunkAddress->GetAddress().GetGlobalStartPosition())) {
                    continue;
                }
                return true;
            }
            InitArrays();
            return IsValid();
        }
    };

    TIterator BuildIterator(const ui32 keyIndex) const {
        return TIterator(keyIndex, Records->GetColumnVerified(keyIndex));
    }

    const TDictStats& GetStats() const {
        return Stats;
    }

    TColumnsData(const TDictStats& dict, const std::shared_ptr<TGeneralContainer>& data)
        : Stats(dict)
        , Records(data) {
        AFL_VERIFY(Records->num_columns() == Stats.GetColumnsCount())("records", Records->num_columns())("stats", Stats.GetColumnsCount());
        for (auto&& i : Records->GetColumns()) {
            AFL_VERIFY(i->GetDataType()->id() == arrow::utf8()->id());
        }
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
