#include "arrow_helpers.h"
#include "permutations.h"
#include "replace_key.h"
#include "simple_arrays_cache.h"

#include "switch/switch_type.h"
#include "validation/validation.h"

#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/services/services.pb.h>
#include <contrib/ydb/library/yverify_stream/yverify_stream.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/string/join.h>
#include <util/system/yassert.h>

#include <memory>

#define Y_VERIFY_OK(status) Y_ABORT_UNLESS(status.ok(), "%s", status.ToString().c_str())

namespace NKikimr::NArrow {

TString SerializeSchema(const arrow::Schema& schema) {
    auto buffer = TStatusValidator::GetValid(arrow::ipc::SerializeSchema(schema));
    return buffer->ToString();
}

std::shared_ptr<arrow::RecordBatch> MakeEmptyBatch(const std::shared_ptr<arrow::Schema>& schema, const ui32 rowsCount) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(schema->num_fields());

    for (auto& field : schema->fields()) {
        auto result = NArrow::TThreadSimpleArraysCache::GetNull(field->type(), rowsCount);
        columns.emplace_back(result);
        Y_ABORT_UNLESS(result);
    }
    return arrow::RecordBatch::Make(schema, rowsCount, columns);
}

std::shared_ptr<arrow::RecordBatch> CombineBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    if (batches.empty()) {
        return nullptr;
    }
    auto table = TStatusValidator::GetValid(arrow::Table::FromRecordBatches(batches));
    return table ? ToBatch(table) : nullptr;
}

std::shared_ptr<arrow::RecordBatch> ToBatch(const std::shared_ptr<arrow::Table>& tableExt) {
    if (!tableExt) {
        return nullptr;
    }
    if (tableExt->num_rows() == 0) {
        return MakeEmptyBatch(tableExt->schema(), 0);
    }
    std::shared_ptr<arrow::Table> res = TStatusValidator::GetValid(tableExt->CombineChunks());
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(tableExt->num_columns());
    for (auto& col : res->columns()) {
        AFL_VERIFY(col->num_chunks() == 1)("size", col->num_chunks())("size_bytes", GetTableDataSize(res))("schema", res->schema()->ToString())(
            "size_new", GetTableDataSize(res));
        columns.push_back(col->chunk(0));
    }
    return arrow::RecordBatch::Make(res->schema(), res->num_rows(), columns);
}

// Check if the permutation doesn't reorder anything
bool IsTrivial(const arrow::UInt64Array& permutation, const ui64 originalLength) {
    if ((ui64)permutation.length() != originalLength) {
        return false;
    }
    for (i64 i = 0; i < permutation.length(); ++i) {
        if (permutation.Value(i) != (ui64)i) {
            return false;
        }
    }
    return true;
}

std::shared_ptr<arrow::RecordBatch> Reorder(
        const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::UInt64Array>& permutation,
        const bool canRemove, arrow::MemoryPool* memoryPool) {
    Y_ABORT_UNLESS(permutation->length() == batch->num_rows() || canRemove);

    arrow::compute::ExecContext ctx(memoryPool);
    auto res = IsTrivial(*permutation, batch->num_rows())
        ? batch
        : arrow::compute::Take(batch, permutation, arrow::compute::TakeOptions::Defaults(), &ctx);
    Y_ABORT_UNLESS(res.ok());
    return (*res).record_batch();
}

THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> ShardingSplit(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        const THashMap<ui64, std::vector<ui32>>& shardRows,
        arrow::MemoryPool* memoryPool) {
    AFL_VERIFY(batch);
    std::shared_ptr<arrow::UInt64Array> permutation;
    {
        arrow::UInt64Builder builder(memoryPool);
        Y_VERIFY_OK(builder.Reserve(batch->num_rows()));

        for (auto&& [shardId, rowIdxs] : shardRows) {
            for (auto& row : rowIdxs) {
                Y_VERIFY_OK(builder.Append(row));
            }
        }
        Y_VERIFY_OK(builder.Finish(&permutation));
    }

    auto reorderedBatch = Reorder(batch, permutation, false);

    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> out;

    int offset = 0;
    for (auto&& [shardId, shardRowIdxs] : shardRows) {
        if (shardRowIdxs.empty()) {
            continue;
        }
        out.emplace(shardId, reorderedBatch->Slice(offset, shardRowIdxs.size()));
        offset += shardRowIdxs.size();
    }

    Y_ABORT_UNLESS(offset == batch->num_rows());
    return out;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> ShardingSplit(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::vector<ui32>>& shardRows, const ui32 numShards) {
    AFL_VERIFY(batch);
    std::shared_ptr<arrow::UInt64Array> permutation;
    {
        arrow::UInt64Builder builder;
        Y_VERIFY_OK(builder.Reserve(batch->num_rows()));

        for (ui32 shardNo = 0; shardNo < numShards; ++shardNo) {
            for (auto& row : shardRows[shardNo]) {
                Y_VERIFY_OK(builder.Append(row));
            }
        }
        Y_VERIFY_OK(builder.Finish(&permutation));
    }

    auto reorderedBatch = Reorder(batch, permutation, false);

    std::vector<std::shared_ptr<arrow::RecordBatch>> out(numShards);

    int offset = 0;
    for (ui32 shardNo = 0; shardNo < numShards; ++shardNo) {
        int length = shardRows[shardNo].size();
        if (length) {
            out[shardNo] = reorderedBatch->Slice(offset, length);
            offset += length;
        }
    }

    Y_ABORT_UNLESS(offset == batch->num_rows());
    return out;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> ShardingSplit(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<ui32>& sharding, ui32 numShards) {
    AFL_VERIFY(batch);
    Y_ABORT_UNLESS((size_t)batch->num_rows() == sharding.size());

    std::vector<std::vector<ui32>> shardRows(numShards);
    for (size_t row = 0; row < sharding.size(); ++row) {
        ui32 shardNo = sharding[row];
        Y_ABORT_UNLESS(shardNo < numShards);
        shardRows[shardNo].push_back(row);
    }
    return ShardingSplit(batch, shardRows, numShards);
}

bool HasAllColumns(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<arrow::Schema>& schema) {
    for (auto& field : schema->fields()) {
        if (batch->schema()->GetFieldIndex(field->name()) < 0) {
            return false;
        }
    }
    return true;
}

std::vector<std::unique_ptr<arrow::ArrayBuilder>> MakeBuilders(
    const std::shared_ptr<arrow::Schema>& schema, size_t reserve, const std::map<std::string, ui64>& sizeByColumn) {
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.reserve(schema->num_fields());

    for (auto& field : schema->fields()) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        TStatusValidator::Validate(arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &builder));
        if (sizeByColumn.size()) {
            auto it = sizeByColumn.find(field->name());
            if (it != sizeByColumn.end()) {
                AFL_VERIFY(NArrow::ReserveData(*builder, it->second))("size", it->second)("field", field->name());
            }
        }

        if (reserve) {
            TStatusValidator::Validate(builder->Reserve(reserve));
        }

        builders.emplace_back(std::move(builder));
    }
    return builders;
}

std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const std::shared_ptr<arrow::Field>& field, const ui32 reserveItems, const ui32 reserveSize) {
    AFL_VERIFY(field);
    return MakeBuilder(field->type(), reserveItems, reserveSize);
}

std::unique_ptr<arrow::ArrayBuilder> MakeBuilder(const std::shared_ptr<arrow::DataType>& type, const ui32 reserveItems, const ui32 reserveSize) {
    AFL_VERIFY(type);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    TStatusValidator::Validate(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder));
    if (reserveSize) {
        ReserveData(*builder, reserveSize);
    }
    TStatusValidator::Validate(builder->Reserve(reserveItems));
    return std::move(builder);
}

std::shared_ptr<arrow::Array> FinishBuilder(std::unique_ptr<arrow::ArrayBuilder>&& builder) {
    std::shared_ptr<arrow::Array> array;
    TStatusValidator::Validate(builder->Finish(&array));
    return array;
}

std::vector<std::shared_ptr<arrow::Array>> Finish(std::vector<std::unique_ptr<arrow::ArrayBuilder>>&& builders) {
    return FinishBuilders(std::move(builders));
}

std::vector<TString> ColumnNames(const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<TString> out;
    out.reserve(schema->num_fields());
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto& name = schema->field(i)->name();
        out.emplace_back(TString(name.data(), name.size()));
    }
    return out;
}

std::shared_ptr<arrow::UInt64Array> MakeUI64Array(const ui64 value, const i64 size) {
    auto res = arrow::MakeArrayFromScalar(arrow::UInt64Scalar(value), size);
    Y_ABORT_UNLESS(res.ok());
    return std::static_pointer_cast<arrow::UInt64Array>(*res);
}

std::shared_ptr<arrow::StringArray> MakeStringArray(const TString& value, const i64 size) {
    auto res = arrow::MakeArrayFromScalar(arrow::StringScalar(value), size);
    Y_ABORT_UNLESS(res.ok());
    return std::static_pointer_cast<arrow::StringArray>(*res);
}

std::pair<int, int> FindMinMaxPosition(const std::shared_ptr<arrow::Array>& array) {
    if (array->length() == 0) {
        return { -1, -1 };
    }

    int minPos = 0;
    int maxPos = 0;
    SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

        auto& column = static_cast<const TArray&>(*array);

        for (int i = 1; i < column.length(); ++i) {
            const auto& value = column.GetView(i);
            if (value < column.GetView(minPos)) {
                minPos = i;
            }
            if (value > column.GetView(maxPos)) {
                maxPos = i;
            }
        }
        return true;
    });
    return { minPos, maxPos };
}

std::shared_ptr<arrow::Scalar> MinScalar(const std::shared_ptr<arrow::DataType>& type) {
    std::shared_ptr<arrow::Scalar> out;
    SwitchType(type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow::TypeTraits<T>::ScalarType;

        if constexpr (std::is_same_v<T, arrow::StringType> || std::is_same_v<T, arrow::BinaryType> ||
                      std::is_same_v<T, arrow::LargeStringType> || std::is_same_v<T, arrow::LargeBinaryType>) {
            out = std::make_shared<TScalar>(arrow::Buffer::FromString(""), type);
        } else if constexpr (std::is_same_v<T, arrow::FixedSizeBinaryType>) {
            std::string s(static_cast<arrow::FixedSizeBinaryType&>(*type).byte_width(), '\0');
            out = std::make_shared<TScalar>(arrow::Buffer::FromString(s), type);
        } else if constexpr (std::is_same_v<T, arrow::HalfFloatType>) {
            return false;
        } else if constexpr (arrow::is_temporal_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
            out = std::make_shared<TScalar>(Min<TCType>(), type);
        } else if constexpr (arrow::has_c_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
            out = std::make_shared<TScalar>(Min<TCType>());
        } else {
            return false;
        }
        return true;
    });
    Y_ABORT_UNLESS(out);
    return out;
}

namespace {

template <class T>
class TDefaultScalarValue {
public:
    static constexpr T Value = 0;
};

template <>
class TDefaultScalarValue<bool> {
public:
    static constexpr bool Value = false;
};

}   // namespace

std::shared_ptr<arrow::Scalar> DefaultScalar(const std::shared_ptr<arrow::DataType>& type) {
    std::shared_ptr<arrow::Scalar> out;
    SwitchType(type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow::TypeTraits<T>::ScalarType;

        if constexpr (std::is_same_v<T, arrow::StringType> || std::is_same_v<T, arrow::BinaryType> ||
                      std::is_same_v<T, arrow::LargeStringType> || std::is_same_v<T, arrow::LargeBinaryType>) {
            out = std::make_shared<TScalar>(arrow::Buffer::FromString(""), type);
        } else if constexpr (std::is_same_v<T, arrow::FixedSizeBinaryType>) {
            std::string s(static_cast<arrow::FixedSizeBinaryType&>(*type).byte_width(), '\0');
            out = std::make_shared<TScalar>(arrow::Buffer::FromString(s), type);
        } else if constexpr (std::is_same_v<T, arrow::HalfFloatType>) {
            return false;
        } else if constexpr (arrow::is_temporal_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
            out = std::make_shared<TScalar>(TDefaultScalarValue<TCType>::Value, type);
        } else if constexpr (arrow::has_c_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
            out = std::make_shared<TScalar>(TDefaultScalarValue<TCType>::Value);
        } else {
            return false;
        }
        return true;
    });
    AFL_VERIFY(out)("type", type->ToString());
    return out;
}

std::shared_ptr<arrow::Scalar> GetScalar(const std::shared_ptr<arrow::Array>& array, int position) {
    auto res = array->GetScalar(position);
    Y_ABORT_UNLESS(res.ok());
    return *res;
}

bool IsGoodScalar(const std::shared_ptr<arrow::Scalar>& x) {
    if (!x) {
        return false;
    }

    return SwitchType(x->type->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
        using TValue = std::decay_t<decltype(static_cast<const TScalar&>(*x).value)>;

        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            const auto& xval = static_cast<const TScalar&>(*x).value;
            return xval && xval->data();
        }
        if constexpr (std::is_arithmetic_v<TValue>) {
            return true;
        }
        return false;
    });
}

bool ScalarLess(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y) {
    Y_ABORT_UNLESS(x);
    Y_ABORT_UNLESS(y);
    return ScalarLess(*x, *y);
}

bool ScalarLess(const arrow::Scalar& x, const arrow::Scalar& y) {
    return ScalarCompare(x, y) < 0;
}

bool ColumnEqualsScalar(const std::shared_ptr<arrow::Array>& c, const ui32 position, const std::shared_ptr<arrow::Scalar>& s) {
    AFL_VERIFY(c);
    if (!s) {
        return c->IsNull(position);
    }
    AFL_VERIFY(c->type()->Equals(s->type))("s", s->type->ToString())("c", c->type()->ToString());

    return SwitchTypeImpl<bool, 0>(c->type()->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
        using TArrayType = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        using TValue = std::decay_t<decltype(static_cast<const TScalar&>(*s).value)>;

        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            const auto& cval = static_cast<const TArrayType&>(*c).GetView(position);
            const auto& sval = static_cast<const TScalar&>(*s).value;
            AFL_VERIFY(sval);
            TStringBuf cBuf(reinterpret_cast<const char*>(cval.data()), cval.size());
            TStringBuf sBuf(reinterpret_cast<const char*>(sval->data()), sval->size());
            return cBuf == sBuf;
        }
        if constexpr (std::is_arithmetic_v<TValue>) {
            const auto cval = static_cast<const TArrayType&>(*c).GetView(position);
            const auto sval = static_cast<const TScalar&>(*s).value;
            return (cval == sval);
        }
        Y_ABORT_UNLESS(false);   // TODO: non primitive types
        return false;
    });
}

int ScalarCompare(const arrow::Scalar& x, const arrow::Scalar& y) {
    Y_VERIFY_S(x.type->Equals(y.type), x.type->ToString() + " vs " + y.type->ToString());

    return SwitchTypeImpl<int, 0>(x.type->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
        using TValue = std::decay_t<decltype(static_cast<const TScalar&>(x).value)>;

        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            const auto& xval = static_cast<const TScalar&>(x).value;
            const auto& yval = static_cast<const TScalar&>(y).value;
            Y_ABORT_UNLESS(xval);
            Y_ABORT_UNLESS(yval);
            TStringBuf xBuf(reinterpret_cast<const char*>(xval->data()), xval->size());
            TStringBuf yBuf(reinterpret_cast<const char*>(yval->data()), yval->size());
            if (xBuf < yBuf) {
                return -1;
            } else if (yBuf < xBuf) {
                return 1;
            } else {
                return 0;
            }
        }
        if constexpr (std::is_arithmetic_v<TValue>) {
            const auto& xval = static_cast<const TScalar&>(x).value;
            const auto& yval = static_cast<const TScalar&>(y).value;
            if (xval < yval) {
                return -1;
            } else if (yval < xval) {
                return 1;
            } else {
                return 0;
            }
        }
        Y_ABORT_UNLESS(false);   // TODO: non primitive types
        return 0;
    });
}

int ScalarCompare(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y) {
    Y_ABORT_UNLESS(x);
    Y_ABORT_UNLESS(y);
    return ScalarCompare(*x, *y);
}

int ScalarCompareNullable(const std::shared_ptr<arrow::Scalar>& x, const std::shared_ptr<arrow::Scalar>& y) {
    if (!x && !!y) {
        return -1;
    }
    if (!!x && !y) {
        return 1;
    }
    if (!x && !y) {
        return 0;
    }
    return ScalarCompare(*x, *y);
}

std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<bool>& vec) {
    std::shared_ptr<arrow::Array> out;
    arrow::BooleanBuilder builder;
    for (const auto val : vec) {
        Y_ABORT_UNLESS(builder.Append(val).ok());
    }
    Y_ABORT_UNLESS(builder.Finish(&out).ok());
    return out;
}

bool ArrayScalarsEqual(const std::shared_ptr<arrow::Array>& lhs, const std::shared_ptr<arrow::Array>& rhs) {
    bool res = lhs->length() == rhs->length();
    for (int64_t i = 0; i < lhs->length() && res; ++i) {
        res &= arrow::ScalarEquals(*lhs->GetScalar(i).ValueOrDie(), *rhs->GetScalar(i).ValueOrDie());
    }
    return res;
}

bool ReserveData(arrow::ArrayBuilder& builder, const size_t size) {
    arrow::Status result = arrow::Status::OK();
    if (builder.type()->id() == arrow::Type::BINARY || builder.type()->id() == arrow::Type::STRING) {
        static_assert(std::is_convertible_v<arrow::StringBuilder&, arrow::BaseBinaryBuilder<arrow::BinaryType>&>,
            "Expected StringBuilder to be BaseBinaryBuilder<BinaryType>");
        auto& bBuilder = static_cast<arrow::BaseBinaryBuilder<arrow::BinaryType>&>(builder);
        result = bBuilder.ReserveData(size);
    }

    if (!result.ok()) {
        AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "ReserveData")("error", result.ToString());
    }
    return result.ok();
}

template <class TData, class TColumn, class TBuilder>
bool MergeBatchColumnsImpl(const std::vector<std::shared_ptr<TData>>& batches, std::shared_ptr<TData>& result,
    const std::vector<std::string>& columnsOrder, const bool orderFieldsAreNecessary, const TBuilder& builder) {
    if (batches.empty()) {
        result = nullptr;
        return true;
    }
    if (batches.size() == 1) {
        result = batches.front();
        return true;
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<TColumn>> columns;
    std::map<std::string, ui32> fieldNames;
    for (auto&& i : batches) {
        Y_ABORT_UNLESS(i);
        for (auto&& f : i->schema()->fields()) {
            if (!fieldNames.emplace(f->name(), fields.size()).second) {
                AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "duplicated column")("name", f->name());
                return false;
            }
            fields.emplace_back(f);
        }
        if (i->num_rows() != batches.front()->num_rows()) {
            AFL_ERROR(NKikimrServices::ARROW_HELPER)("event", "inconsistency record sizes")("i", i->num_rows())(
                "front", batches.front()->num_rows());
            return false;
        }
        for (auto&& c : i->columns()) {
            columns.emplace_back(c);
        }
    }

    Y_ABORT_UNLESS(fields.size() == columns.size());
    if (columnsOrder.size()) {
        std::vector<std::shared_ptr<arrow::Field>> fieldsOrdered;
        std::vector<std::shared_ptr<TColumn>> columnsOrdered;
        for (auto&& i : columnsOrder) {
            auto it = fieldNames.find(i);
            if (orderFieldsAreNecessary) {
                Y_ABORT_UNLESS(it != fieldNames.end());
            } else if (it == fieldNames.end()) {
                continue;
            }
            fieldsOrdered.emplace_back(fields[it->second]);
            columnsOrdered.emplace_back(columns[it->second]);
        }
        std::swap(fieldsOrdered, fields);
        std::swap(columnsOrdered, columns);
    }
    result = builder(std::make_shared<arrow::Schema>(fields), batches.front()->num_rows(), std::move(columns));
    return true;
}

bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow::Table>>& batches, std::shared_ptr<arrow::Table>& result,
    const std::vector<std::string>& columnsOrder, const bool orderFieldsAreNecessary) {
    const auto builder = [](const std::shared_ptr<arrow::Schema>& schema, const ui32 recordsCount,
                             std::vector<std::shared_ptr<arrow::ChunkedArray>>&& columns) {
        return arrow::Table::Make(schema, columns, recordsCount);
    };

    return MergeBatchColumnsImpl<arrow::Table, arrow::ChunkedArray>(batches, result, columnsOrder, orderFieldsAreNecessary, builder);
}

bool MergeBatchColumns(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, std::shared_ptr<arrow::RecordBatch>& result,
    const std::vector<std::string>& columnsOrder, const bool orderFieldsAreNecessary) {
    const auto builder = [](const std::shared_ptr<arrow::Schema>& schema, const ui32 recordsCount,
                             std::vector<std::shared_ptr<arrow::Array>>&& columns) {
        return arrow::RecordBatch::Make(schema, recordsCount, columns);
    };

    return MergeBatchColumnsImpl<arrow::RecordBatch, arrow::Array>(batches, result, columnsOrder, orderFieldsAreNecessary, builder);
}

std::partial_ordering ColumnsCompare(
    const std::vector<std::shared_ptr<arrow::Array>>& x, const ui32 xRow, const std::vector<std::shared_ptr<arrow::Array>>& y, const ui32 yRow) {
    return TRawReplaceKey(&x, xRow).CompareNotNull(TRawReplaceKey(&y, yRow));
}

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::RecordBatch> array, const ui32 position) {
    NJson::TJsonValue result = NJson::JSON_ARRAY;
    for (auto&& i : array->columns()) {
        result.AppendValue(DebugJson(i, position));
    }
    return result;
}

TString DebugString(std::shared_ptr<arrow::Array> array, const ui32 position) {
    if (!array) {
        return "_NO_DATA";
    }
    Y_ABORT_UNLESS(position < array->length());
    TStringBuilder result;
    SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

        auto& column = static_cast<const TArray&>(*array);
        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            auto value = column.GetString(position);
            result << TString(value.data(), value.size());
        }
        if constexpr (arrow::has_c_type<typename TWrap::T>()) {
            result << column.Value(position);
        }
        return true;
    });
    return result;
}

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::Array> array, const ui32 position) {
    if (!array) {
        return NJson::JSON_NULL;
    }
    Y_ABORT_UNLESS(position < array->length());
    NJson::TJsonValue result = NJson::JSON_MAP;
    SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

        auto& column = static_cast<const TArray&>(*array);
        result.InsertValue("type", typeid(TArray).name());
        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            auto value = column.GetString(position);
            result.InsertValue("value", TString(value.data(), value.size()));
        }
        if constexpr (arrow::has_c_type<typename TWrap::T>()) {
            result.InsertValue("value", column.Value(position));
        }
        return true;
    });
    return result;
}

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::Array> array, const ui32 head, const ui32 tail) {
    if (!array) {
        return NJson::JSON_NULL;
    }
    NJson::TJsonValue resultFull = NJson::JSON_MAP;
    resultFull.InsertValue("length", array->length());
    SwitchType(array->type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

        auto& column = static_cast<const TArray&>(*array);
        resultFull.InsertValue("type", typeid(TArray).name());
        resultFull.InsertValue("head", head);
        resultFull.InsertValue("tail", tail);
        auto& result = resultFull.InsertValue("data", NJson::JSON_ARRAY);
        for (int i = 0; i < column.length(); ++i) {
            if (i >= (int)head && i + (int)tail < column.length()) {
                continue;
            }
            if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                auto value = column.GetString(i);
                result.AppendValue(TString(value.data(), value.size()));
            }
            if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                result.AppendValue(column.Value(i));
            }
        }
        return true;
    });
    return resultFull;
}

NJson::TJsonValue DebugJson(std::shared_ptr<arrow::RecordBatch> batch, const ui32 head, const ui32 tail) {
    if (!batch) {
        return NJson::JSON_NULL;
    }
    NJson::TJsonValue result = NJson::JSON_ARRAY;
    ui32 idx = 0;
    for (auto&& i : batch->columns()) {
        auto& jsonColumn = result.AppendValue(NJson::JSON_MAP);
        jsonColumn.InsertValue("name", batch->column_name(idx));
        jsonColumn.InsertValue("data", DebugJson(i, head, tail));
        ++idx;
    }
    return result;
}

std::shared_ptr<arrow::RecordBatch> MergeColumns(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::optional<ui32> recordsCount;
    std::set<std::string> columnNames;
    for (auto&& batch : batches) {
        if (!batch) {
            continue;
        }
        for (auto&& column : batch->columns()) {
            columns.emplace_back(column);
            if (!recordsCount) {
                recordsCount = column->length();
            } else {
                Y_ABORT_UNLESS(*recordsCount == column->length());
            }
        }
        for (auto&& field : batch->schema()->fields()) {
            AFL_VERIFY(columnNames.emplace(field->name()).second)("field_name", field->name());
            fields.emplace_back(field);
        }
    }
    if (columns.empty()) {
        return nullptr;
    }
    auto schema = std::make_shared<arrow::Schema>(fields);
    return arrow::RecordBatch::Make(schema, *recordsCount, columns);
}

std::vector<std::shared_ptr<arrow::RecordBatch>> SliceToRecordBatches(const std::shared_ptr<arrow::Table>& t) {
    if (!t->num_rows()) {
        return {};
    }
    std::vector<ui32> positions;
    {
        for (auto&& i : t->columns()) {
            ui32 pos = 0;
            for (auto&& arr : i->chunks()) {
                positions.emplace_back(pos);
                pos += arr->length();
            }
            AFL_VERIFY(pos == t->num_rows())("pos", pos)("length", t->num_rows());
        }
        positions.emplace_back(t->num_rows());
    }
    std::sort(positions.begin(), positions.end());
    positions.erase(std::unique(positions.begin(), positions.end()), positions.end());
    AFL_VERIFY(positions.size() > 1)("size", positions.size())("positions", JoinSeq(",", positions));
    std::vector<std::vector<std::shared_ptr<arrow::Array>>> slicedData;
    slicedData.resize(positions.size() - 1);
    for (auto&& i : t->columns()) {
        ui32 currentPosition = 0;
        auto it = i->chunks().begin();
        ui32 length = 0;
        const auto initializeIt = [&length, &it, &i]() {
            for (; it != i->chunks().end() && !(*it)->length(); ++it) {
            }
            if (it != i->chunks().end()) {
                length = (*it)->length();
            }
        };
        initializeIt();
        for (ui32 idx = 0; idx + 1 < positions.size(); ++idx) {
            AFL_VERIFY(it != i->chunks().end());
            AFL_VERIFY(positions[idx + 1] - currentPosition <= length)("length", length)("idx+1", positions[idx + 1])("pos", currentPosition);
            auto chunk = (*it)->Slice(positions[idx] - currentPosition, positions[idx + 1] - positions[idx]);
            AFL_VERIFY_DEBUG(chunk->length() == positions[idx + 1] - positions[idx])
            ("length", chunk->length())("expect", positions[idx + 1] - positions[idx]);
            if (positions[idx + 1] - currentPosition == length) {
                ++it;
                initializeIt();
                currentPosition = positions[idx + 1];
            }
            slicedData[idx].emplace_back(chunk);
        }
    }
    std::vector<std::shared_ptr<arrow::RecordBatch>> result;
    ui32 count = 0;
    for (auto&& i : slicedData) {
        AFL_VERIFY(i.size());
        AFL_VERIFY(i.front()->length());
        result.emplace_back(arrow::RecordBatch::Make(t->schema(), i.front()->length(), i));
        count += result.back()->num_rows();
    }
    AFL_VERIFY(count == t->num_rows())("count", count)("t", t->num_rows())("sd_size", slicedData.size())("columns", t->num_columns())(
                          "schema", t->schema()->ToString());
    return result;
}

std::shared_ptr<arrow::Table> ToTable(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return nullptr;
    }
    return TStatusValidator::GetValid(arrow::Table::FromRecordBatches(batch->schema(), { batch }));
}

bool HasNulls(const std::shared_ptr<arrow::Array>& column) {
    AFL_VERIFY(column);
    return column->null_bitmap_data();
}

std::vector<TString> ConvertStrings(const std::vector<std::string>& input) {
    std::vector<TString> result;
    for (auto&& i : input) {
        result.emplace_back(i);
    }
    return result;
}

std::vector<std::string> ConvertStrings(const std::vector<TString>& input) {
    std::vector<std::string> result;
    for (auto&& i : input) {
        result.emplace_back(i);
    }
    return result;
}

std::shared_ptr<arrow::Table> DeepCopy(const std::shared_ptr<arrow::Table>& table, arrow::MemoryPool* pool) {
    arrow::ArrayVector arrays;

    for (const auto& column : table->columns()) {
        auto&& array = TStatusValidator::GetValid(arrow::Concatenate(column->chunks(), pool));
        arrays.push_back(std::move(array));
    }

    return arrow::Table::Make(table->schema(), arrays);
}

TConclusion<bool> ScalarIsTrue(const arrow::Scalar& x) {
    std::optional<bool> result;
    if (!SwitchTypeImpl<bool, false>(x.type->id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
            using TValue = std::decay_t<decltype(static_cast<const TScalar&>(x).value)>;

            if constexpr (std::is_arithmetic_v<TValue>) {
                result = ((int)static_cast<const TScalar&>(x).value == 1);
                return true;
            }
            return false;
        })) {
        return TConclusionStatus::Fail("not appropriate scalar type for bool interpretation");
    }
    Y_ABORT_UNLESS(result);
    return *result;
}

TConclusion<bool> ScalarIsFalse(const arrow::Scalar& x) {
    std::optional<bool> result;
    if (!SwitchTypeImpl<bool, false>(x.type->id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
            using TValue = std::decay_t<decltype(static_cast<const TScalar&>(x).value)>;

            if constexpr (std::is_arithmetic_v<TValue>) {
                result = ((int)static_cast<const TScalar&>(x).value == 0);
                return true;
            }
            return false;
        })) {
        return TConclusionStatus::Fail("not appropriate scalar type for bool interpretation");
    }
    Y_ABORT_UNLESS(result);
    return *result;
}

TConclusion<bool> ScalarIsFalse(const std::shared_ptr<arrow::Scalar>& x) {
    if (!x) {
        return true;
    }
    return ScalarIsFalse(*x);
}
TConclusion<bool> ScalarIsTrue(const std::shared_ptr<arrow::Scalar>& x) {
    if (!x) {
        return false;
    }
    return ScalarIsTrue(*x);
}

std::vector<std::shared_ptr<arrow::Array>> FinishBuilders(std::vector<std::unique_ptr<arrow::ArrayBuilder>>&& builders) {
    std::vector<std::shared_ptr<arrow::Array>> out;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        TStatusValidator::Validate(builder->Finish(&array));
        out.emplace_back(array);
    }
    return out;
}

}   // namespace NKikimr::NArrow
