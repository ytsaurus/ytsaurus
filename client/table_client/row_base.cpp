#include "row_base.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/format.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TColumnFilter::TColumnFilter()
    : IsUniversal_(true)
{ }

TColumnFilter::TColumnFilter(const std::initializer_list<int>& indexes)
    : IsUniversal_(false)
    , Indexes_(indexes.begin(), indexes.end())
{ }

TColumnFilter::TColumnFilter(TIndexes&& indexes)
    : IsUniversal_(false)
    , Indexes_(std::move(indexes))
{ }

TColumnFilter::TColumnFilter(const std::vector<int>& indexes)
    : IsUniversal_(false)
    , Indexes_(indexes.begin(), indexes.end())
{ }

TColumnFilter::TColumnFilter(int schemaColumnCount)
    : IsUniversal_(false)
{
    for (int i = 0; i < schemaColumnCount; ++i) {
        Indexes_.push_back(i);
    }
}

TNullable<int> TColumnFilter::FindPosition(int columnIndex) const
{
    if (IsUniversal_) {
        THROW_ERROR_EXCEPTION("Unable to search index in column filter with IsUniversal flag");
    }
    auto it = std::find(Indexes_.begin(), Indexes_.end(), columnIndex);
    if (it == Indexes_.end()) {
        return Null;
    }
    return std::distance(Indexes_.begin(), it);
}

int TColumnFilter::GetPosition(int columnIndex) const
{
    if (auto indexOrNull = FindPosition(columnIndex)) {
        return *indexOrNull;
    }

    THROW_ERROR_EXCEPTION("Column filter does not contain column index %Qv", columnIndex);
}

bool TColumnFilter::ContainsIndex(int columnIndex) const
{
    if (IsUniversal_) {
        return true;
    }

    return std::find(Indexes_.begin(), Indexes_.end(), columnIndex) != Indexes_.end();
}

const TColumnFilter::TIndexes& TColumnFilter::GetIndexes() const
{
    YCHECK(!IsUniversal_);
    return Indexes_;
}

bool TColumnFilter::IsUniversal() const
{
    return IsUniversal_;
}

////////////////////////////////////////////////////////////////////////////////

void ValidateDataValueType(EValueType type)
{
    if (type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String &&
        type != EValueType::Any &&
        type != EValueType::Null)
    {
        THROW_ERROR_EXCEPTION("Invalid data value type %Qlv", type);
    }
}

void ValidateKeyValueType(EValueType type)
{
    if (type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String &&
        type != EValueType::Null &&
        type != EValueType::Min &&
        type != EValueType::Max)
    {
        THROW_ERROR_EXCEPTION("Invalid key value type %Qlv; only scalar types are allowed for the key columns", type);
    }
}

void ValidateSchemaValueType(EValueType type)
{
    if (type != EValueType::Int64 &&
        type != EValueType::Uint64 &&
        type != EValueType::Double &&
        type != EValueType::Boolean &&
        type != EValueType::String &&
        type != EValueType::Any)
    {
        THROW_ERROR_EXCEPTION("Invalid schema value type %Qlv", type);
    }
}

void ValidateColumnFilter(const TColumnFilter& columnFilter, int schemaColumnCount)
{
    if (columnFilter.IsUniversal())
        return;

    SmallVector<bool, TypicalColumnCount> flags;
    flags.resize(schemaColumnCount);

    for (int index : columnFilter.GetIndexes()) {
        if (index < 0 || index >= schemaColumnCount) {
            THROW_ERROR_EXCEPTION("Column filter contains invalid index: actual %v, expected in range [0, %v]",
                index,
                schemaColumnCount - 1);
        }
        if (flags[index]) {
            THROW_ERROR_EXCEPTION("Column filter contains duplicate index %v",
                index);
        }
        flags[index] = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TColumnFilter& columnFilter)
{
    if (columnFilter.IsUniversal()) {
        return TString("{All}");
    } else {
        return Format("{%v}", columnFilter.GetIndexes());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
