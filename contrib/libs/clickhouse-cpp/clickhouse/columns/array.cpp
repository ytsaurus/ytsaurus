#include "array.h"
#include "numeric.h"

#include <stdexcept>

namespace clickhouse {

namespace {
std::shared_ptr<ColumnUInt64> make_single_offset(size_t value) {
    auto res = std::make_shared<ColumnUInt64>();
    if (value != 0) {
        res->Append(value);
    }
    return res;
}
}  // namespace

ColumnArray::ColumnArray(ColumnRef data) : ColumnArray(data, make_single_offset(data->Size())) {
}

ColumnArray::ColumnArray(ColumnRef data, std::shared_ptr<ColumnUInt64> offsets)
    : Column(Type::CreateArray(data->Type()))
    , data_(data)
    , offsets_(offsets)
{
    const auto rows            = offsets_->Size();
    const auto expected_values = rows > 0 ? offsets_->At(rows - 1) : 0;
    std::uint64_t prev         = 0;
    // Ensure the offset array is internally consistent
    for (const auto& o : offsets_->GetWritableData()) {
        if (o < prev) {
            throw ValidationError("offsets must be monotonically increasing");
        }
        prev = o;
    }
    if (data_->Size() != expected_values) {
        throw ValidationError("Mismatch between data and offsets: Expected " + std::to_string(expected_values) +
                              " values in data, but got " + std::to_string(data_->Size()));
    }
}

ColumnArray::ColumnArray(ColumnArray&& other)
    : Column(other.Type())
    , data_(std::move(other.data_))
    , offsets_(std::move(other.offsets_))
{
}

void ColumnArray::AppendAsColumn(ColumnRef array) {
    // appending data may throw (i.e. due to ype check failure), so do it first to avoid partly modified state.
    data_->Append(array);
    AddOffset(array->Size());
}

ColumnRef ColumnArray::GetAsColumn(size_t n) const {
    if (n >= Size())
        throw ValidationError("Index is out ouf bounds: " + std::to_string(n));

    return data_->Slice(GetOffset(n), GetSize(n));
}

ColumnRef ColumnArray::Slice(size_t begin, size_t size) const {
    if (size && begin + size > Size())
        throw ValidationError("Slice indexes are out of bounds");

    auto sliced_data = data_->Slice(GetOffset(begin), GetOffset(begin + size) - GetOffset(begin));
    auto offsets     = std::make_shared<ColumnUInt64>();
    auto offset      = uint64_t{0};
    for (size_t i = 0; i < size; i++) {
        offset += GetSize(begin + i);
        offsets->Append(offset);
    }

    return std::make_shared<ColumnArray>(std::move(sliced_data), std::move(offsets));
}

ColumnRef ColumnArray::CloneEmpty() const {
    return std::make_shared<ColumnArray>(data_->CloneEmpty());
}

void ColumnArray::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
    offsets_->Reserve(new_cap);
}

void ColumnArray::Append(ColumnRef column) {
    if (auto col = column->As<ColumnArray>()) {
        for (size_t i = 0; i < col->Size(); ++i) {
            AppendAsColumn(col->GetAsColumn(i));
        }
    }
}

bool ColumnArray::LoadPrefix(InputStream* input, size_t rows) {
    if (!rows) {
        return true;
    }

    return data_->LoadPrefix(input, rows);
}

bool ColumnArray::LoadBody(InputStream* input, size_t rows) {
    if (!rows) {
        return true;
    }
    if (!offsets_->LoadBody(input, rows)) {
        return false;
    }
    const auto nested_rows = (*offsets_)[rows - 1];
    if (nested_rows == 0) {
        return true;
    }
    if (!data_->LoadBody(input, nested_rows)) {
        return false;
    }
    return true;
}

void ColumnArray::SavePrefix(OutputStream* output) {
    data_->SavePrefix(output);
}

void ColumnArray::SaveBody(OutputStream* output) {
    offsets_->SaveBody(output);
    if (data_->Size() > 0) {
        data_->SaveBody(output);
    }
}

void ColumnArray::Clear() {
    offsets_->Clear();
    data_->Clear();
}

size_t ColumnArray::Size() const {
    return offsets_->Size();
}

void ColumnArray::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnArray &>(other);
    data_.swap(col.data_);
    offsets_.swap(col.offsets_);
}

void ColumnArray::OffsetsIncrease(size_t n) {
    offsets_->Append(n);
}

size_t ColumnArray::GetOffset(size_t n) const {

    return (n == 0) ? 0 : (*offsets_)[n - 1];
}

void ColumnArray::AddOffset(size_t n) {
    if (offsets_->Size() == 0) {
        offsets_->Append(n);
    } else {
        offsets_->Append((*offsets_)[offsets_->Size() - 1] + n);
    }
}

size_t ColumnArray::GetSize(size_t n) const {
    return (n == 0) ? (*offsets_)[n] : ((*offsets_)[n] - (*offsets_)[n - 1]);
}

ColumnRef ColumnArray::GetData() {
    return data_;
}

std::shared_ptr<const Column> ColumnArray::GetData() const {
    return data_;
}

std::shared_ptr<ColumnUInt64>& ColumnArray::GetOffsets() {
    return offsets_;
}

std::shared_ptr<const ColumnUInt64> ColumnArray::GetOffsets() const {
    return offsets_;
}

void ColumnArray::Reset() {
    data_.reset();
    offsets_.reset();
}

}
