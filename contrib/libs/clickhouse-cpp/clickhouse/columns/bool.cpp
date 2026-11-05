#include "bool.h"

#include "../types/types.h"

namespace clickhouse {

ColumnBool::ColumnBool()
    : Column(Type::CreateSimple<bool>())
    , data_()
{
}

ColumnBool::ColumnBool(std::vector<uint8_t> data)
    : Column(Type::CreateSimple<bool>())
    , data_(std::move(data))
{
}

void ColumnBool::Reserve(size_t new_cap) {
    data_.Reserve(new_cap);
}

size_t ColumnBool::Capacity() const {
    return data_.Capacity();
}

void ColumnBool::Append(bool value) {
    data_.Append(static_cast<uint8_t>(value));
}

bool ColumnBool::At(size_t n) const {
    return static_cast<bool>(data_.At(n));
}

void ColumnBool::Append(ColumnRef column) {
    if (auto col = column->As<ColumnBool>()) {
        auto& src = col->data_.GetWritableData();
        data_.GetWritableData().insert(data_.GetWritableData().end(), src.begin(), src.end());
    } else if (auto col = column->As<ColumnUInt8>()) {
        auto& src = col->GetWritableData();
        data_.GetWritableData().insert(data_.GetWritableData().end(), src.begin(), src.end());
    }
}

bool ColumnBool::LoadBody(InputStream* input, size_t rows) {
    return data_.LoadBody(input, rows);
}

void ColumnBool::SaveBody(OutputStream* output) {
    data_.SaveBody(output);
}

void ColumnBool::Clear() {
    data_.Clear();
}

size_t ColumnBool::Size() const {
    return data_.Size();
}

ColumnRef ColumnBool::Slice(size_t begin, size_t len) const {
    auto sliced = std::static_pointer_cast<ColumnUInt8>(data_.Slice(begin, len));
    return std::make_shared<ColumnBool>(std::move(sliced->GetWritableData()));
}

ColumnRef ColumnBool::CloneEmpty() const {
    return std::make_shared<ColumnBool>();
}

void ColumnBool::Swap(Column& other) {
    auto& col = dynamic_cast<ColumnBool&>(other);
    data_.Swap(col.data_);
}

ItemView ColumnBool::GetItem(size_t index) const {
    return ItemView{Type::Bool, data_.At(index)};
}

}  // namespace clickhouse
