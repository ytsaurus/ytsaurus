#include "time.h"

namespace clickhouse {

ColumnTime::ColumnTime() 
    : ColumnTime(Type::CreateTime(), std::make_shared<ColumnInt32>()) {}

ColumnTime::ColumnTime(std::vector<int32_t>&& data)
    : ColumnTime(Type::CreateTime(), std::make_shared<ColumnInt32>(std::move(data))) {}

ColumnTime::ColumnTime(TypeRef type, std::shared_ptr<ColumnInt32> data)
    : Column(std::move(type)),
      data_(std::move(data))
{}

void ColumnTime::Append(ValueType value) {
    data_->Append(value);
}

ColumnTime::ValueType ColumnTime::At(size_t n) const {
    return data_->At(n);
}

std::vector<int32_t>& ColumnTime::GetWritableData() {
    return data_->GetWritableData();
}

void ColumnTime::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

size_t ColumnTime::Capacity() const {
    return data_->Capacity();
}

void ColumnTime::Append(ColumnRef column) {
    if (auto col = column->As<ColumnTime>()) {
        data_->Append(col->data_);
    }
}

bool ColumnTime::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnTime::Clear() {
    data_->Clear();
}

void ColumnTime::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

size_t ColumnTime::Size() const {
    return data_->Size();
}

ColumnRef ColumnTime::Slice(size_t begin, size_t len) const {
    auto sliced_data = data_->Slice(begin, len)->As<ColumnInt32>();
    return ColumnRef{new ColumnTime(type_, sliced_data)};
}

ColumnRef ColumnTime::CloneEmpty() const {
    return ColumnRef{new ColumnTime(type_, data_->CloneEmpty()->As<ColumnInt32>())};
}

void ColumnTime::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnTime &>(other);
    data_.swap(col.data_);
}

ItemView ColumnTime::GetItem(size_t index) const {
    return ItemView{Type::Time, data_->GetItem(index)};
}

ColumnTime64::ColumnTime64(size_t precision) 
    : ColumnTime64(Type::CreateTime64(precision), std::make_shared<ColumnInt64>())
{}

ColumnTime64::ColumnTime64(size_t precision, std::vector<int64_t>&& data)
    : ColumnTime64(Type::CreateTime64(precision), std::make_shared<ColumnInt64>(std::move(data)))
{}

ColumnTime64::ColumnTime64(TypeRef type, std::shared_ptr<ColumnInt64> data)
    : Column(std::move(type)),
      data_(std::move(data)),
      precision_{type_->As<Time64Type>()->GetPrecision()}
{}

void ColumnTime64::Append(ValueType value) {
    data_->Append(value);
}

ColumnTime64::ValueType ColumnTime64::At(size_t n) const {
    return data_->At(n);
}

std::vector<int64_t>& ColumnTime64::GetWritableData() {
    return data_->GetWritableData();
}

void ColumnTime64::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

size_t ColumnTime64::Capacity() const {
    return data_->Capacity();
}

void ColumnTime64::Append(ColumnRef column) {
    if (auto col = column->As<ColumnTime64>()) {
        data_->Append(col->data_);
    }
}

bool ColumnTime64::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnTime64::Clear() {
    data_->Clear();
}

void ColumnTime64::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

size_t ColumnTime64::Size() const {
    return data_->Size();
}

ColumnRef ColumnTime64::Slice(size_t begin, size_t len) const {
    auto sliced_data = data_->Slice(begin, len)->As<ColumnInt64>();
    return ColumnRef{new ColumnTime64(type_, sliced_data)};
}

ColumnRef ColumnTime64::CloneEmpty() const {
    return ColumnRef{new ColumnTime64(type_, data_->CloneEmpty()->As<ColumnInt64>())};
}

void ColumnTime64::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnTime64 &>(other);
    if (col.GetPrecision() != GetPrecision()) {
        throw ValidationError("Can't swap Time64 columns when precisions are not the same: "
            + std::to_string(GetPrecision()) + "(this) != "
            + std::to_string(col.GetPrecision()) + "(that)");
    }
    data_.swap(col.data_);
}

ItemView ColumnTime64::GetItem(size_t index) const {
    return ItemView{Type::Time64, data_->GetItem(index)};
}

}  // namespace clickhouse
