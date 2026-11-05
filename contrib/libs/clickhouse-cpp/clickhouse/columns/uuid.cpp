#include "uuid.h"
#include "utils.h"
#include "../exceptions.h"

#include <stdexcept>

namespace clickhouse {

ColumnUUID::ColumnUUID()
    : Column(Type::CreateUUID())
    , data_(std::make_shared<ColumnUInt64>())
{
}

ColumnUUID::ColumnUUID(ColumnRef data)
    : Column(Type::CreateUUID())
    , data_(data->As<ColumnUInt64>())
{
    if (data_->Size() % 2 != 0) {
        throw ValidationError("number of entries must be even (two 64-bit numbers for each UUID)");
    }
}

void ColumnUUID::Append(const UUID& value) {
    data_->Append(value.first);
    data_->Append(value.second);
}

void ColumnUUID::Clear() {
    data_->Clear();
}

const UUID ColumnUUID::At(size_t n) const {
    return UUID(data_->At(n * 2), data_->At(n * 2 + 1));
}

void ColumnUUID::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

void ColumnUUID::Append(ColumnRef column) {
    if (auto col = column->As<ColumnUUID>()) {
        data_->Append(col->data_);
    }
}

bool ColumnUUID::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows * 2);
}

void ColumnUUID::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

size_t ColumnUUID::Size() const {
    return data_->Size() / 2;
}

ColumnRef ColumnUUID::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnUUID>(data_->Slice(begin * 2, len * 2));
}

ColumnRef ColumnUUID::CloneEmpty() const {
    return std::make_shared<ColumnUUID>();
}

void ColumnUUID::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnUUID &>(other);
    data_.swap(col.data_);
}

ItemView ColumnUUID::GetItem(size_t index) const {
    // We know that ColumnUInt64 stores it's data in continius memory region,
    // and that every 2 values from data represent 1 UUID value.
    const auto data_item_view = data_->GetItem(index * 2);

    return ItemView{Type::UUID, std::string_view{data_item_view.data.data(), data_item_view.data.size() * 2}};
}

}
