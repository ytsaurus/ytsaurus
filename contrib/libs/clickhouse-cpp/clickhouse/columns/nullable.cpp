#include "nullable.h"

#include <assert.h>
#include <stdexcept>

namespace clickhouse {

ColumnNullable::ColumnNullable(ColumnRef nested, ColumnRef nulls)
    : Column(Type::CreateNullable(nested->Type()))
    , nested_(nested)
    , nulls_(nulls->As<ColumnUInt8>())
{
    if (nested_->Size() != nulls->Size()) {
        throw ValidationError("count of elements in nested and nulls should be the same");
    }
}

void ColumnNullable::Append(bool isnull)
{
    nulls_->Append(isnull ? 1 : 0);
}


bool ColumnNullable::IsNull(size_t n) const {
    return nulls_->At(n) != 0;
}

ColumnRef ColumnNullable::Nested() const {
    return nested_;
}

ColumnRef ColumnNullable::Nulls() const
{
       return nulls_;
}

void ColumnNullable::Reserve(size_t new_cap) {
    nested_->Reserve(new_cap);
    nulls_->Reserve(new_cap);
}

void ColumnNullable::Append(ColumnRef column) {
    if (auto col = column->As<ColumnNullable>()) {
        if (!col->nested_->Type()->IsEqual(nested_->Type())) {
            return;
        }

        nested_->Append(col->nested_);
        nulls_->Append(col->nulls_);
    }
}

void ColumnNullable::Clear() {
    nested_->Clear();
    nulls_->Clear();
}

bool ColumnNullable::LoadPrefix(InputStream* input, size_t rows) {
    return nested_->LoadPrefix(input, rows);
}

bool ColumnNullable::LoadBody(InputStream* input, size_t rows) {
    if (!nulls_->LoadBody(input, rows)) {
        return false;
    }
    if (!nested_->LoadBody(input, rows)) {
        return false;
    }
    return true;
}

void ColumnNullable::SavePrefix(OutputStream* output) {
    nested_->SavePrefix(output);
}

void ColumnNullable::SaveBody(OutputStream* output) {
    nulls_->SaveBody(output);
    nested_->SaveBody(output);
}

size_t ColumnNullable::Size() const {
    return nulls_->Size();
}

ColumnRef ColumnNullable::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnNullable>(nested_->Slice(begin, len), nulls_->Slice(begin, len));
}

ColumnRef ColumnNullable::CloneEmpty() const {
    return std::make_shared<ColumnNullable>(nested_->CloneEmpty(), nulls_->CloneEmpty());
}

void ColumnNullable::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnNullable &>(other);
    if (!nested_->Type()->IsEqual(col.nested_->Type()))
        throw ValidationError("Can't swap() Nullable columns of different types.");

    nested_.swap(col.nested_);
    nulls_.swap(col.nulls_);
}

ItemView ColumnNullable::GetItem(size_t index) const  {
    if (IsNull(index))
        return ItemView();

    return nested_->GetItem(index);
}

}
