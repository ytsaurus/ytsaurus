#include "numeric.h"
#include "utils.h"

#include "../base/wire_format.h"

namespace clickhouse {

template <typename T>
ColumnVector<T>::ColumnVector()
    : Column(Type::CreateSimple<T>())
{
}

template <typename T>
ColumnVector<T>::ColumnVector(const std::vector<T> & data)
    : Column(Type::CreateSimple<T>())
    , data_(data)
{
}

template <typename T>
ColumnVector<T>::ColumnVector(std::vector<T> && data)
    : Column(Type::CreateSimple<T>())
    , data_(std::move(data))
{
}

template <typename T>
void ColumnVector<T>::Append(const T& value) {
    data_.push_back(value);
}

template <typename T>
void ColumnVector<T>::Erase(size_t pos, size_t count) {
    const auto begin = std::min(pos, data_.size());
    const auto last = begin + std::min(data_.size() - begin, count);

    data_.erase(data_.begin() + begin, data_.begin() + last);
}

template <typename T>
std::vector<T>& ColumnVector<T>::GetWritableData() {
    return data_;
}

template <typename T>
void ColumnVector<T>::Reserve(size_t new_cap) {
    data_.reserve(new_cap);
}

template <typename T>
size_t ColumnVector<T>::Capacity() const {
    return data_.capacity();
}

template <typename T>
void ColumnVector<T>::Clear() {
    data_.clear();
}

template <typename T>
const T& ColumnVector<T>::At(size_t n) const {
    return data_.at(n);
}

template <typename T>
void ColumnVector<T>::Append(ColumnRef column) {
    if (auto col = column->As<ColumnVector<T>>()) {
        data_.insert(data_.end(), col->data_.begin(), col->data_.end());
    }
}

template <typename T>
bool ColumnVector<T>::LoadBody(InputStream* input, size_t rows) {
    data_.resize(rows);

    return WireFormat::ReadBytes(*input, data_.data(), data_.size() * sizeof(T));
}

template <typename T>
void ColumnVector<T>::SaveBody(OutputStream* output) {
    WireFormat::WriteBytes(*output, data_.data(), data_.size() * sizeof(T));
}

template <typename T>
size_t ColumnVector<T>::Size() const {
    return data_.size();
}

template <typename T>
ColumnRef ColumnVector<T>::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnVector<T>>(SliceVector(data_, begin, len));
}

template <typename T>
ColumnRef ColumnVector<T>::CloneEmpty() const {
    return std::make_shared<ColumnVector<T>>();
}

template <typename T>
void ColumnVector<T>::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnVector<T> &>(other);
    data_.swap(col.data_);
}

template <typename T>
ItemView ColumnVector<T>::GetItem(size_t index) const  {
    return ItemView{type_->GetCode(), data_[index]};
}

template class ColumnVector<int8_t>;
template class ColumnVector<int16_t>;
template class ColumnVector<int32_t>;
template class ColumnVector<int64_t>;

template class ColumnVector<uint8_t>;
template class ColumnVector<uint16_t>;
template class ColumnVector<uint32_t>;
template class ColumnVector<uint64_t>;
template class ColumnVector<Int128>;
template class ColumnVector<UInt128>;

template class ColumnVector<float>;
template class ColumnVector<double>;

}
