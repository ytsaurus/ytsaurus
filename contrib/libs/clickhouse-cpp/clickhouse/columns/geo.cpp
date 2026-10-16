#include "geo.h"

#include "utils.h"

namespace {
using namespace ::clickhouse;

template <Type::Code type_code>
TypeRef CreateGeoType() {
    if constexpr (type_code == Type::Code::Point) {
        return Type::CreatePoint();
    } else if constexpr (type_code == Type::Code::Ring) {
        return Type::CreateRing();
    } else if constexpr (type_code == Type::Code::Polygon) {
        return Type::CreatePolygon();
    } else if constexpr (type_code == Type::Code::MultiPolygon) {
        return Type::CreateMultiPolygon();
    }
}

template <typename ColumnType>
std::shared_ptr<ColumnType> CreateColumn() {
    if constexpr (std::is_same_v<ColumnType, ColumnTupleT<ColumnFloat64, ColumnFloat64>>) {
        return std::make_shared<ColumnTupleT<ColumnFloat64, ColumnFloat64>>(
            std::make_tuple(std::make_shared<ColumnFloat64>(), std::make_shared<ColumnFloat64>()));
    } else {
        return std::make_shared<ColumnType>();
    }
}

}  // namespace

namespace clickhouse {

template <typename NestedColumnType, Type::Code type_code>
ColumnGeo<NestedColumnType, type_code>::ColumnGeo()
    : Column(CreateGeoType<type_code>()),
      data_(CreateColumn<NestedColumnType>()) {
}

template <typename NestedColumnType, Type::Code type_code>
ColumnGeo<NestedColumnType, type_code>::ColumnGeo(ColumnRef data)
    : Column(CreateGeoType<type_code>())
    , data_(WrapColumn<NestedColumnType>(std::move(data))) {
}

template <typename NestedColumnType, Type::Code type_code>
void ColumnGeo<NestedColumnType, type_code>::Clear() {
    data_->Clear();
}

template <typename NestedColumnType, Type::Code type_code>
const typename ColumnGeo<NestedColumnType, type_code>::ValueType ColumnGeo<NestedColumnType, type_code>::At(size_t n) const {
    return data_->At(n);
}

template<typename NestedColumnType, Type::Code type_code>
void ColumnGeo<NestedColumnType, type_code>::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

template <typename NestedColumnType, Type::Code type_code>
void ColumnGeo<NestedColumnType, type_code>::Append(ColumnRef column) {
    if (auto col = column->template As<ColumnGeo>()) {
        data_->Append(col->data_->template As<Column>());
    }
}

template <typename NestedColumnType, Type::Code type_code>
bool ColumnGeo<NestedColumnType, type_code>::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

template <typename NestedColumnType, Type::Code type_code>
void ColumnGeo<NestedColumnType, type_code>::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

template <typename NestedColumnType, Type::Code type_code>
size_t ColumnGeo<NestedColumnType, type_code>::Size() const {
    return data_->Size();
}

template <typename NestedColumnType, Type::Code type_code>
ColumnRef ColumnGeo<NestedColumnType, type_code>::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnGeo>(data_->Slice(begin, len));
}

template <typename NestedColumnType, Type::Code type_code>
ColumnRef ColumnGeo<NestedColumnType, type_code>::CloneEmpty() const {
    return std::make_shared<ColumnGeo>();
}

template <typename NestedColumnType, Type::Code type_code>
void ColumnGeo<NestedColumnType, type_code>::Swap(Column& other) {
    auto& col = dynamic_cast<ColumnGeo&>(other);
    data_.swap(col.data_);
}

template class ColumnGeo<ColumnTupleT<ColumnFloat64, ColumnFloat64>, Type::Code::Point>;

template class ColumnGeo<ColumnArrayT<ColumnPoint>, Type::Code::Ring>;

template class ColumnGeo<ColumnArrayT<ColumnRing>, Type::Code::Polygon>;

template class ColumnGeo<ColumnArrayT<ColumnPolygon>, Type::Code::MultiPolygon>;

}  // namespace clickhouse
