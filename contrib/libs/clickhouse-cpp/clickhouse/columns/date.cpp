#include "date.h"
#include <cstdint>

namespace clickhouse {

ColumnDate::ColumnDate()
    : Column(Type::CreateDate())
    , data_(std::make_shared<ColumnUInt16>())
{
}

ColumnDate::ColumnDate(std::vector<uint16_t>&& data)
    : Column(Type::CreateDate())
    , data_(std::make_shared<ColumnUInt16>(std::move(data)))
{
}

void ColumnDate::Append(const std::time_t& value) {
    /// The implementation is fundamentally wrong, ignores timezones, leap years and daylight saving.
    data_->Append(static_cast<uint16_t>(value / std::time_t(86400)));
}

void ColumnDate::Clear() {
    data_->Clear();
}

std::time_t ColumnDate::At(size_t n) const {
    /// The implementation is fundamentally wrong, ignores timezones, leap years and daylight saving.
    return static_cast<std::time_t>(data_->At(n)) * 86400;
}

void ColumnDate::AppendRaw(uint16_t value) {
    data_->Append(value);
}

uint16_t ColumnDate::RawAt(size_t n) const {
    return data_->At(n);
}

void ColumnDate::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDate>()) {
        data_->Append(col->data_);
    }
}

std::vector<uint16_t>& ColumnDate::GetWritableData() {
    return data_->GetWritableData();
}

void ColumnDate::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

size_t ColumnDate::Capacity() const {
    return data_->Capacity();
}

bool ColumnDate::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnDate::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

size_t ColumnDate::Size() const {
    return data_->Size();
}

ColumnRef ColumnDate::Slice(size_t begin, size_t len) const {
    auto col = data_->Slice(begin, len)->As<ColumnUInt16>();
    auto result = std::make_shared<ColumnDate>();

    result->data_->Append(col);

    return result;
}

ColumnRef ColumnDate::CloneEmpty() const {
    return std::make_shared<ColumnDate>();
}

void ColumnDate::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDate &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDate::GetItem(size_t index) const {
    return ItemView(Type::Date, data_->GetItem(index));
}


ColumnDate32::ColumnDate32()
    : Column(Type::CreateDate32())
    , data_(std::make_shared<ColumnInt32>())
{
}

ColumnDate32::ColumnDate32(std::vector<int32_t>&& data)
    : Column(Type::CreateDate32())
    , data_(std::make_shared<ColumnInt32>(std::move(data)))
{
}

void ColumnDate32::Append(const std::time_t& value) {
    /// The implementation is fundamentally wrong, ignores timezones, leap years and daylight saving.
    data_->Append(static_cast<int32_t>(value / std::time_t(86400)));
}

void ColumnDate32::Clear() {
    data_->Clear();
}

std::time_t ColumnDate32::At(size_t n) const {
    /// The implementation is fundamentally wrong, ignores timezones, leap years and daylight saving.
    return static_cast<std::time_t>(data_->At(n)) * 86400;
}

void ColumnDate32::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDate32>()) {
        data_->Append(col->data_);
    }
}

std::vector<int32_t>& ColumnDate32::GetWritableData() {
    return data_->GetWritableData();
}

void ColumnDate32::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

size_t ColumnDate32::Capacity() const {
    return data_->Capacity();
}

void ColumnDate32::AppendRaw(int32_t value) {
    data_->Append(value);
}

int32_t ColumnDate32::RawAt(size_t n) const {
    return data_->At(n);
}

bool ColumnDate32::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnDate32::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

size_t ColumnDate32::Size() const {
    return data_->Size();
}

ColumnRef ColumnDate32::Slice(size_t begin, size_t len) const {
    auto col = data_->Slice(begin, len)->As<ColumnInt32>();
    auto result = std::make_shared<ColumnDate32>();

    result->data_->Append(col);

    return result;
}

ColumnRef ColumnDate32::CloneEmpty() const {
    return std::make_shared<ColumnDate32>();
}

void ColumnDate32::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDate32 &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDate32::GetItem(size_t index) const {
    return ItemView{Type()->GetCode(), data_->GetItem(index)};
}

ColumnDateTime::ColumnDateTime()
    : Column(Type::CreateDateTime())
    , data_(std::make_shared<ColumnUInt32>())
{
}

ColumnDateTime::ColumnDateTime(std::string timezone)
    : Column(Type::CreateDateTime(std::move(timezone)))
    , data_(std::make_shared<ColumnUInt32>())
{
}

ColumnDateTime::ColumnDateTime(std::vector<uint32_t>&& data)
    : Column(Type::CreateDateTime())
    , data_(std::make_shared<ColumnUInt32>(std::move(data))) {
}

ColumnDateTime::ColumnDateTime(std::string timezone, std::vector<uint32_t>&& data)
    : Column(Type::CreateDateTime(std::move(timezone)))
    , data_(std::make_shared<ColumnUInt32>(std::move(data))) {
}

void ColumnDateTime::Append(const std::time_t& value) {
    data_->Append(static_cast<uint32_t>(value));
}

std::time_t ColumnDateTime::At(size_t n) const {
    return data_->At(n);
}

void ColumnDateTime::AppendRaw(uint32_t value) {
    data_->Append(value);
}

uint32_t ColumnDateTime::RawAt(size_t n) const {
	return data_->At(n);
}

std::string ColumnDateTime::Timezone() const {
    return type_->As<DateTimeType>()->Timezone();
}

void ColumnDateTime::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDateTime>()) {
        data_->Append(col->data_);
    }
}

std::vector<uint32_t>& ColumnDateTime::GetWritableData() {
    return data_->GetWritableData();
}

void ColumnDateTime::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

size_t ColumnDateTime::Capacity() const {
    return data_->Capacity();
}

bool ColumnDateTime::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnDateTime::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

size_t ColumnDateTime::Size() const {
    return data_->Size();
}

void ColumnDateTime::Clear() {
    data_->Clear();
}

ColumnRef ColumnDateTime::Slice(size_t begin, size_t len) const {
    auto col = data_->Slice(begin, len)->As<ColumnUInt32>();
    auto result = std::make_shared<ColumnDateTime>();

    result->data_->Append(col);

    return result;
}

ColumnRef ColumnDateTime::CloneEmpty() const {
    return std::make_shared<ColumnDateTime>();
}

void ColumnDateTime::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDateTime &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDateTime::GetItem(size_t index) const {
    return ItemView(Type::DateTime, data_->GetItem(index));
}

ColumnDateTime64::ColumnDateTime64(size_t precision)
    : ColumnDateTime64(Type::CreateDateTime64(precision), std::make_shared<ColumnDecimal>(18ul, precision))
{}

ColumnDateTime64::ColumnDateTime64(size_t precision, std::string timezone)
    : ColumnDateTime64(Type::CreateDateTime64(precision, std::move(timezone)), std::make_shared<ColumnDecimal>(18ul, precision))
{}

ColumnDateTime64::ColumnDateTime64(TypeRef type, std::shared_ptr<ColumnDecimal> data)
    : Column(type),
      data_(data),
      precision_(type->As<DateTime64Type>()->GetPrecision())
{}

void ColumnDateTime64::Append(const Int64& value) {
    // TODO: we need a type, which safely represents datetime.
    // The precision of Poco.DateTime is not big enough.
    data_->Append(value);
}

//void ColumnDateTime64::Append(const std::string& value) {
//    data_->Append(value);
//}

Int64 ColumnDateTime64::At(size_t n) const {
    // make sure to use Absl's Int128 conversion
    return static_cast<Int64>(data_->At(n));
}

std::string ColumnDateTime64::Timezone() const {
    return type_->As<DateTime64Type>()->Timezone();
}

void ColumnDateTime64::Reserve(size_t new_cap)
{
    data_->Reserve(new_cap);
}

void ColumnDateTime64::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDateTime64>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDateTime64::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnDateTime64::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

void ColumnDateTime64::Clear() {
    data_->Clear();
}

size_t ColumnDateTime64::Size() const {
    return data_->Size();
}

ItemView ColumnDateTime64::GetItem(size_t index) const {
    return ItemView(Type::DateTime64, data_->GetItem(index));
}

void ColumnDateTime64::Swap(Column& other) {
    auto& col = dynamic_cast<ColumnDateTime64&>(other);
    if (col.GetPrecision() != GetPrecision()) {
        throw ValidationError("Can't swap DateTime64 columns when precisions are not the same: "
                + std::to_string(GetPrecision()) + "(this) != " + std::to_string(col.GetPrecision()) + "(that)");
    }

    data_.swap(col.data_);
}

ColumnRef ColumnDateTime64::Slice(size_t begin, size_t len) const {
    auto sliced_data = data_->Slice(begin, len)->As<ColumnDecimal>();

    return ColumnRef{new ColumnDateTime64(type_, sliced_data)};
}

ColumnRef ColumnDateTime64::CloneEmpty() const {
    return ColumnRef{new ColumnDateTime64(type_, data_->CloneEmpty()->As<ColumnDecimal>())};
}

size_t ColumnDateTime64::GetPrecision() const {
    return precision_;
}

}
