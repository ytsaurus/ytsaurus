#include "json.h"
#include "../base/wire_format.h"

namespace clickhouse {

enum class JSONSerializationVersion : uint64_t {
  // String is the only currently supported serialization of JSON.
  // it should be enabled with output_format_native_write_json_as_string=1
  String = 1,
};

ColumnJSON::ColumnJSON()
    : Column(Type::CreateJSON())
    , data_(std::make_shared<ColumnString>())
{}

ColumnJSON::ColumnJSON(std::vector<std::string> data)
    : Column(Type::CreateJSON())
    , data_(std::make_shared<ColumnString>(std::move(data)))
{}

void ColumnJSON::Append(std::string_view str) {
    data_->Append(str);
}

void ColumnJSON::Append(const char* str) {
    data_->Append(str);
}
void ColumnJSON::Append(std::string&& str) {
    data_->Append(std::move(str));
}

std::string_view ColumnJSON::At(size_t n) const {
    return data_->At(n);
}

void ColumnJSON::Append(ColumnRef column) {
    if (auto col = column->As<ColumnJSON>()) {
        data_->Append(col->data_);
    }
}

void ColumnJSON::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

bool ColumnJSON::LoadPrefix(InputStream* input, size_t) {
    uint64_t v;
    if (!WireFormat::ReadFixed(*input, &v)) {
        return false;
    }
    if (v != static_cast<uint64_t>(JSONSerializationVersion::String)) {
        // Hard stop: the library can only parse JSON when `output_format_native_write_json_as_string` is enabled.
        // Further processing is meaningless after this error and the user must be notified immediately.
        throw ProtocolError("Unsupported JSON serialization version. "
                            "Make sure output_format_native_write_json_as_string=1 is set.");
    }
    return true;
}

bool ColumnJSON::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnJSON::SavePrefix(OutputStream* output) {
    WireFormat::WriteFixed(*output, static_cast<uint64_t>(JSONSerializationVersion::String));
}

void ColumnJSON::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

void ColumnJSON::Clear() {
    data_->Clear();
}

size_t ColumnJSON::Size() const {
    return data_->Size();
}

ColumnRef ColumnJSON::Slice(size_t begin, size_t len) const {
    auto ret = std::make_shared<ColumnJSON>();
    auto sliced_data = data_->Slice(begin, len)->As<ColumnString>();
    ret->data_->Swap(*sliced_data);
    return ret;
}

ColumnRef ColumnJSON::CloneEmpty() const
{
    return std::make_shared<ColumnJSON>();
}

void ColumnJSON::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnJSON &>(other);
    data_.swap(col.data_);
}

ItemView ColumnJSON::GetItem(size_t index) const {
    return ItemView{Type::JSON, data_->GetItem(index)};
}

}
