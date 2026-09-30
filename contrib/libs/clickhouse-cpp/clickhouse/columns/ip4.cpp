#include "ip4.h"

#include "../base/socket.h" // for platform-specific IPv4-related functions
#include <stdexcept>

namespace clickhouse {

ColumnIPv4::ColumnIPv4()
    : Column(Type::CreateIPv4())
    , data_(std::make_shared<ColumnUInt32>())
{
}

ColumnIPv4::ColumnIPv4(ColumnRef data)
    : Column(Type::CreateIPv4())
    , data_(data ? data->As<ColumnUInt32>() : nullptr)
{
    if (!data_)
        throw ValidationError("Expecting ColumnUInt32, got " + (data ? data->GetType().GetName() : "null"));
}

ColumnIPv4::ColumnIPv4(std::vector<uint32_t>&& data)
    : Column(Type::CreateIPv4())
{
    for (auto& addr : data) {
        addr = htonl(addr);
    }
    data_ = std::make_shared<ColumnUInt32>(std::move(data));
}

void ColumnIPv4::Append(const std::string& str) {
    uint32_t address;
    if (inet_pton(AF_INET, str.c_str(), &address) != 1)
        throw ValidationError("invalid IPv4 format, ip: " + str);
    data_->Append(htonl(address));
}

void ColumnIPv4::Append(uint32_t ip) {
    data_->Append(htonl(ip));
}

void ColumnIPv4::Append(in_addr ip) {
    data_->Append(htonl(ip.s_addr));
}

void ColumnIPv4::Clear() {
    data_->Clear();
}

in_addr ColumnIPv4::At(size_t n) const {
    in_addr addr;
    addr.s_addr = ntohl(data_->At(n));
    return addr;
}

in_addr ColumnIPv4::operator [] (size_t n) const {
    in_addr addr;
    addr.s_addr = ntohl(data_->operator[](n));
    return addr;
}

std::string ColumnIPv4::AsString(size_t n) const {
    const auto& addr = this->At(n);
    auto tmp_addr = addr;

    char buf[INET_ADDRSTRLEN];
    const char* ip_str = inet_ntop(AF_INET, &tmp_addr, buf, INET_ADDRSTRLEN);

    if (ip_str == nullptr) {
        throw std::system_error(
                std::error_code(errno, std::generic_category()),
                "Invalid IPv4 data");
    }

    return ip_str;
}

void ColumnIPv4::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

void ColumnIPv4::Append(ColumnRef column) {
    if (auto col = column->As<ColumnIPv4>()) {
        data_->Append(col->data_);
    }
}

bool ColumnIPv4::LoadBody(InputStream * input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnIPv4::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

size_t ColumnIPv4::Size() const {
    return data_->Size();
}

ColumnRef ColumnIPv4::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnIPv4>(data_->Slice(begin, len));
}

ColumnRef ColumnIPv4::CloneEmpty() const {
    return std::make_shared<ColumnIPv4>(data_->CloneEmpty());
}

void ColumnIPv4::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnIPv4 &>(other);
    data_.swap(col.data_);
}

ItemView ColumnIPv4::GetItem(size_t index) const {
    return ItemView(Type::IPv4, data_->GetItem(index));
}

}
