#include "ip6.h"
#include "../base/socket.h" // for IPv6 platform-specific stuff
#include "../exceptions.h"

#include <stdexcept>

namespace clickhouse {

static_assert(sizeof(struct in6_addr) == 16, "sizeof in6_addr should be 16 bytes");

ColumnIPv6::ColumnIPv6()
    : Column(Type::CreateIPv6())
    , data_(std::make_shared<ColumnFixedString>(16))
{
}

ColumnIPv6::ColumnIPv6(ColumnRef data)
    : Column(Type::CreateIPv6())
    , data_(data ? data->As<ColumnFixedString>() : nullptr)
{
    if (!data_ || data_->FixedSize() != sizeof(in6_addr))
        throw ValidationError("Expecting ColumnFixedString(16), got " + (data ? data->GetType().GetName() : "null"));
}

void ColumnIPv6::Append(const std::string_view& str) {
    unsigned char buf[16];
    if (inet_pton(AF_INET6, str.data(), buf) != 1) {
        throw ValidationError("invalid IPv6 format, ip: " + std::string(str));
    }
    data_->Append(std::string_view((const char*)buf, 16));
}

void ColumnIPv6::Append(const in6_addr* addr) {
    data_->Append(std::string_view((const char*)addr->s6_addr, 16));
}

void ColumnIPv6::Append(const in6_addr& addr) {
    Append(&addr);
}

void ColumnIPv6::Clear() {
    data_->Clear();
}

std::string ColumnIPv6::AsString (size_t n) const {
    const auto& addr = this->At(n);
    auto tmp_addr = addr;

    char buf[INET6_ADDRSTRLEN];
    const char* ip_str = inet_ntop(AF_INET6, &tmp_addr, buf, INET6_ADDRSTRLEN);

    if (ip_str == nullptr) {
        throw std::system_error(
                std::error_code(errno, std::generic_category()),
                "Invalid IPv6 data");
    }

    return ip_str;
}

in6_addr ColumnIPv6::At(size_t n) const {
    return *reinterpret_cast<const in6_addr*>(data_->At(n).data());
}

in6_addr ColumnIPv6::operator [] (size_t n) const {
    return *reinterpret_cast<const in6_addr*>(data_->At(n).data());
}

void ColumnIPv6::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

void ColumnIPv6::Append(ColumnRef column) {
    if (auto col = column->As<ColumnIPv6>()) {
        data_->Append(col->data_);
    }
}

bool ColumnIPv6::LoadBody(InputStream* input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnIPv6::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

size_t ColumnIPv6::Size() const {
    return data_->Size();
}

ColumnRef ColumnIPv6::Slice(size_t begin, size_t len) const {
    return std::make_shared<ColumnIPv6>(data_->Slice(begin, len));
}

ColumnRef ColumnIPv6::CloneEmpty() const {
    return std::make_shared<ColumnIPv6>(data_->CloneEmpty());
}

void ColumnIPv6::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnIPv6 &>(other);
    data_.swap(col.data_);
}

ItemView ColumnIPv6::GetItem(size_t index) const {
    return ItemView{Type::IPv6, data_->GetItem(index)};
}

}
