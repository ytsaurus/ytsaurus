#include "decimal.h"

namespace
{
using namespace clickhouse;

#ifdef ABSL_HAVE_INTRINSIC_INT128
template <typename T>
inline bool addOverflow(const Int128 & l, const T & r, Int128 * result)
{
    __int128 res;
    const auto ret_value = __builtin_add_overflow(static_cast<__int128>(l), static_cast<__int128>(r), &res);

    *result = res;
    return ret_value;
}

template <typename T>
inline bool mulOverflow(const Int128 & l, const T & r, Int128 * result)
{
    __int128 res;
    const auto ret_value = __builtin_mul_overflow(static_cast<__int128>(l), static_cast<__int128>(r), &res);

    *result = res;
    return ret_value;
}

#else
template <typename T>
inline bool getSignBit(const T & v)
{
    return v < static_cast<T>(0);
}

inline bool getSignBit(const Int128 & v)
{
//    static constexpr Int128 zero {};
//    return v < zero;

    // Sign of the whole absl::int128 value is determined by sign of higher 64 bits.
    return absl::Int128High64(v) < 0;
}

inline bool addOverflow(const Int128 & l, const Int128 & r, Int128 * result)
{
    //    *result = l + r;
    //    const auto result_sign = getSignBit(*result);
    //    return l_sign == r_sign && l_sign != result_sign;

    // Based on code from:
    // https://wiki.sei.cmu.edu/confluence/display/c/INT32-C.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow#INT32C.Ensurethatoperationsonsignedintegersdonotresultinoverflow-CompliantSolution
    const auto r_positive = !getSignBit(r);

    if ((r_positive && (l > (std::numeric_limits<Int128>::max() - r))) ||
        (!r_positive && (l < (std::numeric_limits<Int128>::min() - r)))) {
        return true;
    }
    *result = l + r;

    return false;
}

template <typename T>
inline bool mulOverflow(const Int128 & l, const T & r, Int128 * result)
{
    // Based on code from:
    // https://wiki.sei.cmu.edu/confluence/display/c/INT32-C.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow#INT32C.Ensurethatoperationsonsignedintegersdonotresultinoverflow-CompliantSolution.3
    const auto l_positive = !getSignBit(l);
    const auto r_positive = !getSignBit(r);

    if (l_positive) {
        if (r_positive) {
            if (r != 0 && l > (std::numeric_limits<Int128>::max() / r)) {
                return true;
            }
        } else {
            if (l != 0 && r < (std::numeric_limits<Int128>::min() / l)) {
                return true;
            }
        }
    } else {
        if (r_positive) {
            if (r != 0 && l < (std::numeric_limits<Int128>::min() / r)) {
                return true;
            }
        } else {
            if (l != 0 && (r < (std::numeric_limits<Int128>::max() / l))) {
                return true;
            }
        }
    }

    *result = l * r;
    return false;
}
#endif

}

namespace clickhouse {

ColumnDecimal::ColumnDecimal(size_t precision, size_t scale)
    : Column(Type::CreateDecimal(precision, scale))
{
    if (precision <= 9) {
        data_ = std::make_shared<ColumnInt32>();
    } else if (precision <= 18) {
        data_ = std::make_shared<ColumnInt64>();
    } else {
        data_ = std::make_shared<ColumnInt128>();
    }
    data_type_code_ = data_->Type()->GetCode();
}

ColumnDecimal::ColumnDecimal(TypeRef type, ColumnRef data)
    : Column(type),
      data_(data),
      data_type_code_(data_->Type()->GetCode())
{
}

void ColumnDecimal::Append(const Int128& value) {
    switch (data_type_code_) {
        case Type::Int32:
            static_cast<ColumnInt32*>(data_.get())->Append(static_cast<int32_t>(value));
            break;
        case Type::Int64:
            static_cast<ColumnInt64*>(data_.get())->Append(static_cast<int64_t>(value));
            break;
        default:
            static_cast<ColumnInt128*>(data_.get())->Append(static_cast<Int128>(value));
            break;
    }
}

void ColumnDecimal::Append(const std::string& value) {
    Int128 int_value = 0;
    auto c = value.begin();
    auto end = value.end();
    bool sign = true;
    bool has_dot = false;

    size_t zeros = 0;

    while (c != end) {
        if (*c == '-') {
            sign = false;
            if (c != value.begin()) {
                break;
            }
        } else if (*c == '.' && !has_dot) {
            size_t distance = std::distance(c, end) - 1;
            auto scale = type_->As<DecimalType>()->GetScale();

            if (distance <= scale) {
                zeros = scale - distance;
            } else {
                std::advance(end, scale - distance);
            }

            has_dot = true;
        } else if (*c >= '0' && *c <= '9') {
            if (mulOverflow(int_value, 10, &int_value) ||
                addOverflow(int_value, *c - '0', &int_value)) {
                throw AssertionError("value is too big for 128-bit integer");
            }
        } else {
            throw ValidationError(std::string("unexpected symbol '") + (*c) + "' in decimal value");
        }
        ++c;
    }

    if (c != end) {
        throw ValidationError("unexpected symbol '-' in decimal value");
    }

    while (zeros) {
        if (mulOverflow(int_value, 10, &int_value)) {
            throw AssertionError("value is too big for 128-bit integer");
        }
        --zeros;
    }

    Append(sign ? int_value : -int_value);
}

Int128 ColumnDecimal::At(size_t i) const {
    switch (data_type_code_) {
        case Type::Int32:
            return static_cast<Int128>(static_cast<const ColumnInt32*>(data_.get())->At(i));
        case Type::Int64:
            return static_cast<Int128>(static_cast<const ColumnInt64*>(data_.get())->At(i));
        case Type::Int128:
            return static_cast<const ColumnInt128*>(data_.get())->At(i);
        default:
            throw ValidationError("Invalid data_ column type in ColumnDecimal");
    }
}

void ColumnDecimal::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

void ColumnDecimal::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDecimal>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDecimal::LoadBody(InputStream * input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnDecimal::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

void ColumnDecimal::Clear() {
    data_->Clear();
}

size_t ColumnDecimal::Size() const {
    return data_->Size();
}

ColumnRef ColumnDecimal::Slice(size_t begin, size_t len) const {
    // coundn't use std::make_shared since this c-tor is private
    return ColumnRef{new ColumnDecimal(type_, data_->Slice(begin, len))};
}

ColumnRef ColumnDecimal::CloneEmpty() const {
    // coundn't use std::make_shared since this c-tor is private
    return ColumnRef{new ColumnDecimal(type_, data_->CloneEmpty())};
}

void ColumnDecimal::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDecimal &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDecimal::GetItem(size_t index) const {
    return ItemView{GetType().GetCode(), data_->GetItem(index)};
}

size_t ColumnDecimal::GetScale() const
{
    return type_->As<DecimalType>()->GetScale();
}

size_t ColumnDecimal::GetPrecision() const
{
    return type_->As<DecimalType>()->GetPrecision();
}

}
