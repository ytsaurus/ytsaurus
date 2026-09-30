#include "../columns/itemview.h"

#include <algorithm>
#include <sstream>

namespace {

template <typename Container>
std::string ContainerToString(Container container, const char * separator = ", ") {
    std::stringstream sstr;
    const auto end = std::end(container);
    for (auto i = std::begin(container); i != end; /*intentionally no ++i*/) {
        const auto & elem = *i;
        sstr << elem;

        if (++i != end) {
            sstr << separator;
        }
    }

    return sstr.str();
}

}

namespace clickhouse {

void ItemView::ValidateData(Type::Code type, DataType data) {

    auto AssertSize = [type, &data](std::initializer_list<int> allowed_sizes) -> void {
        const auto end = std::end(allowed_sizes);
        if (std::find(std::begin(allowed_sizes), end, static_cast<int>(data.size())) == end) {
            throw AssertionError(std::string("ItemView value size mismatch for ")
                    + Type::TypeName(type)
                    + " expected: " + ContainerToString(allowed_sizes, " or ")
                    + ", got: " + std::to_string(data.size()));
        }
    };

    switch (type) {
        case Type::Code::Void:
            return AssertSize({0});

        case Type::Code::Int8:
        case Type::Code::UInt8:
        case Type::Code::Enum8:
        case Type::Code::Bool:
            return AssertSize({1});

        case Type::Code::Int16:
        case Type::Code::UInt16:
        case Type::Code::Date:
        case Type::Code::Enum16:
            return AssertSize({2});

        case Type::Code::Int32:
        case Type::Code::UInt32:
        case Type::Code::Float32:
        case Type::Code::DateTime:
        case Type::Code::Date32:
        case Type::Code::IPv4:
        case Type::Code::Decimal32:
        case Type::Code::Time:
            return AssertSize({4});

        case Type::Code::Int64:
        case Type::Code::UInt64:
        case Type::Code::Float64:
        case Type::Code::DateTime64:
        case Type::Code::Decimal64:
        case Type::Code::Time64:
            return AssertSize({8});

        case Type::Code::String:
        case Type::Code::FixedString:
        case Type::Code::JSON:
            // value can be of any size
            return;

        case Type::Code::Array:
        case Type::Code::Nullable:
        case Type::Code::Tuple:
        case Type::Code::LowCardinality:
        case Type::Code::Map:
            throw AssertionError("Unsupported type in ItemView: " + std::string(Type::TypeName(type)));

        case Type::Code::IPv6:
        case Type::Code::UUID:
        case Type::Code::UInt128:
        case Type::Code::Int128:
        case Type::Code::Decimal128:
            return AssertSize({16});

        case Type::Code::Decimal:
            // Could be either Decimal32, Decimal64 or Decimal128
            return AssertSize({4, 8, 16});

        default:
            throw UnimplementedError("Unknown type code:" + std::to_string(static_cast<int>(type)));
    }
}

}
