#pragma once

#include "../types/types.h"
#include "../exceptions.h"

#include <string_view>
#include <stdexcept>
#include <type_traits>

namespace clickhouse {

/** ItemView is a view on a data stored in Column, safe-ish interface for reading values from Column.
 *
 * Data is not owned (hence the name View) and will be invalidated on column update, load
 * or destruction (basically on calling any non-const method of Column).
 * `type` reflects what is stored in `data` and can be almost any value-type
 * (except Nullable, Array, Tuple, LowCardinality).
 *
 */
struct ItemView {
    using DataType = std::string_view;

    const Type::Code type;
    const DataType data;

private:
    template <typename T>
    inline auto ConvertToStorageValue(const T& t) {
        if constexpr (std::is_same_v<std::string_view, T> || std::is_same_v<std::string, T>) {
            return std::string_view{t};
        } else if constexpr (std::is_fundamental_v<T>
                          || std::is_same_v<Int128, std::decay_t<T>>
                          || std::is_same_v<UInt128, std::decay_t<T>>) {
            return std::string_view{reinterpret_cast<const char*>(&t), sizeof(T)};
        } else {
            static_assert(!std::is_same_v<T, T>, "Unknown type, which can't be stored in ItemView");
            return;
        }
    }

public:
    ItemView(Type::Code type, DataType data)
        : type(type),
          data(data)
    {
        ValidateData(type, data);
    }

    ItemView(Type::Code type, ItemView other)
        : type(type),
          data(other.data)
    {
        ValidateData(type, data);
    }

    explicit ItemView()
        : ItemView(Type::Void, std::string_view{})
    {}

    template <typename T>
    explicit ItemView(Type::Code type, const T & value)
        : ItemView(type, ConvertToStorageValue(value))
    {}

    template <typename T>
    auto get() const {
        using ValueType = std::remove_cv_t<std::decay_t<T>>;
        if constexpr (std::is_same_v<std::string_view, ValueType> || std::is_same_v<std::string, ValueType>) {
            return data;
        } else if constexpr (std::is_fundamental_v<ValueType>
                          || std::is_same_v<Int128, ValueType>
                          || std::is_same_v<UInt128, ValueType>) {
            if (sizeof(ValueType) == data.size()) {
                return *reinterpret_cast<const T*>(data.data());
            } else {
                throw AssertionError("Incompatitable value type and size. Requested size: "
                        + std::to_string(sizeof(ValueType)) + " stored size: " + std::to_string(data.size()));
            }
        }
    }

    inline std::string_view AsBinaryData() const {
        return data;
    }

    // Validate that value matches type, will throw an exception if validation fails.
    static void ValidateData(Type::Code type, DataType data);
};

}
