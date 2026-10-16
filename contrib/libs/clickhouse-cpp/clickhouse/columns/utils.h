#pragma once

#include <algorithm>
#include <vector>
#include <memory>

namespace clickhouse {

template <typename T>
std::vector<T> SliceVector(const std::vector<T>& vec, size_t begin, size_t len) {
    std::vector<T> result;

    if (begin < vec.size()) {
        len = std::min(len, vec.size() - begin);
        result.assign(vec.begin() + begin, vec.begin() + (begin + len));
    }

    return result;
}

template <typename T>
struct HasWrapMethod {
private:
    static int detect(...);
    template <typename U>
    static decltype(U::Wrap(std::move(std::declval<ColumnRef>()))) detect(const U&);

public:
    static constexpr bool value = !std::is_same<int, decltype(detect(std::declval<T>()))>::value;
};

template <typename T>
inline std::shared_ptr<T> WrapColumn(ColumnRef&& column) {
    if constexpr (HasWrapMethod<T>::value) {
        return T::Wrap(std::move(column));
    } else {
        return column->template AsStrict<T>();
    }
}

}
