#pragma once

namespace clickhouse {

template <typename T>
T* Singleton() {
    static T instance;
    return &instance;
}

}
