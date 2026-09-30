#pragma once

#include <string>

namespace clickhouse {
struct Exception {
    int code = 0;
    std::string name;
    std::string display_text;
    std::string stack_trace;
};

}
