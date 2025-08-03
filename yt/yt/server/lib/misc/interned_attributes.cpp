#include "interned_attributes.h"

#include <algorithm>

namespace {

////////////////////////////////////////////////////////////////////////////////

#define XX(camelCaseName, snakeCaseName) #snakeCaseName,

constexpr std::string_view Names[] = {
    FOR_EACH_INTERNED_ATTRIBUTE(XX)
};

#undef XX

static_assert(std::ranges::is_sorted(Names));

////////////////////////////////////////////////////////////////////////////////

}
