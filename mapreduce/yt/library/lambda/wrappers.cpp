#include "wrappers.h"

namespace NYT::NDetail {

TKeyColumns GetReduceByFields(const TKeyColumns& reduceFields) {
    TKeyColumns dst;
    for (auto& name : reduceFields.Parts_) {
        if (name.empty())
            break;
        dst.Parts_.push_back(name);
    }
    return dst;
}

TKeyColumns GetSortByFields(const TKeyColumns& reduceFields) {
    TKeyColumns dst;
    for (auto& name : reduceFields.Parts_) {
        if (name.empty())
            continue;
        dst.Parts_.push_back(name);
    }
    return dst;
}

} // namespace NDetail
