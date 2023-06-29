#include "wrappers.h"

namespace NYT::NDetail {

TSortColumns GetReduceByFields(const TSortColumns& reduceFields) {
    TSortColumns dst;
    for (auto& name : reduceFields.Parts_) {
        if (name == SortBySep)
            break;
        dst.Parts_.push_back(name);
    }
    return dst;
}

TSortColumns GetSortByFields(const TSortColumns& reduceFields) {
    TSortColumns dst;
    for (auto& name : reduceFields.Parts_) {
        if (name == SortBySep)
            continue;
        dst.Parts_.push_back(name);
    }
    return dst;
}

} // namespace NDetail
