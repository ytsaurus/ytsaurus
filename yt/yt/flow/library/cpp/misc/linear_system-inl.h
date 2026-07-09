#ifndef LINEAR_SYSTEM_INL_H_
    #error "Direct inclusion of this file is not allowed, include linear_system.h"
    // For the sake of sane code completion.
    #include "linear_system.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TKeyedLinearSystem<T>::AddRow(
    std::vector<std::pair<T /*variable key*/, double /*coefficient*/>> a,
    double b)
{
    std::vector<std::pair<ssize_t /*variable index*/, double /*coefficient*/>> transformedA;
    transformedA.reserve(a.size());
    for (const auto& [key, coeff] : a) {
        auto it = KeyToIndex_.find(key);
        ssize_t variableIndex;
        if (it == KeyToIndex_.end()) {
            variableIndex = std::ssize(IndexToKey_);
            it = KeyToIndex_.emplace(key, variableIndex).first;
            IndexToKey_.push_back(it);
        } else {
            variableIndex = it->second;
        }
        transformedA.emplace_back(variableIndex, coeff);
    }
    System_.AddRow(transformedA, b);
}

template <class T>
THashMap<T, double> TKeyedLinearSystem<T>::Solve() const
{
    auto transformedSolution = System_.Solve();
    THashMap<T, double> result;
    for (ssize_t i = 0; i < std::ssize(IndexToKey_); ++i) {
        result[IndexToKey_[i]->first] = transformedSolution[i];
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
