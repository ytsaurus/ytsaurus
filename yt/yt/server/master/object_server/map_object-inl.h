#ifndef MAP_OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include map_object.h"
// For the sake of sane code completion.
#include "map_object.h"
#endif

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TSelf>
void TNonversionedMapObjectBase<TSelf>::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, Acd_);
    PHOENIX_REGISTER_FIELD(2, IsRoot_);
    PHOENIX_REGISTER_FIELD(3, Parent_);
    PHOENIX_REGISTER_FIELD(4, KeyToChild_);
    PHOENIX_REGISTER_FIELD(5, SubtreeSize_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        // Reconstruct ChildToKey_ map.
        for (const auto& [key, child] : this_->KeyToChild_) {
            if (child) {
                YT_VERIFY(this_->ChildToKey_.emplace(child, key).second);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

template <class TObject, class TFunctor>
void TraverseMapObjectSubtreeTopDown(TObject* object, TFunctor functor)
{
    functor(object);

    for (const auto& [key, child] : object->KeyToChild()) {
        TraverseMapObjectSubtreeTopDown(child.Get(), functor);
    }
}

template <class TObject, class TResult, class TFunctor>
TResult AccumulateOverMapObjectSubtree(TObject* object, TResult init, TFunctor functor)
{
    auto result = std::move(init);
    TraverseMapObjectSubtreeTopDown(object, [&] (auto* currentObject) {
        functor(currentObject, &result);
    });
    return result;
}

template <class TObject>
TObject* FindMapObjectLCA(TObject* lhs, TObject* rhs)
{
    auto getRootAndDepth = [] (TObject* object) {
        auto depth = 0;
        auto* root = object;
        for (; root && root->GetParent(); root = root->GetParent()) {
            ++depth;
        }
        return std::pair(root, depth);
    };

    auto [lhsRoot, lhsDepth] = getRootAndDepth(lhs);
    auto [rhsRoot, rhsDepth] = getRootAndDepth(rhs);
    if (lhsRoot != rhsRoot) {
        return nullptr;
    }

    if (lhsDepth < rhsDepth) {
        std::swap(lhs, rhs);
        std::swap(lhsDepth, rhsDepth);
    }

    for (auto i = 0; i < lhsDepth - rhsDepth; ++i) {
        lhs = lhs->GetParent();
    }

    while (lhs != rhs) {
        lhs = lhs->GetParent();
        rhs = rhs->GetParent();
    }

    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
