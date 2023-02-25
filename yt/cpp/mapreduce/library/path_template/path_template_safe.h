#pragma once

#include "path_template.h"

namespace NYtPathTemplate {
    /// @brief Same as @ref NDetail::TPathTemplate but counts empty node's count at compile time
    template<ui64 EmptyNodes>
    class TPathTemplateEmptyNodesCount : public TPathTemplate {
    public:
        using ThisType = TPathTemplateEmptyNodesCount<EmptyNodes>;
        using EmptyNodeChildType = TPathTemplateEmptyNodesCount<EmptyNodes + 1>;

        constexpr static ui64 EmptyNodesCount = EmptyNodes;

        /// @copybrief NYT::TCypressPath TPathTemplate::ToPathUnsafe(Args...)
        /// do not throws
        template<class ...Args>
        [[nodiscard]] NYT::TCypressPath ToPathSafe(const Args& ...emptySubPaths) const noexcept {
            static_assert(sizeof...(emptySubPaths) == EmptyNodesCount, "Wrong num of subpaths");
            return ToPathImpl({emptySubPaths...});
        }

        /// @copybrief NYT::TCypressPath TPathTemplate::UnsafePathRelativeTo(TPathTemplatePtr, Args...)
        /// do not throws
        template<class TParentType, class ...Args>
        [[nodiscard]] NYT::TCypressPath SafePathRelativeTo(TParentType parent, Args ...emptySubPaths) const noexcept {
            static_assert(sizeof...(emptySubPaths) == EmptyNodes - TParentType::TValueType::EmptyNodesCount);
            return PathRelativeToImpl(parent, {emptySubPaths...});
        }

        static TIntrusiveConstPtr<ThisType> MakeNode(
            std::optional<NYT::TCypressPath> path,
            TPathTemplatePtr parent = TPathTemplatePtr())
        {
            return TIntrusiveConstPtr<ThisType>(new ThisType(path, parent));
        }

    private:
        /// @copybrief NDetail::TPathTemplate(std::optional<NYT::TCypressPath>, NDetail::TPathTemplateConstPtr)
        /// @attention For counting empty nodes correctly use MakeSafeRootNode and MakeSafeChildNode functions
        /// to create TPathTemplateEmptyNodesCount instances
        TPathTemplateEmptyNodesCount(
                std::optional<NYT::TCypressPath> path,
                TPathTemplatePtr parent = TPathTemplatePtr())
            : TPathTemplate(std::move(path), std::move(parent))
        {
        }
    };

    /// @brief Creates root node of TPathTemplateEmptyNodesCount
    inline auto MakeSafeRootNode(NYT::TCypressPath path) {
        return TPathTemplateEmptyNodesCount<0>::MakeNode(path);
    }

    /// @brief Creates root node of TPathTemplateEmptyNodesCount
    inline auto MakeSafeRootNode() {
        return TPathTemplateEmptyNodesCount<1>::MakeNode(std::nullopt);
    }

    /// @brief Creates child node of TPathTemplateEmptyNodesCount
    /// TParentType - ::TIntrusivePtr on parent's node
    /// Fails on parent = ::TIntrusivePtr<TParentType>(nullptr)
    template <class TParentType>
    auto MakeSafeChildNode(TParentType parent, NYT::TCypressPath path) {
        Y_VERIFY(parent.Get());
        using returnType = typename TParentType::TValueType;
        return returnType::MakeNode(std::move(path), std::move(parent));
    }

    /// @brief Creates child node of TPathTemplateEmptyNodesCount
    /// TParentType - ::TIntrusivePtr on parent's node
    /// Fails on parent = ::TIntrusivePtr<TParentType>(nullptr)
    template <class TParentType>
    auto MakeSafeChildNode(TParentType parent) {
        Y_VERIFY(parent.Get());
        using returnType = typename TParentType::TValueType::EmptyNodeChildType;
        return returnType::MakeNode(std::nullopt, std::move(parent));
    }
}
