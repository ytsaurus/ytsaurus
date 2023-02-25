#pragma once

#include <yt/cpp/mapreduce/library/cypress_path/cypress_path.h>

#include <util/generic/ptr.h>

#include <optional>

namespace NYtPathTemplate {
    class TPathTemplate;
    using TPathTemplatePtr = TIntrusiveConstPtr<TPathTemplate>;

    TPathTemplatePtr MakeRootNode(std::optional<NYT::TCypressPath> = std::nullopt);
    TPathTemplatePtr MakeChildNode(TPathTemplatePtr, std::optional<NYT::TCypressPath> = std::nullopt);

    /// @brief Class describes relative path of cypress node against it's parent
    class TPathTemplate : public TSimpleRefCount<TPathTemplate> {
    protected:
        /// @brief Constructs TPathTemplate
        /// [intput] path - relative path against parent.
        /// May be std::nullopt that means that you don't no it at compilation time. That paths called empty.
        /// [intput] parent - node's parent.
        /// May be  TPathTemplateConstPtr() if it is root node
        TPathTemplate(std::optional<NYT::TCypressPath> path, TPathTemplatePtr parent)
            : Path_(std::move(path))
            , Parent_(std::move(parent))
        {
        }

    public:
        /// @brief Returns full path of node
        /// emptySubPaths - list of paths that will replace it's and parent's empty nodes
        /// Fails when number of subpaths is different from it's and parent's empty nodes count
        template<class ...Args>
        [[nodiscard]] NYT::TCypressPath ToPathUnsafe(Args ...emptySubPaths) const {
            return ToPathImpl({emptySubPaths...});
        }

        /// @brief Returns path against some parent node
        /// emptySubPaths - list of paths that will replace it's and parent's empty nodes
        /// Fails when number of subpaths is different from relative path's empty nodes conut
        template<class ...Args>
        [[nodiscard]] NYT::TCypressPath UnsafePathRelativeTo(TPathTemplatePtr parent, Args ...emptySubPaths) const {
            return PathRelativeToImpl(parent, {emptySubPaths...});
        }

    protected:
        [[nodiscard]] NYT::TCypressPath ToPathImpl(const std::initializer_list<NYT::TCypressPath>& emptySubPaths) const;

        [[nodiscard]] NYT::TCypressPath PathRelativeToImpl(
            TPathTemplatePtr parent,
            const std::initializer_list<NYT::TCypressPath>& emptySubPaths) const;

    private:
        /// @brief Creates root node of TPathTemplate
        friend TPathTemplatePtr MakeRootNode(std::optional<NYT::TCypressPath>);

        /// @brief Creates child node of TPathTemplate
        /// @throws on parent = TPathTemplateConstPtr(nullptr)
        friend TPathTemplatePtr MakeChildNode(TPathTemplatePtr, std::optional<NYT::TCypressPath>);

    private:
        const std::optional<NYT::TCypressPath> Path_;
        const TPathTemplatePtr Parent_;
    };
}
