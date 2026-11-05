#pragma once

#include <iterator>
#include <type_traits>
#include <utility>

namespace clickhouse {

template <typename UnaryFunction, typename Iterator, typename Reference = decltype(std::declval<UnaryFunction>()(std::declval<Iterator>())),
          typename Value = std::decay_t<Reference>>
class ProjectedIterator {
public:
    using value_type        = Value;
    using reference         = Reference;
    using pointer           = Reference;
    using difference_type   = typename std::iterator_traits<Iterator>::difference_type;
    using iterator_category = typename std::iterator_traits<Iterator>::iterator_category;

    ProjectedIterator() = default;

    inline ProjectedIterator(Iterator const& iterator, UnaryFunction functor)
        : iterator_(iterator)
        , functor_(std::move(functor)) {
    }

    inline UnaryFunction functor() const { return functor; }

    inline Iterator const& base() const { return iterator_; }

    inline reference operator*() const { return functor_(iterator_); }

    inline ProjectedIterator& operator++() {
        ++iterator_;
        return *this;
    }

    inline ProjectedIterator& operator--() {
        --iterator_;
        return *this;
    }

    inline bool operator==(const ProjectedIterator& other) const {
        return this->iterator_ == other.iterator_;
    }

    inline bool operator!=(const ProjectedIterator& other) const {
        return !(*this == other);
    }

private:
    Iterator iterator_;
    UnaryFunction functor_;
};

}  // namespace clickhouse
