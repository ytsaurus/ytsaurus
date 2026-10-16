#pragma once

#include <array>

namespace NHp::NUtil {

    template <std::size_t Size, class TBucketType>
    class TStaticBucketTraits {
        using TBuckets = std::array<TBucketType, Size>;

    public:
        using TBucketPtr = TBuckets::pointer;
        using TSizeType = std::size_t;

    public:
        TStaticBucketTraits() noexcept = default;

        TSizeType size() const noexcept {
            return Data_.size();
        }

        TBucketPtr data() const noexcept {
            return const_cast<TBucketPtr>(Data_.data());
        }

        void swap(TStaticBucketTraits& other) noexcept {
            std::swap(Data_, other.Data_);
        }

        friend void swap(TStaticBucketTraits& left, TStaticBucketTraits& right) noexcept { // NOLINT(readability-identifier-naming)
            left.swap(right);
        }

    private:
        TBuckets Data_{};
    };

} // namespace NHp::NUtil
