#pragma once

namespace NHp::NIntrusive::NPrivate {

    template <class _TSizeType, bool _IsTrackingSize = true>
    class TSizeTracker {
    public:
        using TSizeType = _TSizeType;
        static constexpr bool IsTrackingSize = true;

    public:
        inline void Increase(TSizeType n) noexcept {
            Size_ += n;
        }

        inline void Decrease(TSizeType n) noexcept {
            Size_ -= n;
        }

        inline void Increment() noexcept {
            ++Size_;
        }

        inline void Decrement() noexcept {
            --Size_;
        }

        inline TSizeType GetSize() const noexcept {
            return Size_;
        }

        inline void SetSize(TSizeType size) noexcept {
            Size_ = size;
        }

    private:
        TSizeType Size_{};
    };

    template <class _TSizeType>
    class TSizeTracker<_TSizeType, false> {
    public:
        using TSizeType = _TSizeType;
        static constexpr bool IsTrackingSize = false;

    public:
        inline void Increase(TSizeType) noexcept {
        }

        inline void Decrease(TSizeType) noexcept {
        }

        inline void Increment() noexcept {
        }

        inline void Decrement() noexcept {
        }

        inline TSizeType GetSize() const noexcept {
            return TSizeType{};
        }

        inline void SetSize(TSizeType) {
        }
    };

} // namespace NHp::NIntrusive::NPrivate
