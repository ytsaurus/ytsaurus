#pragma once

#include <yt/cpp/roren/interface/roren.h>

#include <util/generic/ptr.h>

#include <any>

namespace NRoren {

namespace NPrivate {
class TBigRtMemoryStorage;
}

////////////////////////////////////////////////////////////////////////////////

class TBigRtMemoryResult
    : public TThrRefBase
{
private:
    template <typename T>
    using TVectorPtr = std::shared_ptr<std::vector<T>>;

public:
    template <typename T>
    const std::vector<T>& GetRowList(const TTypeTag<T>& tag) const
    {
        auto it = VectorMap_.find(TDynamicTypeTag{tag}.GetKey());
        if (it == VectorMap_.end()) {
            Y_FAIL("Tag %s is unknown", tag.GetDescription().c_str());
        } else {
            return *std::any_cast<TVectorPtr<T>>(it->second);
        }
    }

    template <typename T>
    std::vector<T>&& MoveRowList(const TTypeTag<T>& tag) const
    {
        auto it = VectorMap_.find(TDynamicTypeTag{tag}.GetKey());
        if (it == VectorMap_.end()) {
            ythrow yexception() << "Tag: " << ToString(tag) << " is unknown";
        } else {
            return std::move(*std::any_cast<TVectorPtr<T>>(it->second));
        }
    }

private:
    THashMap<TDynamicTypeTag::TKey, std::any> VectorMap_;

private:
    friend class NPrivate::TBigRtMemoryStorage;
};
using TBigRtMemoryResultPtr = ::TIntrusivePtr<TBigRtMemoryResult>;

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TBigRtMemoryStorage
{
private:
    template <typename T>
    using TVectorPtr = TBigRtMemoryResult::TVectorPtr<T>;

public:
    template <typename T>
    NPrivate::IRawOutputPtr AddOutput(const TTypeTag<T>& tag)
    {
        auto [it, inserted] = VectorMap_.emplace(TDynamicTypeTag{tag}.GetKey(), std::any{});
        if (!inserted) {
            ythrow yexception() << "Tag " << ToString(tag) << " already exists";
        }
        auto vector = std::make_shared<std::vector<T>>();
        it->second = std::any{vector};
        return ::MakeIntrusive<TRawVectorOutput<T>>(std::move(vector));
    }

    TBigRtMemoryResultPtr ToResult()
    {
        auto result = ::MakeIntrusive<TBigRtMemoryResult>();
        result->VectorMap_ = std::move(VectorMap_);
        return result;
    }

private:
    template <typename T>
    class TRawVectorOutput : public IRawOutput
    {
    public:
        explicit TRawVectorOutput(TVectorPtr<T> output)
            : Output_(std::move(output))
        { }

        void AddRaw(const void* rows, ssize_t count) override
        {
            Output_->insert(
                Output_->end(),
                static_cast<const T*>(rows),
                static_cast<const T*>(rows) + count);
        }

        void Close() override
        {
            Output_ = nullptr;
        }

    private:
        TVectorPtr<T> Output_;
    };

private:
    THashMap<TDynamicTypeTag::TKey, std::any> VectorMap_;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate
} // namespace NRoren
