#include "status_profiler.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TStatusProfilerUnregistrator = std::function<void()>;

DEFINE_REFCOUNTED_TYPE(IStatusErrorState);
DEFINE_REFCOUNTED_TYPE(IStatusProfiler);

class TStatusErrorStateImpl
    : public IStatusErrorState
{
public:
    // #unregistrator holds parent alive, so the root can reach this node. Also it used to unlink from parent when object is destroyed.
    explicit TStatusErrorStateImpl(TStatusProfilerUnregistrator unregistrator)
        : Unregistrator_(std::move(unregistrator))
    { }

    ~TStatusErrorStateImpl() override
    {
        if (Unregistrator_) {
            Unregistrator_();
        }
    }

    void SetError(TError error) override
    {
        const TInstant now = TInstant::Now();
        auto guard = Guard(Lock_);
        Status_.IsOK = error.IsOK();
        if (Status_.IsOK.value()) {
            Status_.LastOKTime = now;
        }
        if (Error_.IsOK() != Status_.IsOK.value()) {
            Status_.LastStateChangeTime = now;
        }
        std::swap(Error_, error);
    }

    void ClearError() override
    {
        SetError(TError());
    }

    TStatus GetStatus() const override
    {
        auto guard = Guard(Lock_);
        return Status_;
    }

    void CollectErrors(TStringBuf prefix, THashMap<std::string, TError>& result) const
    {
        auto guard = Guard(Lock_);
        if (!Error_.IsOK()) {
            result[prefix] = Error_;
        }
    }

private:
    TStatusProfilerUnregistrator Unregistrator_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TError Error_;
    TStatus Status_;
};

////////////////////////////////////////////////////////////////////////////////

class TStatusProfilerImpl
    : public NYT::NFlow::IStatusProfiler
{
    using TRegistrationCookie = ui64;
    using TErrorCollector = std::function<void(const std::string& prefix, THashMap<std::string, TError>& result)>;

public:
    // #unregistrator holds parent alive, so the root can reach this node. Also it used to unlink from parent when object is destroyed.
    explicit TStatusProfilerImpl(TStatusProfilerUnregistrator unregistrator)
        : Unregistrator_(std::move(unregistrator))
    { }

    ~TStatusProfilerImpl() override
    {
        if (Unregistrator_) {
            Unregistrator_();
        }
    }

    template <typename TNode>
    TIntrusivePtr<TNode> CreateChild(TStringBuf nodePrefix)
    {
        auto guard = Guard(Lock_);
        auto cookie = RegistrationCookieCounter_++;
        auto node = New<TNode>([this, this_ = MakeStrong(this), cookie] () {
            TErrorCollector collector;
            {
                auto guard = Guard(Lock_);
                auto it = GetIteratorOrCrash(ErrorCollectors_, cookie);
                std::swap(collector, it->second);
                ErrorCollectors_.erase(it);
            }
        });
        ErrorCollectors_[cookie] = [weakNode = MakeWeak(node), nodePrefix = std::string(nodePrefix)] (const std::string& prefix, THashMap<std::string, TError>& result) {
            if (auto node = weakNode.Lock()) {
                node->CollectErrors(prefix + nodePrefix, result);
            }
        };
        return node;
    }

    NYT::NFlow::IStatusErrorStatePtr ErrorState(TStringBuf name) override
    {
        return CreateChild<TStatusErrorStateImpl>(name);
    }

    NYT::NFlow::IStatusProfilerPtr WithPrefix(TStringBuf prefix) override
    {
        return CreateChild<TStatusProfilerImpl>(prefix);
    }

    NYT::NFlow::IStatusProfiler::TUnitedProfilerStatus GetStatus() const override
    {
        NYT::NFlow::IStatusProfiler::TUnitedProfilerStatus status;
        CollectErrors("", status.Errors);
        return status;
    }

private:
    TStatusProfilerUnregistrator Unregistrator_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TRegistrationCookie RegistrationCookieCounter_ = 0;
    THashMap<TRegistrationCookie, TErrorCollector> ErrorCollectors_;

    void CollectErrors(
        const std::string& prefix,
        THashMap<std::string, TError>& result) const
    {
        std::vector<TErrorCollector> errorCollectors;
        {
            auto guard = Guard(Lock_);
            errorCollectors.reserve(ErrorCollectors_.size());
            for (const auto& [cookie, collector] : ErrorCollectors_) {
                errorCollectors.emplace_back(collector);
            }
        }
        for (const auto& collector : errorCollectors) {
            YT_VERIFY(collector);
            collector(prefix, result);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IStatusErrorState);
DEFINE_REFCOUNTED_TYPE(IStatusProfiler);

////////////////////////////////////////////////////////////////////////////////

IStatusProfilerPtr CreateStatusProfiler()
{
    return New<TStatusProfilerImpl>(TStatusProfilerUnregistrator());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
