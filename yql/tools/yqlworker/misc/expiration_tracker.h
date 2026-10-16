#pragma once

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NYql {

/*
Single-threaded expiration tracker
Type T should be comparable using operator==, operator< and support operator<<
*/
template <typename T>
class TExpirationTracker {
public:
    /*
    Does nothing if item already added
    */
    void Add(TInstant expireAt, T item) {
        if (ContainsItem(item)) {
            return;
        }

        Items_.emplace(item);
        Instant2Items_.emplace(expireAt, item);
    }

    void Remove(T item) {
        if (!ContainsItem(item)) {
            return;
        }

        Items_.erase(item);
        auto it = FindIf(Instant2Items_, [&](auto& p) { return p.second == item; });
        YQL_ENSURE(it != Instant2Items_.end());
        Instant2Items_.erase(it);
    }

    size_t GetSize() const {
        YQL_ENSURE(Items_.size() == Instant2Items_.size());
        return Items_.size();
    }

    bool IsEmpty() const {
        return GetSize() == 0;
    }

    /*
    Find expired items and call 'handler' for them.
    'handler' should not modify this container
    Expired items will be removed
    */
    template <typename F>
    void Process(TInstant now, F handler) {
        if (IsEmpty()) {
            return;
        }

        // exit fast if smallest timestamp is still greater than now
        if (Instant2Items_.begin()->first > now) {
            return;
        }

        EraseNodesIf(Instant2Items_, [&, this](auto& p) {
            if (p.first > now) {
                return false;
            }

            try {
                handler(p.second);
            } catch (const std::exception& e) {
                YQL_LOG(ERROR) << "Error in expiration handler for item " << p.second << ", msg: " << e.what();
            }

            Items_.erase(p.second);
            return true;
        });
    }

private:
    bool ContainsItem(T item) const {
        return Items_.contains(item);
    }

private:
    TSet<T> Items_;
    TMap<TInstant, T> Instant2Items_;
};

}
