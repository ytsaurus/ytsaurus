#include "ytree_helpers.h"

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NQueueAgent {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TMergedVirtualMapService
    : public TVirtualMapBase
{
public:
    explicit TMergedVirtualMapService(std::vector<TVirtualMapPartBasePtr> parts)
        : Parts_(std::move(parts))
    { }

    i64 GetSize() const override
    {
        i64 size = 0;
        for (const auto& part : Parts_) {
            size += part->GetSize();
        }
        return size;
    }

    std::vector<std::string> GetKeys(i64 limit) const override
    {
        std::vector<std::string> keys;
        for (const auto& part : Parts_) {
            if (std::ssize(keys) >= limit) {
                break;
            }
            auto partKeys = part->GetKeys(limit - std::ssize(keys));
            keys.insert(
                keys.end(),
                std::make_move_iterator(partKeys.begin()),
                std::make_move_iterator(partKeys.end()));
        }
        return keys;
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        for (const auto& part : Parts_) {
            if (auto service = part->FindItemService(key)) {
                return service;
            }
        }
        return nullptr;
    }

private:
    const std::vector<TVirtualMapPartBasePtr> Parts_;
};

IYPathServicePtr CreateMergedVirtualMapService(std::vector<TVirtualMapPartBasePtr> parts)
{
    return New<TMergedVirtualMapService>(std::move(parts));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
