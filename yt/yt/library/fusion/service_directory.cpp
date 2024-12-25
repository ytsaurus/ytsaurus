#include "service_directory.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <any>

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

class TServiceDirectory
    : public IServiceDirectory
{
public:
    void* FindUntypedService(TServiceId id) final
    {
        return ServiceMap_.Read([&] (const auto& serviceMap) -> void* {
            auto it = serviceMap.find(id);
            return it == serviceMap.end() ? nullptr : it->second.UntypedService;
        });
    }

    void RegisterUntypedService(
        TServiceId id,
        void* untypedService,
        std::any serviceHolder) final
    {
        ServiceMap_.Transform([&] (auto& serviceMap) {
            auto [_, inserted] = serviceMap.emplace(id, TServiceEntry{untypedService, std::move(serviceHolder)});
            if (!inserted) {
                THROW_ERROR_EXCEPTION("Service %v is already registered",
                    id);
            }
        });
    }

private:
    struct TServiceEntry
    {
        void* UntypedService;
        std::any ServiceHolder;
    };

    NThreading::TAtomicObject<THashMap<TServiceId, TServiceEntry>> ServiceMap_;
};

////////////////////////////////////////////////////////////////////////////////

IServiceDirectoryPtr CreateServiceDirectory()
{
    return New<TServiceDirectory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion
