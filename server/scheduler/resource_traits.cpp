#include "resource_traits.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

bool IsAnonymousResource(EResourceKind kind)
{
    switch (kind) {
        case EResourceKind::Disk:
        case EResourceKind::Gpu:
            return false;
        case EResourceKind::Cpu:
        case EResourceKind::Memory:
        case EResourceKind::Slot:
        case EResourceKind::Network:
            return true;
        default:
            YT_UNIMPLEMENTED();
    }
}

bool IsSingletonResource(EResourceKind kind)
{
    switch (kind) {
        case EResourceKind::Disk:
        case EResourceKind::Gpu:
            return false;
        case EResourceKind::Cpu:
        case EResourceKind::Memory:
        case EResourceKind::Slot:
        case EResourceKind::Network:
            return true;
        default:
            YT_UNIMPLEMENTED();
    }
}

bool IsHomogeneousResource(EResourceKind kind)
{
    switch (kind) {
        case EResourceKind::Disk:
            return false;
        case EResourceKind::Cpu:
        case EResourceKind::Memory:
        case EResourceKind::Slot:
        case EResourceKind::Network:
        case EResourceKind::Gpu:
            return true;
        default:
            YT_UNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
