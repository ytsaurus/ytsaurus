#pragma once

#include "public.h"

#include <core/misc/public.h>

#include <core/ytree/public.h>

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceManager
{
public:
    static TTraceManager* Get();

    void Configure(NYTree::INodePtr node, const NYPath::TYPath& path = "");
    void Configure(const Stroka& fileName, const NYPath::TYPath& path);

    void Enqueue(
        const NTracing::TTraceContext& context,
        const Stroka& serviceName,
        const Stroka& spanName,
        const Stroka& annotationName);

    void Enqueue(
        const NTracing::TTraceContext& context,
        const Stroka& annotationKey,
        const Stroka& annotationValue);

    DECLARE_SINGLETON_MIXIN(TTraceManager, TStaticInstanceMixin);
    DECLARE_SINGLETON_PRIORITY(TTraceManager, 10);

private:
    TTraceManager();

    ~TTraceManager();

    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

