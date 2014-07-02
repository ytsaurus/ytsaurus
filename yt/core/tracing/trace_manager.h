#pragma once

#include "public.h"

#include <core/ytree/public.h>

#include <util/generic/singleton.h>

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceManager
{
public:
    static TTraceManager* Get();

    void Configure(NYTree::INodePtr node, const NYPath::TYPath& path = "");
    void Configure(const Stroka& fileName, const NYPath::TYPath& path);

    void Shutdown();

    void Enqueue(
        const NTracing::TTraceContext& context,
        const Stroka& serviceName,
        const Stroka& spanName,
        const Stroka& annotationName);

    void Enqueue(
        const NTracing::TTraceContext& context,
        const Stroka& annotationKey,
        const Stroka& annotationValue);

private:
    TTraceManager();

    DECLARE_SINGLETON_FRIEND(TTraceManager);

    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

