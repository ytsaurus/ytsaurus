#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

#include <core/ytree/public.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

class TLogManager
    : public IShutdownable
{
public:
    TLogManager();
    ~TLogManager();

    static TLogManager* Get();

    static void StaticShutdown();

    void Configure(NYTree::INodePtr node);
    void Configure(const Stroka& fileName, const NYPath::TYPath& path);
    void Configure(TLogConfigPtr&& config);

    virtual void Shutdown() override;

    int GetVersion() const;
    ELogLevel GetMinLevel(const Stroka& category) const;

    void Enqueue(TLogEvent&& event);

    void Reopen();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT

template <>
struct TSingletonTraits<NYT::NLogging::TLogManager>
{
    enum
    {
        Priority = 2048
    };
};
