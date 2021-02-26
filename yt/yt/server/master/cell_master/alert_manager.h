#pragma once

#include "public.h"

#include <yt/core/actions/signal.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

using TAlertSource = std::function<std::vector<TError>()>;

////////////////////////////////////////////////////////////////////////////////

class TAlertManager
    : public TRefCounted
{
public:
    explicit TAlertManager(TBootstrap* bootstrap);
    ~TAlertManager();

    void Initialize();

    void RegisterAlertSource(TAlertSource alertSource);

    std::vector<TError> GetAlerts() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAlertManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
