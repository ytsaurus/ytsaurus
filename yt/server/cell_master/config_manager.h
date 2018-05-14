#include "public.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TConfigManager
    : public TRefCounted
{
public:
    explicit TConfigManager(NCellMaster::TBootstrap* bootstrap);
    ~TConfigManager();

    void Initialize();

    const TDynamicClusterConfigPtr& GetConfig() const;
    void SetConfig(TDynamicClusterConfigPtr config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
