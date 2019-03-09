#include "public.h"

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TAnnotationSetter
    : public TRefCounted
{
public:
    explicit TAnnotationSetter(NCellMaster::TBootstrap* bootstrap);
    void Start();

    ~TAnnotationSetter();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAnnotationSetter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
