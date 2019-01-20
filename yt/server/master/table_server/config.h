#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableAutoReshard;
    bool EnableAutoTabletMove;

    std::optional<i64> MinTabletSize;
    std::optional<i64> MaxTabletSize;
    std::optional<i64> DesiredTabletSize;
    std::optional<int> DesiredTabletCount;

    TTabletBalancerConfig()
    {
        RegisterParameter("enable_auto_reshard", EnableAutoReshard)
            .Default(true);

        RegisterParameter("enable_auto_tablet_move", EnableAutoTabletMove)
            .Default(true);

        RegisterParameter("min_tablet_size", MinTabletSize)
            .Default();

        RegisterParameter("max_tablet_size", MaxTabletSize)
            .Default();

        RegisterParameter("desired_tablet_size", DesiredTabletSize)
            .Default();

        RegisterParameter("desired_tablet_count", DesiredTabletCount)
            .Default();

        RegisterPostprocessor([&] {
            CheckTabletSizeInequalities();
        });
    }

    // COMPAT(ifsmirnov)
    void SetMinTabletSize(std::optional<i64> value)
    {
        SetTabletSizeConstraint(&MinTabletSize, value);
    }

    void SetDesiredTabletSize(std::optional<i64> value)
    {
        SetTabletSizeConstraint(&DesiredTabletSize, value);
    }

    void SetMaxTabletSize(std::optional<i64> value)
    {
        SetTabletSizeConstraint(&MaxTabletSize, value);
    }

private:
    void CheckTabletSizeInequalities() const
    {
        if (MinTabletSize && DesiredTabletSize && *MinTabletSize > *DesiredTabletSize) {
            THROW_ERROR_EXCEPTION("\"min_tablet_size\" must be less than or equal to \"desired_tablet_size\"");
        }
        if (DesiredTabletSize && MaxTabletSize && *DesiredTabletSize > *MaxTabletSize) {
            THROW_ERROR_EXCEPTION("\"desired_tablet_size\" must be less than or equal to \"max_tablet_size\"");
        }
    }

    void SetTabletSizeConstraint(std::optional<i64>* member, std::optional<i64> value)
    {
        auto oldValue = *member;
        try {
            *member = value;
            CheckTabletSizeInequalities();
        } catch (const std::exception& ex) {
            *member = oldValue;
            throw;
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
