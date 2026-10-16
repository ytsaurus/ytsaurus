#pragma once

#include "companion_manager.h"

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! YSON-serializable parameters for the Java companion manager.
/*!
 *  Adds three Java-specific shortcut fields on top of the generic companion params.
 *  TJavaCompanionManager spawns the JVM via TJavaProcessManager using these fields directly,
 *  bypassing the generic `entrypoint`.
 */
struct TJavaCompanionManagerParameters
    : public TCompanionManagerParameters
{
    //! Path to the JDK bin directory (e.g. containing the `java` executable).
    std::string JdkBinPath;

    //! Java classpath passed to the JVM on startup.
    std::string Classpath;

    //! Fully-qualified main class name to execute.
    std::string MainClass;

    REGISTER_YSON_STRUCT(TJavaCompanionManagerParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJavaCompanionManagerParameters);

////////////////////////////////////////////////////////////////////////////////

//! Companion manager specialization for Java-based companions.
//! Spawns the JVM via TJavaProcessManager using JdkBinPath/Classpath/MainClass directly.
class TJavaCompanionManager
    : public TCompanionManager
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TJavaCompanionManagerParameters, TCompanionManager);

    TJavaCompanionManager(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

protected:
    TProcessManagerBasePtr CreateProcessManager() override;
};

DEFINE_REFCOUNTED_TYPE(TJavaCompanionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
