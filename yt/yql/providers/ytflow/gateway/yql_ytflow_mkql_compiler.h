#pragma once

namespace NYql {

struct TTypeAnnotationContext;
struct TYtflowSettings;

} // namespace NYql

namespace NYql::NCommon {

class TMkqlCallableCompilerBase;

} // namespace NYql::NCommon

namespace NYql {

void RegisterYtflowMkqlCompiler(
    NCommon::TMkqlCallableCompilerBase& compiler,
    const TTypeAnnotationContext& ctx,
    const TYtflowSettings& config);

} // namespace NYql
