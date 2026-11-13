#pragma once

namespace NYql::NFmr::NTests {

    enum class EExecutionMode {
        Fmr,
        FailFirstOperation,
        FailSecondOperation,
    };

    enum class EInjectedFailure {
        FallbackOperation,
        RestartOperation,
    };

} // namespace NYql::NFmr::NTests
