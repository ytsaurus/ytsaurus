#include "helpers.h"

#include <yt/yt/library/process/subprocess.h>

namespace NYT::NGpu {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FatalErrorMessage = "Unable to determine";
constexpr TStringBuf MinorNumberMessage = "Minor Number";
constexpr TStringBuf GpuUuidMessage = "GPU UUID";

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, int> GetGpuMinorNumbers(TDuration timeout)
{
    TSubprocess subprocess("nvidia-smi");
    subprocess.AddArguments({"-q"});

    auto nvidiaSmiResult = subprocess.Execute(
        /*input*/ TSharedRef::MakeEmpty(),
        timeout);

    if (!nvidiaSmiResult.Status.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: 'nvidia-smi -q' exited with an error")
            << nvidiaSmiResult.Status;
    }

    auto output = nvidiaSmiResult.Output.ToStringBuf();
    if (output.Contains(FatalErrorMessage)) {
        THROW_ERROR_EXCEPTION("Failed to check healthy GPUs: 'nvidia-smi -q' exited with fatal error");
    }

    THashMap<TString, int> result;

    size_t index = 0;
    while (true) {
        // Process GPU UUID.
        index = output.find(GpuUuidMessage, index);
        if (index == TString::npos) {
            break;
        }

        TString gpuId;
        int gpuNumber;

        {
            auto semicolonIndex = output.find(":", index);
            auto eolIndex = output.find("\n", index);
            if (semicolonIndex == TString::npos || eolIndex == TString::npos || eolIndex <= semicolonIndex) {
                THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi -q' output format: failed to parse GPU UUID");
            }

            gpuId = StripString(output.substr(semicolonIndex + 1, eolIndex - semicolonIndex - 1));

            index = eolIndex;
        }

        // Process GPU Minor Number.
        index = output.find(MinorNumberMessage, index);
        if (index == TString::npos) {
            THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi -q' output format: failed to find Minor Number after GPU UUID");
        }

        {
            auto semicolonIndex = output.find(":", index);
            auto eolIndex = output.find("\n", index);
            if (semicolonIndex == TString::npos || eolIndex == TString::npos || eolIndex <= semicolonIndex) {
                THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi -q' output format: failed to parse GPU Minor Number");
            }

            try {
                auto gpuNumberString = StripString(output.substr(semicolonIndex + 1, eolIndex - semicolonIndex - 1));
                gpuNumber = FromString<int>(gpuNumberString);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Invalid 'nvidia-smi -q' output format: failed to parse GPU Minor Number")
                    << ex;
            }

            index = eolIndex;
        }

        EmplaceOrCrash(result, gpuId, gpuNumber);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
