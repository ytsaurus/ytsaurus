#include <yt/build/build.h>

#include <library/resource/resource.h>

#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

int main() {
    TString text = NResource::Find("/package.json.input");

    SubstGlobal(text, "@YT_VERSION_MAJOR@", ToString(NYT::GetVersionMajor()));
    SubstGlobal(text, "@YT_VERSION_MINOR@", ToString(NYT::GetVersionMinor()));
    SubstGlobal(text, "@YT_VERSION_PATCH@", ToString(NYT::GetVersionPatch()));
    SubstGlobal(text, "@YT_VERSION@", ToString(NYT::GetVersion()));

    Cout.Write(text);
    Cout.Flush();

    return 0;
}
