#include <yt/yql/tools/dq/worker_job/child_environment.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/system/shellcommand.h>
#include <util/system/sysstat.h>

namespace NYql::NDq::NWorker {
    namespace {

        int RunChild(
            const TFsPath& executable,
            const TFsPath& workingDirectory,
            const THashMap<TString, TString>& environment,
            TString* error = nullptr)
        {
            TShellCommandOptions options;
            options.SetUseShell(false);
            options.Environment = environment;

            TShellCommand command(executable.GetPath(), {}, options, workingDirectory.GetPath());
            command.Run().Wait();
            if (error) {
                *error = command.GetError();
            }

            UNIT_ASSERT(command.GetExitCode().Defined());
            return *command.GetExitCode();
        }

        Y_UNIT_TEST_SUITE(TChildLdLibraryPathTest) {
            Y_UNIT_TEST(IsNotSetWhenLocalLibraryPathIsDisabled) {
                for (const bool enablePorto : {false, true}) {
                    THashMap<TString, TString> environment;

                    NDetail::ConfigureChildLdLibraryPath(&environment, /*useLocalLdLibraryPath*/ false, enablePorto, "/yt/job/sandbox");

                    UNIT_ASSERT(!environment.contains("LD_LIBRARY_PATH"));
                }
            }

            Y_UNIT_TEST(UsesJobSandboxBeforeTaskWorkDirWithoutPorto) {
                THashMap<TString, TString> environment;

                NDetail::ConfigureChildLdLibraryPath(&environment, /*useLocalLdLibraryPath*/ true, /*enablePorto*/ false, "/yt/job/sandbox");

                UNIT_ASSERT_VALUES_EQUAL(environment.at("LD_LIBRARY_PATH"), "/yt/job/sandbox:.");
            }

            Y_UNIT_TEST(PreservesCurrentPortoBehavior) {
                THashMap<TString, TString> environment;

                NDetail::ConfigureChildLdLibraryPath(&environment, /*useLocalLdLibraryPath*/ true, /*enablePorto*/ true, "/yt/job/sandbox");

                UNIT_ASSERT_VALUES_EQUAL(environment.at("LD_LIBRARY_PATH"), ".");
            }

            Y_UNIT_TEST(RejectsRelativeJobSandboxPathWithoutPorto) {
                THashMap<TString, TString> environment;

                UNIT_ASSERT_EXCEPTION_CONTAINS(
                    NDetail::ConfigureChildLdLibraryPath(
                        &environment,
                        /*useLocalLdLibraryPath*/ true,
                        /*enablePorto*/ false,
                        "relative/job/sandbox"),
                    yexception,
                    "must be absolute");
            }

            Y_UNIT_TEST(LoadsRuntimeLibraryAfterExecutableRelocation) {
                TTempDir root;
                const auto sandbox = root.Path() / "sandbox";
                const auto cache = root.Path() / "file_cache2" / "cache";
                const auto slot = cache / "Slot-1";
                const auto relocatedExecutable = cache / "dq_worker_runtime_test_child";

                sandbox.MkDirs();
                slot.MkDirs();
                TFsPath(BinaryPath("yt/yql/tools/dq/worker_job/ut/test_child/dq_worker_runtime_test_child"))
                    .CopyTo(relocatedExecutable.GetPath(), true);
                TFsPath(BinaryPath("contrib/libs/libiconv/dynamic/libiconv.so"))
                    .CopyTo((sandbox / "libiconv.so").GetPath(), true);
                UNIT_ASSERT_VALUES_EQUAL(Chmod(relocatedExecutable.c_str(), MODE0755), 0);

                THashMap<TString, TString> legacyEnvironment{{"LD_LIBRARY_PATH", "."}};
                TString loaderError;
                UNIT_ASSERT_VALUES_UNEQUAL(
                    RunChild(relocatedExecutable, slot, legacyEnvironment, &loaderError),
                    0);
                UNIT_ASSERT_STRING_CONTAINS(loaderError, "libiconv.so");

                THashMap<TString, TString> environment;
                NDetail::ConfigureChildLdLibraryPath(
                    &environment,
                    /*useLocalLdLibraryPath*/ true,
                    /*enablePorto*/ false,
                    sandbox.GetPath());
                UNIT_ASSERT_VALUES_EQUAL(RunChild(relocatedExecutable, slot, environment), 0);

                const auto runtimeLibrary = sandbox / "libiconv.so";
                runtimeLibrary.DeleteIfExists();
                TFsPath(BinaryPath("contrib/libs/libiconv/dynamic/libiconv.so"))
                    .CopyTo((slot / "libiconv.so").GetPath(), true);
                UNIT_ASSERT_VALUES_EQUAL(RunChild(relocatedExecutable, slot, environment), 0);
            }
        } // Y_UNIT_TEST_SUITE(TChildLdLibraryPathTest)

    } // namespace
} // namespace NYql::NDq::NWorker
