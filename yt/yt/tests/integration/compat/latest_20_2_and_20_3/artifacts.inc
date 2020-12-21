# Add arbitrary YT_BINARIES resources here and specify "artifact_bin/<name>" as a resource destianation.
# After that, specify version overrides in the test suite as follows:
# ARTIFACT_COMPONENTS = {
#     "<name>" : ["master", "controller-agent", ...]
# }

DATA(
    # https://sandbox.yandex-team.ru/task/844096514/view
    sbr://1878788066=artifact_bin/20_2

    # https://sandbox.yandex-team.ru/task/849263681/view
    sbr://1878788066=artifact_bin/20_3

    # This version was deployed to Arnold tablet nodes on 17.12.20
    # resulting in data loss due to lost node <-> master compatibility.
    # https://sandbox.yandex-team.ru/task/843387463/view
    # sbr://1877197090=artifact_bin/20_3
)
