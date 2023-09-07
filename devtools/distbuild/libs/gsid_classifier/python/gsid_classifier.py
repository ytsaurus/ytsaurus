from __future__ import unicode_literals

import six


AUTOCHECK = 'autocheck-build'
AUTOCHECK_ACCEPTANCE = 'autocheck-acceptance'
AUTOCHECK_BRANCH_PRECOMMIT = 'autocheck-branch-precommit'
AUTOCHECK_BRANCH_PRECOMMIT_RECHECK = 'autocheck-branch-precommit-recheck'
AUTOCHECK_TRUNK_PRECOMMIT = 'autocheck-trunk-precommit'
AUTOCHECK_TRUNK_PRECOMMIT_RECHECK = 'autocheck-trunk-precommit-recheck'
AUTOCHECK_BRANCH_POSTCOMMIT = 'autocheck-branch-postcommit'
AUTOCHECK_BRANCH_POSTCOMMIT_RECHECK = 'autocheck-branch-postcommit-recheck'
AUTOCHECK_TRUNK_POSTCOMMIT = 'autocheck-trunk-postcommit'
AUTOCHECK_TRUNK_POSTCOMMIT_RECHECK = 'autocheck-trunk-postcommit-recheck'
COVERAGE = 'coverage'
COVERAGE_LARGE_SANDBOX = 'coverage-large-sandbox'
COVERAGE_NIGHTLY = 'coverage-nightly'
COVERAGE_NIGHTLY_LARGE_SANDBOX = 'coverage-nightly-large-sandbox'
COVERAGE_REVIEW = 'coverage-review'
COVERAGE_REVIEW_LARGE_SANDBOX = 'coverage-review-large-sandbox'
FUZZING = 'fuzzing-build'
GRAPH_GENERATION = 'graph-generation'
GRAPH_GENERATION_PRE_COMMIT = 'graph-generation-pre-commit'
GRAPH_GENERATION_POST_COMMIT = 'graph-generation-post-commit'
GRAPH_GENERATION_CI = 'graph-generation-pre-ci'
GRAPH_GENERATION_OTHER = 'graph-generation-other'
LARGE_TEST = 'large-test-build'
RECHECK = 'recheck'
REVIEW_BOARD = 'review-check-build'
REVIEW_LARGE_TEST = 'review-check-large-test-build'
REVIEW_LOW_CACHE_HIT = 'review-low-cache-hit'
REVIEW_PESSIMIZED = 'review-pessimized'
REVIEW_SANDBOX_TASK = 'review-sandbox-task'
SANDBOX_TASK = 'sandbox-task-build'
TEAMCITY = 'teamcity'
UNDEFINED = 'undefined'
USER_BUILD = 'user-build'
YA_DEV = 'ya-dev-build'
YA_USE_WORKING_COPY_REVISION = 'ya-use-working-copy-revision'
YNDEXER = 'yndexer'
YT_HEATER_FOR_ARCADIA = 'yt-heater-for-arcadia'

BUILD_CLASSES = (
    UNDEFINED, LARGE_TEST, REVIEW_LARGE_TEST, REVIEW_LOW_CACHE_HIT,
    AUTOCHECK_BRANCH_POSTCOMMIT, AUTOCHECK_BRANCH_POSTCOMMIT_RECHECK, AUTOCHECK_TRUNK_POSTCOMMIT, AUTOCHECK_TRUNK_POSTCOMMIT_RECHECK,
    AUTOCHECK_BRANCH_PRECOMMIT, AUTOCHECK_BRANCH_PRECOMMIT_RECHECK,
    REVIEW_PESSIMIZED,
    AUTOCHECK_TRUNK_PRECOMMIT, AUTOCHECK_TRUNK_PRECOMMIT_RECHECK,
    RECHECK, REVIEW_BOARD,
    YA_DEV, FUZZING, YA_USE_WORKING_COPY_REVISION, AUTOCHECK_ACCEPTANCE, AUTOCHECK, YNDEXER, REVIEW_SANDBOX_TASK, YT_HEATER_FOR_ARCADIA, SANDBOX_TASK, TEAMCITY, USER_BUILD,
    COVERAGE_NIGHTLY, COVERAGE_NIGHTLY_LARGE_SANDBOX, COVERAGE_REVIEW, COVERAGE_REVIEW_LARGE_SANDBOX,
    GRAPH_GENERATION_PRE_COMMIT, GRAPH_GENERATION_POST_COMMIT, GRAPH_GENERATION_CI, GRAPH_GENERATION_OTHER,
)

ORDERED_BUILD_CLASSES = (
    AUTOCHECK_TRUNK_PRECOMMIT,
    AUTOCHECK_TRUNK_PRECOMMIT_RECHECK,
    REVIEW_PESSIMIZED,
    REVIEW_LOW_CACHE_HIT,
    REVIEW_BOARD,
    RECHECK,
    GRAPH_GENERATION_PRE_COMMIT,
    GRAPH_GENERATION_POST_COMMIT,
    GRAPH_GENERATION_CI,
    AUTOCHECK,
    AUTOCHECK_TRUNK_POSTCOMMIT,
    AUTOCHECK_TRUNK_POSTCOMMIT_RECHECK,
    GRAPH_GENERATION_OTHER,
    AUTOCHECK_ACCEPTANCE,
    FUZZING,
    YNDEXER,
    COVERAGE_NIGHTLY_LARGE_SANDBOX,
    COVERAGE_NIGHTLY,
    COVERAGE_REVIEW_LARGE_SANDBOX,
    COVERAGE_REVIEW,
    AUTOCHECK_BRANCH_PRECOMMIT,
    AUTOCHECK_BRANCH_PRECOMMIT_RECHECK,
    AUTOCHECK_BRANCH_POSTCOMMIT,
    AUTOCHECK_BRANCH_POSTCOMMIT_RECHECK,
    REVIEW_LARGE_TEST,
    REVIEW_SANDBOX_TASK,
    YT_HEATER_FOR_ARCADIA,
    SANDBOX_TASK,
    USER_BUILD,
    TEAMCITY,
    LARGE_TEST,
    YA_DEV,
    YA_USE_WORKING_COPY_REVISION,
    UNDEFINED,
)


def classify_gsid(data):
    gsid = data.decode('utf-8') if isinstance(data, six.binary_type) else data
    result = {}
    for item in gsid.split():
        if item.startswith('TE:autocheck-trunk:FAT_'):
            result[LARGE_TEST] = True
        elif item.startswith('TE:') and item[item.find(':', len('TE:')):].startswith(':FAT_'):
            result[REVIEW_LARGE_TEST] = True
        elif item.startswith('ARCANUM:') or item.startswith('RB:'):
            result[REVIEW_BOARD] = True
        elif item.startswith('TE:ya:AUTOCHECK_YA_DEV:'):
            result[YA_DEV] = True
        elif item.startswith('TE:fuzzing'):
            result[FUZZING] = True
        elif item.startswith('COVERAGE:LARGE_SANDBOX'):
            result[COVERAGE_LARGE_SANDBOX] = True
        elif item.startswith('SB:COVERAGE_YA_MAKE'):
            result[COVERAGE] = True
        elif item.startswith('SB:AUTOCHECK_BUILD_PARENT') or item.startswith('SB:AUTOCHECK_BUILD_PARENT_2'):
            result[AUTOCHECK] = True
        elif item.startswith('SB:YNDEXER'):
            result[YNDEXER] = True
        elif item.startswith('SB:CHECK_AUTOBUILD_TIME'):
            result[REVIEW_BOARD] = True
        elif (item.startswith('SB:AUTOCHECK_BUILD_YA') or item.startswith('SB:AUTOCHECK_BUILD_YA_2')) and item.endswith('LOW_CACHE_HIT'):
            result[REVIEW_LOW_CACHE_HIT] = True
        elif item.startswith('SB:PESSIMIZED'):
            result[REVIEW_PESSIMIZED] = True
        elif item.startswith('SB:YT_HEATER_FOR_ARCADIA'):
            result[YT_HEATER_FOR_ARCADIA] = True
        elif item.startswith('SB:') and not item.startswith('SB:AUTOCHECK_BUILD_') and not item.startswith('SB:AUTOCHECK_FROM_REVIEW_BOARD'):
            result[SANDBOX_TASK] = True
        elif item.startswith('YA:USE_WORKING_COPY_REVISION'):
            result[YA_USE_WORKING_COPY_REVISION] = True
        elif item.startswith('YA:'):
            result[USER_BUILD] = True
        elif item == 'USER:teamcity':
            result[TEAMCITY] = True
        elif item.startswith('TE:') and item[item.find(':', len('TE:')):].startswith(':AUTOCHECK_RECHECK_'):
            result[RECHECK] = True
        elif item.startswith('TE:autocheck_acceptance:AUTOCHECK_ACCEPTANCE') or item.startswith('SB:AUTOCHECK_ACCEPTANCE'):
            result[AUTOCHECK_ACCEPTANCE] = True
        elif item.startswith('GG:'):
            result[GRAPH_GENERATION] = True
        elif item.startswith('CI:autocheck/a.yaml:ACTION:autocheck-branch-precommits:'):
            result[AUTOCHECK_BRANCH_PRECOMMIT] = True
        elif item.startswith('CI:autocheck/a.yaml:ACTION:autocheck-branch-precommits-recheck:'):
            result[AUTOCHECK_BRANCH_PRECOMMIT_RECHECK] = True
        elif item.startswith('CI:autocheck/a.yaml:ACTION:autocheck-trunk-precommits:'):
            result[AUTOCHECK_TRUNK_PRECOMMIT] = True
        elif item.startswith('CI:autocheck/a.yaml:ACTION:autocheck-trunk-precommits-recheck:'):
            result[AUTOCHECK_TRUNK_PRECOMMIT_RECHECK] = True
        elif item.startswith('CI:autocheck/a.yaml:ACTION:autocheck-branch-postcommits:'):
            result[AUTOCHECK_BRANCH_POSTCOMMIT] = True
        elif item.startswith('CI:autocheck/a.yaml:ACTION:autocheck-branch-postcommits-recheck:'):
            result[AUTOCHECK_BRANCH_POSTCOMMIT_RECHECK] = True
        elif item.startswith('CI:autocheck/a.yaml:ACTION:autocheck-trunk-postcommits:'):
            result[AUTOCHECK_TRUNK_POSTCOMMIT] = True
        elif item.startswith('CI:autocheck/a.yaml:ACTION:autocheck-trunk-postcommits-recheck:'):
            result[AUTOCHECK_TRUNK_POSTCOMMIT_RECHECK] = True

    if result.get(GRAPH_GENERATION):
        if result.get(REVIEW_BOARD):
            return GRAPH_GENERATION_PRE_COMMIT
        if result.get(AUTOCHECK):
            return GRAPH_GENERATION_POST_COMMIT
        return GRAPH_GENERATION_OTHER

    if result.get(COVERAGE):
        if result.get(COVERAGE_LARGE_SANDBOX):
            return COVERAGE_REVIEW_LARGE_SANDBOX if result.get(REVIEW_BOARD) else COVERAGE_NIGHTLY_LARGE_SANDBOX
        else:
            return COVERAGE_REVIEW if result.get(REVIEW_BOARD) else COVERAGE_NIGHTLY

    if result.get(REVIEW_BOARD) and result.get(SANDBOX_TASK) and not result.get(REVIEW_LARGE_TEST):
        return REVIEW_SANDBOX_TASK

    for build_type in BUILD_CLASSES:
        if result.get(build_type):
            return build_type
    return UNDEFINED
