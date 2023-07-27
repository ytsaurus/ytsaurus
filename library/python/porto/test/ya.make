OWNER(g:rtc-sysdev)

PY23_LIBRARY()

TEST_SRCS(test_api.py)

PEERDIR(library/python/porto)

END()

RECURSE(
  local-py2
  local-py3
  rtc-precise-py2
  rtc-precise-py3
  rtc-xenial-py2
  rtc-xenial-py3
  rtc-precise-ng-py2
  rtc-precise-ng-py3
  rtc-xenial-ng-py2
  rtc-xenial-ng-py3
)
