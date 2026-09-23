"""Integration tests for trusting the system certificate store in the HTTP client (issue #846).

They bring up a real local YTsaurus cluster with TLS enabled (a self-signed CA is generated on
the fly and the HTTP proxy serves HTTPS with a certificate signed by it) and connect the real
``yt.wrapper`` Python client to the HTTPS proxy.

The cluster CA is exposed to the operating system trust store (consulted by ``ssl.create_default_context``)
via the ``SSL_CERT_FILE`` environment variable rather than installed system-wide (which would need
root). Each scenario runs in its own subprocess (``files/https_client_probe.py``) so that the
client builds a fresh session against the chosen configuration.
"""

import os
import subprocess
import sys

import pytest

from yt.testlib import authors

from yt.wrapper.testlib.conftest_helpers import init_environment_for_test_session, _yt_env
from yt.wrapper.testlib.helpers import get_python, get_test_file_path


# SSL_CERT_FILE only seeds the OpenSSL/system trust store on Linux; the cluster cluster fixture
# itself runs only on Linux CI, but skip explicitly for the cases that rely on the variable.
linux_only = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="SSL_CERT_FILE is OpenSSL/Linux-specific")


@pytest.fixture(scope="class", params=["v4"])
def test_environment_tls(request):
    return init_environment_for_test_session(
        request,
        request.param,
        env_options={"enable_tls": True},
    )


@pytest.fixture(scope="function")
def yt_env_tls(request, test_environment_tls):
    return _yt_env(request, test_environment_tls)


def _probe_https_client(env, use_system_ca, ca_bundle_path=None, seed_system_ca=False, requests_ca_bundle_env=None):
    subprocess_env = dict(os.environ)
    if requests_ca_bundle_env:
        subprocess_env["REQUESTS_CA_BUNDLE"] = requests_ca_bundle_env
    else:
        subprocess_env.pop("REQUESTS_CA_BUNDLE", None)
    if seed_system_ca:
        # Put the cluster CA into the trust store that ssl.create_default_context() consults.
        subprocess_env["SSL_CERT_FILE"] = env.yt_config.public_ca_cert
    else:
        subprocess_env.pop("SSL_CERT_FILE", None)

    command = [get_python(), get_test_file_path("https_client_probe.py"), "--proxy-url", env.get_https_proxy_url()]
    if use_system_ca:
        command.append("--use-system-ca")
    if ca_bundle_path:
        command += ["--ca-bundle-path", ca_bundle_path]

    return subprocess.run(
        command,
        env=subprocess_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=120,
    )


@pytest.mark.usefixtures("yt_env_tls")
class TestHttpsSystemCa(object):
    @linux_only
    @authors("desertfury")
    def test_system_ca_disabled_rejects_cluster_cert(self, yt_env_tls):
        # Reproduces issue #846: without the fix (use_system_ca=False) the client verifies only
        # against the certifi bundle and rejects the cluster's self-signed certificate, even though
        # its CA is present in the system trust store (SSL_CERT_FILE).
        proc = _probe_https_client(yt_env_tls.env, use_system_ca=False, seed_system_ca=True)
        assert proc.returncode != 0
        assert "verify" in proc.stderr.lower() or "ca certificate" in proc.stderr.lower(), proc.stderr

    @linux_only
    @authors("desertfury")
    def test_system_ca_trusts_cluster_cert(self, yt_env_tls):
        # The fix: with use_system_ca the client trusts the operating system certificate store
        # (seeded via SSL_CERT_FILE) and accepts the self-signed certificate, no ca_bundle_path.
        proc = _probe_https_client(yt_env_tls.env, use_system_ca=True, seed_system_ca=True)
        assert proc.returncode == 0, proc.stderr

    @authors("desertfury")
    def test_system_ca_honors_requests_ca_bundle_env(self, yt_env_tls):
        # use_system_ca trusts a CA supplied only via the REQUESTS_CA_BUNDLE environment variable
        # (as requests itself would), even when it is absent from the system trust store. The
        # variable is folded into the ssl_context rather than read by requests.
        proc = _probe_https_client(
            yt_env_tls.env,
            use_system_ca=True,
            seed_system_ca=False,
            requests_ca_bundle_env=yt_env_tls.env.yt_config.public_ca_cert,
        )
        assert proc.returncode == 0, proc.stderr

    @authors("desertfury")
    def test_system_ca_rejects_untrusted_cert(self, yt_env_tls):
        # Sanity: use_system_ca does not blindly trust everything — a certificate whose CA is in
        # neither the system store nor ca_bundle_path is still rejected.
        proc = _probe_https_client(yt_env_tls.env, use_system_ca=True, seed_system_ca=False)
        assert proc.returncode != 0
        assert "verify" in proc.stderr.lower(), proc.stderr

    @authors("desertfury")
    def test_ca_bundle_path_still_works(self, yt_env_tls):
        # Control: the pre-existing proxy/ca_bundle_path knob keeps working (both with and without
        # the system-CA path), so the change does not regress explicit CA bundles.
        for use_system_ca in (False, True):
            proc = _probe_https_client(
                yt_env_tls.env,
                use_system_ca=use_system_ca,
                ca_bundle_path=yt_env_tls.env.yt_config.public_ca_cert,
            )
            assert proc.returncode == 0, proc.stderr
