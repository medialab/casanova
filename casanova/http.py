# =============================================================================
# Casanova HTTP Helpers
# =============================================================================
#
# HTTP Helpers working with optional urllib3 & certifi deps.
#
try:
    import urllib3
except ImportError:
    urllib3 = None

try:
    import certifi
except ImportError:
    certifi = None

from casanova.exceptions import NoHTTPSupportError

urllib3_installed = urllib3 is not None
certifi_installed = certifi is not None

pool_manager = None

if urllib3_installed:
    manager_kwargs = {}

    if not certifi_installed:
        manager_kwargs["cert_reqs"] = "CERT_NONE"
    else:
        manager_kwargs["cert_reqs"] = "CERT_REQUIRED"
        manager_kwargs["ca_certs"] = certifi.where()

    pool_manager = urllib3.PoolManager(**manager_kwargs)


def request(url):
    if pool_manager is None:
        raise NoHTTPSupportError(
            "casanova is not able to make http calls. please install urllib3 (and certifi if you want secure HTTPS)"
        )

    response = pool_manager.request("GET", url, preload_content=False)

    # Ref: https://github.com/urllib3/urllib3/issues/1305
    response._fp.isclosed = lambda: False

    return response
