"""check_ckan_response: say who refused a CKAN call, and don't retry a 403."""

import dagster as dg
import pytest
import requests

from wprdc_etl.resources import check_ckan_response


def _resp(
    status: int, body: str, content_type: str, server: str = ""
) -> requests.Response:
    r = requests.Response()
    r.status_code = status
    r._content = body.encode()
    r.headers["content-type"] = content_type
    if server:
        r.headers["server"] = server
    return r


def test_ok_passes() -> None:
    check_ckan_response(_resp(200, '{"success": true}', "application/json"), "x")


def test_a_waf_403_says_so_and_is_not_retried() -> None:
    html = "<html><title>Attention Required!</title>  blocked  </html>"
    with pytest.raises(dg.Failure) as e:
        check_ckan_response(
            _resp(403, html, "text/html", server="cloudflare"), "resource_patch"
        )
    assert e.value.allow_retries is False
    assert "WAF" in e.value.description
    assert "cloudflare" in e.value.description
    assert "Attention Required" in e.value.description


def test_ckans_own_403_names_the_ckan_error() -> None:
    body = '{"success": false, "error": {"__type": "Authorization Error"}}'
    with pytest.raises(dg.Failure) as e:
        check_ckan_response(_resp(403, body, "application/json"), "resource_patch")
    assert "CKAN refused it" in e.value.description
    assert "Authorization Error" in e.value.description
    assert e.value.allow_retries is False


def test_a_5xx_stays_retryable() -> None:
    with pytest.raises(requests.HTTPError) as e:
        check_ckan_response(_resp(502, "Bad Gateway", "text/html"), "package_show")
    assert "HTTP 502" in str(e.value)


def _ckan_answering(monkeypatch: pytest.MonkeyPatch, resp: requests.Response):
    from wprdc_etl.resources import CkanResource

    monkeypatch.setattr(requests, "post", lambda *a, **k: resp)
    return CkanResource(base_url="http://localhost:5001", api_key="t")


def test_no_datastore_table_is_a_first_load(monkeypatch: pytest.MonkeyPatch) -> None:
    body = '{"success": false, "error": {"__type": "Not Found Error"}}'
    ckan = _ckan_answering(monkeypatch, _resp(404, body, "application/json"))
    assert ckan._datastore_state("rid") == ({}, 0)


def test_a_refused_lookup_is_not_mistaken_for_a_first_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ckan = _ckan_answering(monkeypatch, _resp(403, "<html>blocked</html>", "text/html"))
    with pytest.raises(dg.Failure):
        ckan._datastore_state("rid")


def test_an_outage_is_not_mistaken_for_a_first_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ckan = _ckan_answering(monkeypatch, _resp(503, "unavailable", "text/html"))
    with pytest.raises(requests.HTTPError):
        ckan._datastore_state("rid")
