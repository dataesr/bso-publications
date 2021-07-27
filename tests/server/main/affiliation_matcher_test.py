from requests.models import Response

from bso.server.main.affiliation_matcher import check_matcher_data_is_loaded, check_matcher_health, load_matcher_data
from bso.server.main.config import AFFILIATION_MATCHER_SERVICE


class TestAffiliationMatcher:
    def test_load_matcher_data_true(self, requests_mock) -> None:
        url = f'{AFFILIATION_MATCHER_SERVICE}/load'
        requests_mock.get(url=url, json={})
        result = load_matcher_data()
        assert result is True

    def test_load_matcher_data_false(self) -> None:
        result = load_matcher_data()
        assert result is False

    def test_check_matcher_data_is_loaded_true(self) -> None:
        response = Response()
        response._content = b'{ "results" : [] }'
        result = check_matcher_data_is_loaded(response)
        assert result is True

    def test_check_matcher_data_is_loaded_false(self, mocker) -> None:
        mocker.patch('bso.server.main.affiliation_matcher.load_matcher_data', return_value=42)
        response = Response()
        response._content = b'{ "no-results" : [] }'
        result = check_matcher_data_is_loaded(response)
        assert result == 42

    def test_check_matcher_health_true(self, mocker, requests_mock) -> None:
        mocker.patch('bso.server.main.affiliation_matcher.check_matcher_data_is_loaded', return_value=42)
        url = f'{AFFILIATION_MATCHER_SERVICE}/match_api'
        requests_mock.post(url=url, json={})
        result = check_matcher_health()
        assert result == 42

    def test_check_matcher_health_false(self) -> None:
        result = check_matcher_health()
        assert result is False

