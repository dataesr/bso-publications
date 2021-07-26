from bso.server.main.affiliation_matcher import check_matcher_health
from bso.server.main.config import AFFILIATION_MATCHER_SERVICE


class TestAffiliationMatcher:
    def test_check_matcher_health(self, requests_mock) -> None:
        url = f'{AFFILIATION_MATCHER_SERVICE}/match_api'
        requests_mock.post(url=url, json={'results': []})
        result = check_matcher_health()
        assert result is True

