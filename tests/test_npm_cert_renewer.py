import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from NpmCertRenewer import NpmCertRenewer


class TestNpmCertRenewer(unittest.TestCase):
    def setUp(self):
        NpmCertRenewer.clear_cooldowns()
        self._env_patcher = patch.dict(
            os.environ,
            {
                "NPM_URL": "http://nginx:81",
                "NPM_IDENTITY": "admin@csctracker.com",
                "NPM_SECRET": "secret123",
                "NPM_RENEW_THRESHOLD_DAYS": "15",
                "NPM_IGNORE_DOMAINS": "",
                "NPM_COOLDOWN_HOURS": "12",
                "NPM_REQUEST_TIMEOUT": "120",
            },
            clear=False,
        )
        self._env_patcher.start()

    def tearDown(self):
        self._env_patcher.stop()
        NpmCertRenewer.clear_cooldowns()

    def test_missing_credentials_returns_error(self):
        with patch.dict(os.environ, {"NPM_IDENTITY": "", "NPM_SECRET": ""}, clear=False):
            renewer = NpmCertRenewer()
            result = renewer.renew_certificates()
            self.assertEqual(result["status"], "error")
            self.assertIn("NPM_IDENTITY/NPM_SECRET not configured", result["message"])
            self.assertEqual(result["checked"], 0)

    @patch("NpmCertRenewer.requests.post")
    @patch("NpmCertRenewer.requests.get")
    def test_renews_only_due_and_expired_letsencrypt_certificates(self, mock_get, mock_post):
        now = datetime.now(timezone.utc)
        expired_str = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
        due_soon_str = (now + timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        healthy_str = (now + timedelta(days=75)).strftime("%Y-%m-%d %H:%M:%S")
        renewed_str = (now + timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")

        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: [
                {
                    "id": 2,
                    "provider": "letsencrypt",
                    "nice_name": "arquivos.mycshtwin.com",
                    "domain_names": ["arquivos.mycshtwin.com"],
                    "expires_on": expired_str,
                },
                {
                    "id": 8,
                    "provider": "letsencrypt",
                    "nice_name": "build.csctracker.com",
                    "domain_names": ["build.csctracker.com"],
                    "expires_on": due_soon_str,
                },
                {
                    "id": 1,
                    "provider": "letsencrypt",
                    "nice_name": "agy.mycshtwin.com",
                    "domain_names": ["agy.mycshtwin.com"],
                    "expires_on": healthy_str,
                },
            ],
        )

        def post_side_effect(url, **kwargs):
            if url.endswith("/api/tokens"):
                return MagicMock(status_code=200, json=lambda: {"token": "jwt-token-123"})
            if url.endswith("/api/nginx/certificates/2/renew") or url.endswith("/api/nginx/certificates/8/renew"):
                return MagicMock(
                    status_code=200,
                    text='{"expires_on": "' + renewed_str + '"}',
                    json=lambda: {"expires_on": renewed_str},
                )
            raise AssertionError(f"Unexpected POST url: {url}")

        mock_post.side_effect = post_side_effect

        renewer = NpmCertRenewer()
        result = renewer.renew_certificates()

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["checked"], 3)
        self.assertEqual(result["eligible"], 2)
        self.assertEqual([c["id"] for c in result["renewed"]], [2, 8])
        self.assertEqual(len(result["skipped"]), 1)
        self.assertEqual(result["skipped"][0]["id"], 1)
        self.assertEqual(result["skipped"][0]["reason"], "not_due")
        self.assertEqual(len(result["failed"]), 0)

    @patch("NpmCertRenewer.requests.post")
    @patch("NpmCertRenewer.requests.get")
    def test_error_isolation_and_cooldown_with_force_bypass(self, mock_get, mock_post):
        now = datetime.now(timezone.utc)
        expired_str = (now - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        renewed_str = (now + timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")

        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: [
                {
                    "id": 10,
                    "provider": "letsencrypt",
                    "domain_names": ["fail.csctracker.com"],
                    "expires_on": expired_str,
                },
                {
                    "id": 11,
                    "provider": "letsencrypt",
                    "domain_names": ["ok.csctracker.com"],
                    "expires_on": expired_str,
                },
            ],
        )

        def post_first_run(url, **kwargs):
            if url.endswith("/api/tokens"):
                return MagicMock(status_code=200, json=lambda: {"token": "jwt-token"})
            if url.endswith("/certificates/10/renew"):
                return MagicMock(status_code=500, text="Certbot DNS challenge failed")
            if url.endswith("/certificates/11/renew"):
                return MagicMock(
                    status_code=200,
                    text='{"expires_on": "' + renewed_str + '"}',
                    json=lambda: {"expires_on": renewed_str},
                )
            raise AssertionError(f"Unexpected POST url: {url}")

        mock_post.side_effect = post_first_run
        renewer = NpmCertRenewer()

        # First run: cert 10 fails, cert 11 succeeds
        res1 = renewer.renew_certificates()
        self.assertEqual(res1["status"], "ok")
        self.assertEqual([c["id"] for c in res1["failed"]], [10])
        self.assertEqual([c["id"] for c in res1["renewed"]], [11])

        # Second run immediately without force: cert 10 is skipped due to cooldown
        res2 = renewer.renew_certificates()
        skipped_reasons = {c["id"]: c["reason"] for c in res2["skipped"]}
        self.assertEqual(skipped_reasons.get(10), "cooldown")

        # Third run with force=True: bypasses cooldown and retries cert 10
        def post_force_run(url, **kwargs):
            if url.endswith("/api/tokens"):
                return MagicMock(status_code=200, json=lambda: {"token": "jwt-token"})
            return MagicMock(
                status_code=200,
                text='{"expires_on": "' + renewed_str + '"}',
                json=lambda: {"expires_on": renewed_str},
            )

        mock_post.side_effect = post_force_run
        res3 = renewer.renew_certificates(force=True, cert_id=10)
        self.assertEqual([c["id"] for c in res3["renewed"]], [10])

    @patch("NpmCertRenewer.requests.post")
    @patch("NpmCertRenewer.requests.get")
    def test_filters_ignored_domains_and_non_letsencrypt_providers(self, mock_get, mock_post):
        now = datetime.now(timezone.utc)
        expired_str = (now - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")

        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: [
                {
                    "id": 20,
                    "provider": "other",
                    "domain_names": ["custom.csctracker.com"],
                    "expires_on": expired_str,
                },
                {
                    "id": 21,
                    "provider": "letsencrypt",
                    "domain_names": ["legacy.mycshtwin.com"],
                    "expires_on": expired_str,
                },
            ],
        )
        mock_post.return_value = MagicMock(status_code=200, json=lambda: {"token": "jwt-token"})

        with patch.dict(os.environ, {"NPM_IGNORE_DOMAINS": "legacy.mycshtwin.com, test.local"}, clear=False):
            renewer = NpmCertRenewer()
            result = renewer.renew_certificates()

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["eligible"], 0)
        reasons = {c["id"]: c["reason"] for c in result["skipped"]}
        self.assertEqual(reasons[20], "non_letsencrypt")
        self.assertEqual(reasons[21], "ignored_domain")

    def test_app_route_renew_certificates_post_and_get(self):
        with patch("csctracker_py_core.starter.Starter.start"), patch(
            "csctracker_py_core.starter.SchedulerService.init"
        ), patch("ScheduleJobs.ScheduleJobs.init"):
            import app as app_module

        client = app_module.app.test_client()
        expected_payload = {
            "status": "ok",
            "checked": 1,
            "eligible": 1,
            "renewed": [{"id": 2, "domains": ["arquivos.mycshtwin.com"]}],
            "skipped": [],
            "failed": [],
        }
        with patch.object(
            app_module.npm_cert_renewer,
            "renew_certificates",
            return_value=expected_payload,
        ) as mock_renew:
            resp = client.post(
                "/npm/renew-certificates",
                json={"threshold_days": 10, "force": True, "cert_id": 2},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json(), expected_payload)
            mock_renew.assert_called_once_with(
                threshold_days=10,
                force=True,
                cert_id=2,
            )


if __name__ == "__main__":
    unittest.main()
