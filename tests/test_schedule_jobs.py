import os
import unittest
from unittest.mock import MagicMock, patch

from csctracker_queue_scheduler.models.enums.time_unit import TimeUnit
from ScheduleJobs import ScheduleJobs


class TestScheduleJobs(unittest.TestCase):
    @patch("ScheduleJobs.SchedulerService.start_scheduled_job")
    def test_init_schedules_active_jobs_and_auto_seeds_npm_job(self, mock_start_job):
        remote_repo = MagicMock()
        http_repo = MagicMock()
        http_repo.get_api_token.return_value = "test-api-token"

        existing_job = {
            "name": "Existing Metrics Job",
            "url": "http://metrics:5000/collect",
            "method": "POST",
            "body": '{"key": "value"}',
            "token": "tok-1",
            "every": "day",
            "period": "03:00",
            "is_active": "S",
        }
        remote_repo.get_objects.return_value = [existing_job]
        remote_repo.insert.side_effect = lambda table, data, headers=None: data

        with patch.dict(
            os.environ,
            {
                "NPM_IDENTITY": "admin@csctracker.com",
                "NPM_SECRET": "secret123",
                "NPM_AUTO_REGISTER_SCHEDULE": "true",
                "NPM_SCHEDULE_PERIOD": "04:00",
            },
            clear=False,
        ):
            scheduler = ScheduleJobs(remote_repo, http_repo)
            scheduler.init()

        remote_repo.insert.assert_called_once()
        inserted_table, inserted_kwargs = (
            remote_repo.insert.call_args[0][0],
            remote_repo.insert.call_args[1],
        )
        self.assertEqual(inserted_table, "http_schedule")
        self.assertEqual(
            inserted_kwargs["data"]["url"],
            "http://schedule:5000/npm/renew-certificates",
        )
        self.assertEqual(inserted_kwargs["data"]["period"], "04:00")
        self.assertEqual(mock_start_job.call_count, 2)

        first_call_kwargs = mock_start_job.call_args_list[0][1]
        self.assertEqual(first_call_kwargs["time_unit"], TimeUnit.DAILY)
        self.assertEqual(first_call_kwargs["args"]["body"], {"key": "value"})

    @patch("ScheduleJobs.SchedulerService.start_scheduled_job")
    def test_init_does_not_duplicate_when_npm_job_already_exists(self, mock_start_job):
        remote_repo = MagicMock()
        http_repo = MagicMock()
        http_repo.get_api_token.return_value = "test-api-token"

        npm_job = {
            "name": "Renew NPM SSL Certificates",
            "url": "http://schedule:5000/npm/renew-certificates",
            "method": "POST",
            "body": "",
            "token": "test-api-token",
            "every": "day",
            "period": "04:00",
            "is_active": "S",
        }
        remote_repo.get_objects.return_value = [npm_job]

        with patch.dict(
            os.environ,
            {"NPM_IDENTITY": "admin@csctracker.com", "NPM_SECRET": "secret123"},
            clear=False,
        ):
            scheduler = ScheduleJobs(remote_repo, http_repo)
            scheduler.init()

        remote_repo.insert.assert_not_called()
        self.assertEqual(mock_start_job.call_count, 1)
        self.assertEqual(mock_start_job.call_args[1]["args"]["body"], {})

    def test_http_request_handles_dict_and_string_body_cleanly(self):
        remote_repo = MagicMock()
        http_repo = MagicMock()
        http_repo.post.return_value = MagicMock(status_code=200, text="OK")

        scheduler = ScheduleJobs(remote_repo, http_repo)
        scheduler.http_request(
            url="http://schedule:5000/npm/renew-certificates",
            method="POST",
            body={"force": True},
            token="tok-123",
            job_name="Renew NPM SSL Certificates",
        )

        http_repo.post.assert_called_once()
        _, kwargs = http_repo.post.call_args
        self.assertEqual(kwargs["body"], {"force": True})
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer tok-123")


if __name__ == "__main__":
    unittest.main()
