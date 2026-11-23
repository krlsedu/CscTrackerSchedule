import json
import logging
import os
import threading
import uuid
from datetime import datetime

from csctracker_py_core.repository.http_repository import HttpRepository
from csctracker_py_core.repository.remote_repository import RemoteRepository
from csctracker_py_core.utils.request_info import RequestInfo
from csctracker_queue_scheduler.models.enums.time_unit import TimeUnit
from csctracker_queue_scheduler.services.scheduler_service import SchedulerService


class ScheduleJobs:
    NPM_RENEW_PATH = "/npm/renew-certificates"

    def __init__(self, remote_repository: RemoteRepository, http_repository: HttpRepository):
        self.logger = logging.getLogger()
        self.remote_repository = remote_repository
        self.http_repository = http_repository

    @staticmethod
    def _parse_job_body(raw_body):
        if raw_body is None or raw_body == "":
            return {}
        if isinstance(raw_body, dict):
            return raw_body
        if isinstance(raw_body, str):
            text = raw_body.strip()
            if not text:
                return {}
            try:
                return json.loads(text)
            except Exception:
                return {}
        return {}

    def _ensure_npm_schedule_job(self, jobs, headers, api_token):
        auto_register = os.getenv("NPM_AUTO_REGISTER_SCHEDULE", "true").strip().lower() == "true"
        if not auto_register:
            return jobs

        has_npm_job = any(
            isinstance(job, dict) and self.NPM_RENEW_PATH in str(job.get("url", ""))
            for job in jobs
        )
        if has_npm_job:
            return jobs

        npm_identity = os.getenv("NPM_IDENTITY", "").strip()
        npm_secret = os.getenv("NPM_SECRET", "").strip()
        if not npm_identity or not npm_secret:
            return jobs

        new_job = {
            "name": "Renew NPM SSL Certificates",
            "url": os.getenv("NPM_SCHEDULE_URL", f"http://schedule:5000{self.NPM_RENEW_PATH}"),
            "method": "POST",
            "body": "{}",
            "token": api_token,
            "every": "day",
            "period": os.getenv("NPM_SCHEDULE_PERIOD", "04:00"),
            "is_active": "S",
        }
        try:
            inserted = self.remote_repository.insert("http_schedule", data=new_job, headers=headers)
            job_to_append = inserted if isinstance(inserted, dict) and inserted.get("url") else new_job
            jobs.append(job_to_append)
            self.logger.info(
                f"[ScheduleJobs] Auto-registered default NPM SSL renewal schedule in http_schedule "
                f"(period={new_job['period']})"
            )
        except Exception as e:
            self.logger.warning(f"[ScheduleJobs] Could not auto-register NPM schedule in http_schedule: {e}")

        return jobs

    def init(self):
        api_token = self.http_repository.get_api_token() or ""
        headers = {
            'authorization': 'Bearer ' + api_token,
            'x-correlation-id': f"scheduler-{str(uuid.uuid4())}"
        }
        jobs = self.remote_repository.get_objects(
            "http_schedule",
            data={"is_active": "S"},
            headers=headers)
        if not isinstance(jobs, list):
            jobs = []

        jobs = self._ensure_npm_schedule_job(jobs, headers, api_token)

        for job in jobs:
            try:
                period = int(job['period'])
            except Exception:
                period = str(job['period'])
            every_ = str(job['every'])
            if every_.lower() == 'day':
                every_ = 'DAILY'
            time_unit = getattr(TimeUnit, every_.upper())
            args_ = {
                "url": job["url"],
                "method": job["method"],
                "body": self._parse_job_body(job.get("body")),
                "token": job.get("token") or "",
                "job_name": job["name"]
            }
            SchedulerService.start_scheduled_job(self.http_request, args=args_, period=period, time_unit=time_unit)

    def http_request(self, url, method="GET", body=None, token=None, params={}, job_name=None):
        try:
            thread = threading.current_thread()
            id_ = RequestInfo.get_request_id()
            safe_job_name = (job_name or "job").replace(" ", "-").lower()
            now_yyyy_mm_dd_hh_mm_ss_milis = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            id_ = f"{safe_job_name}-scheduled-{now_yyyy_mm_dd_hh_mm_ss_milis}_{id_}"
            thread.__setattr__('correlation_id', id_)
            headers = {
                "Authorization": "Bearer " + (token or ""),
                "Content-Type": "application/json",
                'x-correlation-id': id_
            }

            if method == "GET":
                response = self.http_repository.get(url, params=params, headers=headers)
            elif method == "POST":
                if isinstance(body, str):
                    body = self._parse_job_body(body)
                elif body is None:
                    body = {}
                response = self.http_repository.post(url, body=body, headers=headers)
            else:
                raise Exception("Method not supported")
            if response.status_code < 200 or response.status_code > 299:
                self.logger.debug(url, response.status_code)
                self.logger.error(f'Error sending metrics: {response.text}')
            else:
                self.logger.debug(url, "OK")
        except Exception as e:
            self.logger.exception(e)
