import json
import logging
import threading
import uuid
from datetime import datetime

from csctracker_py_core.repository.http_repository import HttpRepository
from csctracker_py_core.repository.remote_repository import RemoteRepository
from csctracker_py_core.utils.request_info import RequestInfo
from csctracker_queue_scheduler.models.enums.time_unit import TimeUnit
from csctracker_queue_scheduler.services.scheduler_service import SchedulerService


class ScheduleJobs:
    def __init__(self, remote_repository: RemoteRepository, http_repository: HttpRepository):
        self.logger = logging.getLogger()
        self.remote_repository = remote_repository
        self.http_repository = http_repository
        pass

    def init(self):
        headers = {
            'authorization': 'Bearer ' + self.http_repository.get_api_token(),
            'x-correlation-id': f"scheduler-{str(uuid.uuid4())}"
        }
        jobs = self.remote_repository.get_objects(
            "http_schedule",
            data={"is_active": "S"},
            headers=headers)

        for job in jobs:
            try:
                period = int(job['period'])
            except:
                period = str(job['period'])
            every_ = job['every']
            if every_.lower() == 'day':
                every_ = 'DAILY'
                time_unit = getattr(TimeUnit, every_.upper())
                args_ = {
                    "url": job["url"],
                    "method": job["method"],
                    "body": json.loads(job["body"]),
                    "token": job["token"],
                    "job_name": job["name"]
                }
            else:
                time_unit = getattr(TimeUnit, every_.upper())
                args_ = {
                    "url": job["url"],
                    "method": job["method"],
                    "body": json.loads(job["body"]),
                    "token": job["token"],
                    "job_name": job["name"]
                }
            SchedulerService.start_scheduled_job(self.http_request, args=args_, period=period, time_unit=time_unit)

    def http_request(self, url, method="GET", body=None, token=None, params={}, job_name=None):
        try:
            thread = threading.current_thread()
            id_ = RequestInfo.get_request_id()
            job_name = job_name.replace(" ", "-").lower()
            now_yyyy_mm_dd_hh_mm_ss_milis = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
            id_ = f"{job_name}-scheduled-{now_yyyy_mm_dd_hh_mm_ss_milis}_{id_}"
            thread.__setattr__('correlation_id', id_)
            headers = {
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
                'x-correlation-id': id_
            }

            if method == "GET":
                response = self.http_repository.get(url, params=params, headers=headers)
            elif method == "POST":
                if body is not None and not isinstance(body, str):
                    try:
                        body = json.loads(body)
                    except:
                        pass
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
        pass
