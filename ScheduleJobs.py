import logging

from csctracker_py_core.repository.http_repository import HttpRepository
from csctracker_py_core.repository.remote_repository import RemoteRepository
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
            'authorization': 'Bearer ' + self.http_repository.get_api_token()
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
                    "body": job["body"],
                    "token": job["token"]
                }
            else:
                time_unit = getattr(TimeUnit, every_.upper())
                args_ = {
                    "url": job["url"],
                    "method": job["method"],
                    "body": job["body"],
                    "token": job["token"]
                }
            SchedulerService.start_scheduled_job(self.http_request, args=args_, period=period, time_unit=time_unit)

    def http_request(self, url, method="GET", body={}, token=None, params={}):
        try:
            headers = {
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json"
            }
            if method == "GET":
                response = self.http_repository.get(url, params=params, headers=headers)
            elif method == "POST":
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
