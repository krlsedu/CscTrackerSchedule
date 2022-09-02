import time

import requests
import schedule

from Interceptor import Interceptor
from Repository import GenericRepository

generic_repository = GenericRepository()


class ScheduleJobs(Interceptor):
    def __init__(self):
        super().__init__()

    def init(self):
        jobs = generic_repository.get_objects("http_schedule", ["is_active"], {"is_active": "S"})
        for job in jobs:
            print("Scheduling ->" + job['name'])
            print(job)
            every = job['every']
            if every == 'seconds':
                schedule.every(int(job['period'])).seconds.do(self.http_request, job['url'], job['method'], job['body'])
            elif every == 'minutes':
                schedule.every(int(job['period'])).minutes.do(self.http_request, job['url'], job['method'], job['body'])
            elif every == 'hours':
                schedule.every(int(job['period'])).hours.do(self.http_request, job['url'], job['method'], job['body'])
            elif every == 'days':
                schedule.every(int(job['period'])).days.do(self.http_request, job['url'], job['method'], job['body'])
            elif every == 'weeks':
                schedule.every(int(job['period'])).weeks.do(self.http_request, job['url'], job['method'], job['body'])
            elif every == 'day':
                schedule.every().day.at(job['period']).do(self.http_request, job['url'], job['method'], job['body'])
        while True:
            schedule.run_pending()
            time.sleep(1)
        pass

    def http_request(self, url, method="GET", body={}, params={}):
        try:
            if method == "GET":
                print("GET -> " + url)
                response = requests.get(url, params=params)
            elif method == "POST":
                print("POST -> " + url)
                response = requests.post(url, json=body)
            else:
                raise Exception("Method not supported")
            if response.status_code != 200:
                print(url, response.status_code)
                print(f'Error sending metrics: {response.text}')
        except Exception as e:
            print(e)
        pass
