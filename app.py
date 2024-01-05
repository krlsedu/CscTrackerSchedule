from csctracker_py_core.starter import Starter

from ScheduleJobs import ScheduleJobs

starter = Starter()
schedule_jobs = ScheduleJobs(starter.get_remote_repository(), starter.get_http_repository())
schedule_jobs.init()


if __name__ == '__main__':
    starter.start()
