import threading
from flask import Flask
from flask_cors import CORS

from ScheduleJobs import ScheduleJobs

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


schedule_jobs = ScheduleJobs()


def schedule_job():
    schedule_jobs.init()


t1 = threading.Thread(target=schedule_job, args=())
t1.start()
print('done')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
