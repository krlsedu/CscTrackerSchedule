from csctracker_py_core.starter import Starter
from flask import jsonify, request

from NpmCertRenewer import NpmCertRenewer
from ScheduleJobs import ScheduleJobs

starter = Starter()
app = starter.get_app()
http_repository = starter.get_http_repository()
npm_cert_renewer = NpmCertRenewer()

schedule_jobs = ScheduleJobs(starter.get_remote_repository(), http_repository)
schedule_jobs.init()


def _parse_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "s")


def _parse_int(value, default=None):
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@app.route('/npm/renew-certificates', methods=['POST', 'GET'])
@http_repository.get_cross_origin()()
def renew_npm_certificates():
    payload = request.get_json(silent=True) or {}
    args = request.args or {}

    threshold_days = _parse_int(
        payload.get("threshold_days", args.get("threshold_days")),
        default=None,
    )
    force = _parse_bool(
        payload.get("force", args.get("force")),
        default=False,
    )
    cert_id = _parse_int(
        payload.get("cert_id", args.get("cert_id")),
        default=None,
    )

    result = npm_cert_renewer.renew_certificates(
        threshold_days=threshold_days,
        force=force,
        cert_id=cert_id,
    )
    status_code = 500 if result.get("status") == "error" else 200
    return jsonify(result), status_code


if __name__ == '__main__':
    starter.start()
