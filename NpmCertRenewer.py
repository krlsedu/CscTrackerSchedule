import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests


class NpmCertRenewer:
    _failed_cooldown: Dict[int, datetime] = {}

    def __init__(self):
        self.logger = logging.getLogger()

    @staticmethod
    def _get_int_env(name: str, default: int) -> int:
        val = os.getenv(name)
        if val is None or str(val).strip() == "":
            return default
        try:
            return int(val)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_expires_on(raw: Optional[str]) -> Optional[datetime]:
        if not raw:
            return None
        text = str(raw).strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(text[:19], fmt).replace(tzinfo=timezone.utc)
                except Exception:
                    continue
        return None

    @staticmethod
    def _get_ignore_domains() -> set:
        raw = os.getenv("NPM_IGNORE_DOMAINS", "")
        if not raw:
            return set()
        return {d.strip().lower() for d in raw.split(",") if d.strip()}

    @classmethod
    def clear_cooldowns(cls):
        cls._failed_cooldown.clear()

    def renew_certificates(
        self,
        threshold_days: Optional[int] = None,
        force: bool = False,
        cert_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        npm_url = os.getenv("NPM_URL", "http://nginx:81").rstrip("/")
        identity = os.getenv("NPM_IDENTITY", "").strip()
        secret = os.getenv("NPM_SECRET", "").strip()

        if not identity or not secret:
            msg = "NPM_IDENTITY/NPM_SECRET not configured"
            self.logger.error(f"[NpmCertRenewer] {msg}")
            return {
                "status": "error",
                "message": msg,
                "checked": 0,
                "eligible": 0,
                "renewed": [],
                "skipped": [],
                "failed": [],
            }

        if threshold_days is None:
            threshold_days = self._get_int_env("NPM_RENEW_THRESHOLD_DAYS", 15)
        cooldown_hours = self._get_int_env("NPM_COOLDOWN_HOURS", 12)
        timeout = self._get_int_env("NPM_REQUEST_TIMEOUT", 120)
        ignore_domains = self._get_ignore_domains()

        self.logger.info(
            f"[NpmCertRenewer] Starting SSL renewal check on {npm_url} "
            f"(threshold_days={threshold_days}, force={force}, cert_id={cert_id})"
        )

        # 1. Authenticate
        try:
            auth_resp = requests.post(
                f"{npm_url}/api/tokens",
                json={"identity": identity, "secret": secret},
                timeout=timeout,
            )
            if auth_resp.status_code < 200 or auth_resp.status_code > 299:
                msg = f"Failed to authenticate on NPM ({npm_url}/api/tokens): HTTP {auth_resp.status_code} - {auth_resp.text}"
                self.logger.error(f"[NpmCertRenewer] {msg}")
                return {
                    "status": "error",
                    "message": msg,
                    "checked": 0,
                    "eligible": 0,
                    "renewed": [],
                    "skipped": [],
                    "failed": [],
                }
            token_data = auth_resp.json()
            jwt_token = token_data.get("token")
            if not jwt_token:
                msg = "NPM /api/tokens response did not contain a valid token"
                self.logger.error(f"[NpmCertRenewer] {msg}")
                return {
                    "status": "error",
                    "message": msg,
                    "checked": 0,
                    "eligible": 0,
                    "renewed": [],
                    "skipped": [],
                    "failed": [],
                }
        except Exception as e:
            msg = f"Exception authenticating on NPM ({npm_url}/api/tokens): {str(e)}"
            self.logger.error(f"[NpmCertRenewer] {msg}")
            return {
                "status": "error",
                "message": msg,
                "checked": 0,
                "eligible": 0,
                "renewed": [],
                "skipped": [],
                "failed": [],
            }

        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
        }

        # 2. List certificates
        try:
            list_resp = requests.get(
                f"{npm_url}/api/nginx/certificates",
                headers=headers,
                timeout=timeout,
            )
            if list_resp.status_code < 200 or list_resp.status_code > 299:
                msg = f"Failed to list NPM certificates: HTTP {list_resp.status_code} - {list_resp.text}"
                self.logger.error(f"[NpmCertRenewer] {msg}")
                return {
                    "status": "error",
                    "message": msg,
                    "checked": 0,
                    "eligible": 0,
                    "renewed": [],
                    "skipped": [],
                    "failed": [],
                }
            certificates = list_resp.json()
            if not isinstance(certificates, list):
                certificates = []
        except Exception as e:
            msg = f"Exception listing NPM certificates: {str(e)}"
            self.logger.error(f"[NpmCertRenewer] {msg}")
            return {
                "status": "error",
                "message": msg,
                "checked": 0,
                "eligible": 0,
                "renewed": [],
                "skipped": [],
                "failed": [],
            }

        now = datetime.now(timezone.utc)
        checked = 0
        eligible = 0
        renewed: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        for cert in certificates:
            cid = cert.get("id")
            if cert_id is not None and cid != cert_id:
                continue

            checked += 1
            provider = str(cert.get("provider", "")).strip().lower()
            domains = cert.get("domain_names") or []
            if not isinstance(domains, list):
                domains = [str(domains)]
            nice_name = str(cert.get("nice_name") or "").strip()
            display_domains = domains if domains else ([nice_name] if nice_name else [])

            if provider != "letsencrypt":
                skipped.append(
                    {
                        "id": cid,
                        "domains": display_domains,
                        "reason": "non_letsencrypt",
                    }
                )
                continue

            # Check ignored domains
            candidate_names = {d.strip().lower() for d in display_domains if d and d.strip()}
            if nice_name:
                candidate_names.add(nice_name.lower())
            if ignore_domains and candidate_names and candidate_names.issubset(ignore_domains):
                self.logger.warning(
                    f"[NpmCertRenewer] Skipping certificate id={cid} ({display_domains}): ignored_domain"
                )
                skipped.append(
                    {
                        "id": cid,
                        "domains": display_domains,
                        "reason": "ignored_domain",
                    }
                )
                continue

            old_expires_raw = cert.get("expires_on")
            expires_dt = self._parse_expires_on(old_expires_raw)
            if expires_dt is None:
                self.logger.warning(
                    f"[NpmCertRenewer] Skipping certificate id={cid} ({display_domains}): invalid_expires_on ({old_expires_raw})"
                )
                skipped.append(
                    {
                        "id": cid,
                        "domains": display_domains,
                        "reason": "invalid_expires_on",
                    }
                )
                continue

            days_remaining = round((expires_dt - now).total_seconds() / 86400.0, 2)

            is_forced_target = force and (cert_id is None or cid == cert_id)
            if days_remaining > threshold_days and not is_forced_target:
                skipped.append(
                    {
                        "id": cid,
                        "domains": display_domains,
                        "days_remaining": days_remaining,
                        "reason": "not_due",
                    }
                )
                continue

            # Check cooldown after previous failure
            last_failed = self._failed_cooldown.get(cid)
            if (
                last_failed is not None
                and not force
                and (now - last_failed).total_seconds() < cooldown_hours * 3600
            ):
                self.logger.warning(
                    f"[NpmCertRenewer] Skipping certificate id={cid} ({display_domains}): in cooldown since {last_failed.isoformat()}"
                )
                skipped.append(
                    {
                        "id": cid,
                        "domains": display_domains,
                        "days_remaining": days_remaining,
                        "reason": "cooldown",
                    }
                )
                continue

            eligible += 1
            self.logger.info(
                f"[NpmCertRenewer] Renewing certificate id={cid} ({display_domains}), "
                f"expires_on={old_expires_raw} (days_remaining={days_remaining})"
            )

            try:
                renew_resp = requests.post(
                    f"{npm_url}/api/nginx/certificates/{cid}/renew",
                    headers=headers,
                    timeout=timeout,
                )
                if 200 <= renew_resp.status_code <= 299:
                    renew_data = renew_resp.json() if renew_resp.text else {}
                    new_expires_on = renew_data.get("expires_on")
                    self._failed_cooldown.pop(cid, None)
                    self.logger.info(
                        f"[NpmCertRenewer] Renewed certificate id={cid} ({display_domains}): "
                        f"old_expires_on={old_expires_raw} -> new_expires_on={new_expires_on}"
                    )
                    renewed.append(
                        {
                            "id": cid,
                            "domains": display_domains,
                            "old_expires_on": old_expires_raw,
                            "new_expires_on": new_expires_on,
                        }
                    )
                else:
                    err_msg = f"HTTP {renew_resp.status_code}: {renew_resp.text}"
                    self._failed_cooldown[cid] = now
                    self.logger.error(
                        f"[NpmCertRenewer] Failed to renew certificate id={cid} ({display_domains}): {err_msg}"
                    )
                    failed.append(
                        {
                            "id": cid,
                            "domains": display_domains,
                            "error": err_msg,
                        }
                    )
            except Exception as e:
                err_msg = str(e)
                self._failed_cooldown[cid] = now
                self.logger.error(
                    f"[NpmCertRenewer] Exception renewing certificate id={cid} ({display_domains}): {err_msg}"
                )
                failed.append(
                    {
                        "id": cid,
                        "domains": display_domains,
                        "error": err_msg,
                    }
                )

        self.logger.info(
            f"[NpmCertRenewer] Finished SSL renewal check: checked={checked}, "
            f"eligible={eligible}, renewed={len(renewed)}, skipped={len(skipped)}, failed={len(failed)}"
        )

        return {
            "status": "ok",
            "checked": checked,
            "eligible": eligible,
            "renewed": renewed,
            "skipped": skipped,
            "failed": failed,
        }
