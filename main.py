from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Optional
from datetime import datetime, timezone
import asyncio
import httpx
import uuid
import time

# ------------------------------------------------------------------
# FastAPI app
# ------------------------------------------------------------------
app = FastAPI(title="ProxyMaze26")

# ------------------------------------------------------------------
# In-memory storage (evaluation සඳහා ප්‍රමාණවත්, production නම් database යොදන්න)
# ------------------------------------------------------------------
config_data = {
    "check_interval_seconds": 15,
    "request_timeout_ms": 3000
}

proxies_db = {}          # { proxy_id: { "id": str, "url": str, "status": "pending"/"up"/"down", "history": [] } }
alerts_db = []           # list of alert dicts
active_alert_id = None   # currently active alert's id
webhooks_db = []         # list of {"id": str, "url": str}
slack_integrations = []  # list of {"webhook_url": str, "username": str, "events": list}
discord_integrations = []

monitor_task = None

# ------------------------------------------------------------------
# Helper: Extract proxy ID from URL (last segment)
# ------------------------------------------------------------------
def extract_proxy_id(url: str) -> str:
    return url.rstrip('/').split('/')[-1]

# ------------------------------------------------------------------
# 1. Health
# ------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}

# ------------------------------------------------------------------
# 2. Config
# ------------------------------------------------------------------
class ConfigUpdate(BaseModel):
    check_interval_seconds: int
    request_timeout_ms: int

@app.post("/config")
async def update_config(cfg: ConfigUpdate):
    global config_data
    config_data = cfg.dict()
    return {"status": "ok"}

@app.get("/config")
async def get_config():
    return config_data

# ------------------------------------------------------------------
# 3. Proxies management
# ------------------------------------------------------------------
class ProxyLoadRequest(BaseModel):
    proxies: List[str]
    replace: bool = False

@app.post("/proxies", status_code=201)
async def add_proxies(req: ProxyLoadRequest):
    global proxies_db
    if req.replace:
        proxies_db.clear()
    accepted = 0
    for url in req.proxies:
        pid = extract_proxy_id(url)
        if pid not in proxies_db:
            proxies_db[pid] = {
                "id": pid,
                "url": url,
                "status": "pending",
                "history": []   # store status changes
            }
            accepted += 1
    # Return list of all proxies (pending status initially)
    proxy_list = [{"id": pid, "url": p["url"], "status": p["status"]} for pid, p in proxies_db.items()]
    return {"accepted": accepted, "proxies": proxy_list}

@app.get("/proxies")
async def list_proxies():
    total = len(proxies_db)
    up = sum(1 for p in proxies_db.values() if p["status"] == "up")
    down = sum(1 for p in proxies_db.values() if p["status"] == "down")
    failure_rate = down / total if total > 0 else 0.0
    proxies_out = [{"id": pid, "url": p["url"], "status": p["status"]} for pid, p in proxies_db.items()]
    return {
        "total": total,
        "up": up,
        "down": down,
        "failure_rate": round(failure_rate, 2),
        "proxies": proxies_out
    }

@app.get("/proxies/{proxy_id}")
async def get_proxy(proxy_id: str):
    if proxy_id not in proxies_db:
        raise HTTPException(404, "Proxy not found")
    p = proxies_db[proxy_id]
    return {"id": proxy_id, "url": p["url"], "status": p["status"]}

@app.get("/proxies/{proxy_id}/history")
async def proxy_history(proxy_id: str):
    if proxy_id not in proxies_db:
        raise HTTPException(404, "Proxy not found")
    return {"proxy_id": proxy_id, "history": proxies_db[proxy_id].get("history", [])}

@app.delete("/proxies", status_code=204)
async def delete_all_proxies():
    global proxies_db
    proxies_db.clear()
    # Do NOT delete alerts
    return

# ------------------------------------------------------------------
# 4. Alerts
# ------------------------------------------------------------------
@app.get("/alerts")
async def get_alerts():
    # return all alerts (active and resolved)
    return alerts_db

# ------------------------------------------------------------------
# 5. Webhooks registration & delivery
# ------------------------------------------------------------------
class WebhookReg(BaseModel):
    url: str

@app.post("/webhooks", status_code=201)
async def register_webhook(wh: WebhookReg):
    wh_id = "wh-" + str(uuid.uuid4())[:8]
    webhooks_db.append({"id": wh_id, "url": wh.url})
    return {"webhook_id": wh_id, "url": wh.url}

async def send_webhook_with_retry(url: str, payload: dict):
    """Send webhook with retry on 5xx errors (max 5 retries, exponential backoff)"""
    async with httpx.AsyncClient() as client:
        for attempt in range(1, 6):
            try:
                resp = await client.post(url, json=payload, timeout=5)
                if resp.status_code < 500:   # 2xx, 4xx is considered success (no retry)
                    return
                # 5xx -> retry
            except (httpx.TimeoutException, httpx.ConnectError):
                pass   # network error -> retry
            await asyncio.sleep(2 ** attempt)  # 2,4,8,16,32 sec
    print(f"Failed to deliver webhook to {url} after 5 attempts")

# ------------------------------------------------------------------
# 6. Integrations (Slack & Discord) – Bonus
# ------------------------------------------------------------------
class IntegrationRequest(BaseModel):
    type: str   # "slack" or "discord"
    webhook_url: str
    username: str
    events: List[str]   # e.g. ["alert.fired", "alert.resolved"]

@app.post("/integrations", status_code=201)
async def add_integration(req: IntegrationRequest):
    if req.type == "slack":
        slack_integrations.append(req.dict())
    elif req.type == "discord":
        discord_integrations.append(req.dict())
    else:
        raise HTTPException(400, "Invalid integration type")
    return {"status": "registered"}

async def send_slack_message(webhook_url: str, username: str, event_type: str, alert_data: dict, resolved_at: str = None):
    """Send formatted Slack alert (required fields)"""
    if event_type == "alert.fired":
        color = "#FF0000"
        text = f"🚨 Proxy pool failure rate exceeded threshold!"
        fields = [
            {"title": "Alert ID", "value": alert_data["alert_id"], "short": False},
            {"title": "Failure Rate", "value": str(alert_data["failure_rate"]), "short": True},
            {"title": "Failed Proxies", "value": str(alert_data["failed_proxies"]), "short": True},
            {"title": "Threshold", "value": "0.20", "short": True},
            {"title": "Failed IDs", "value": ", ".join(alert_data["failed_proxy_ids"]), "short": False},
            {"title": "Fired At", "value": alert_data["fired_at"], "short": True}
        ]
        ts = int(time.time())
        payload = {
            "username": username,
            "text": text,
            "attachments": [{
                "color": color,
                "fields": fields,
                "footer": "ProxyMaze",
                "ts": ts
            }]
        }
    else:  # resolved
        color = "#00FF00"
        text = f"✅ Alert resolved: {alert_data['alert_id']}"
        fields = [
            {"title": "Alert ID", "value": alert_data["alert_id"], "short": False},
            {"title": "Resolved At", "value": resolved_at, "short": True}
        ]
        ts = int(time.time())
        payload = {
            "username": username,
            "text": text,
            "attachments": [{
                "color": color,
                "fields": fields,
                "footer": "ProxyMaze",
                "ts": ts
            }]
        }
    async with httpx.AsyncClient() as client:
        await client.post(webhook_url, json=payload, timeout=5)

async def send_discord_message(webhook_url: str, username: str, event_type: str, alert_data: dict, resolved_at: str = None):
    """Send formatted Discord alert (required fields)"""
    if event_type == "alert.fired":
        color_int = 0xFF0000
        description = f"Failure rate {alert_data['failure_rate']*100:.0f}% exceeded threshold 20%"
        fields = [
            {"name": "Alert ID", "value": alert_data["alert_id"], "inline": False},
            {"name": "Failure Rate", "value": str(alert_data["failure_rate"]), "inline": True},
            {"name": "Failed Proxies", "value": str(alert_data["failed_proxies"]), "inline": True},
            {"name": "Threshold", "value": "0.20", "inline": True},
            {"name": "Failed IDs", "value": ", ".join(alert_data["failed_proxy_ids"]), "inline": False}
        ]
        title = "🔥 Alert Fired"
    else:
        color_int = 0x00FF00
        description = f"Alert {alert_data['alert_id']} resolved at {resolved_at}"
        fields = [{"name": "Resolved At", "value": resolved_at, "inline": False}]
        title = "✅ Alert Resolved"
    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color_int,
            "fields": fields,
            "footer": {"text": "ProxyMaze Monitor"}
        }]
    }
    async with httpx.AsyncClient() as client:
        await client.post(webhook_url, json=payload, timeout=5)

# ------------------------------------------------------------------
# 7. Background monitor loop (core logic)
# ------------------------------------------------------------------
async def check_single_proxy(client: httpx.AsyncClient, url: str, timeout_sec: float):
    try:
        resp = await client.get(url, timeout=timeout_sec)
        return "up" if 200 <= resp.status_code < 300 else "down"
    except:
        return "down"

async def monitor_loop():
    global proxies_db, active_alert_id, alerts_db
    while True:
        if proxies_db:
            timeout_sec = config_data["request_timeout_ms"] / 1000.0
            async with httpx.AsyncClient() as client:
                tasks = [check_single_proxy(client, pdata["url"], timeout_sec) for pdata in proxies_db.values()]
                results = await asyncio.gather(*tasks)
            
            # Update statuses and store history
            down_count = 0
            new_down_ids = []
            for (pid, pdata), new_status in zip(proxies_db.items(), results):
                old_status = pdata["status"]
                if new_status != old_status:
                    pdata["history"].append({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "from": old_status,
                        "to": new_status
                    })
                pdata["status"] = new_status
                if new_status == "down":
                    down_count += 1
                    new_down_ids.append(pid)
            
            total = len(proxies_db)
            failure_rate = down_count / total if total > 0 else 0.0
            
            # Alert Lifecycle
            if failure_rate >= 0.20:
                if active_alert_id is None:
                    alert_id = str(uuid.uuid4())
                    active_alert_id = alert_id
                    alert_obj = {
                        "alert_id": alert_id,
                        "status": "active",
                        "failure_rate": round(failure_rate, 2),
                        "total_proxies": total,
                        "failed_proxies": down_count,
                        "failed_proxy_ids": new_down_ids.copy(),
                        "threshold": 0.20,
                        "fired_at": datetime.now(timezone.utc).isoformat(),
                        "resolved_at": None,
                        "message": "Proxy pool failure rate exceeded threshold"
                    }
                    alerts_db.append(alert_obj)
                    print(f"🔥 ALERT FIRED: {alert_id}")
                    
                    # Send to registered webhooks
                    payload = {
                        "event": "alert.fired",
                        "alert_id": alert_id,
                        "fired_at": alert_obj["fired_at"],
                        "failure_rate": alert_obj["failure_rate"],
                        "total_proxies": total,
                        "failed_proxies": down_count,
                        "failed_proxy_ids": new_down_ids,
                        "threshold": 0.20,
                        "message": alert_obj["message"]
                    }
                    for wh in webhooks_db:
                        asyncio.create_task(send_webhook_with_retry(wh["url"], payload))
                    
                    # Slack & Discord integrations
                    for sl in slack_integrations:
                        if "alert.fired" in sl["events"]:
                            asyncio.create_task(send_slack_message(sl["webhook_url"], sl["username"], "alert.fired", alert_obj))
                    for dc in discord_integrations:
                        if "alert.fired" in dc["events"]:
                            asyncio.create_task(send_discord_message(dc["webhook_url"], dc["username"], "alert.fired", alert_obj))
            else:
                if active_alert_id is not None:
                    # Resolve the active alert
                    for alert in alerts_db:
                        if alert["alert_id"] == active_alert_id:
                            alert["status"] = "resolved"
                            resolved_at = datetime.now(timezone.utc).isoformat()
                            alert["resolved_at"] = resolved_at
                            break
                    print(f"✅ ALERT RESOLVED: {active_alert_id}")
                    # Send resolved webhook
                    payload_resolved = {
                        "event": "alert.resolved",
                        "alert_id": active_alert_id,
                        "resolved_at": resolved_at
                    }
                    for wh in webhooks_db:
                        asyncio.create_task(send_webhook_with_retry(wh["url"], payload_resolved))
                    # Integrations for resolved
                    for sl in slack_integrations:
                        if "alert.resolved" in sl["events"]:
                            asyncio.create_task(send_slack_message(sl["webhook_url"], sl["username"], "alert.resolved", {"alert_id": active_alert_id}, resolved_at))
                    for dc in discord_integrations:
                        if "alert.resolved" in dc["events"]:
                            asyncio.create_task(send_discord_message(dc["webhook_url"], dc["username"], "alert.resolved", {"alert_id": active_alert_id}, resolved_at))
                    active_alert_id = None
        
        await asyncio.sleep(config_data["check_interval_seconds"])

# ------------------------------------------------------------------
# 8. Metrics endpoint
# ------------------------------------------------------------------
@app.get("/metrics")
async def get_metrics():
    total_checks = sum(len(p.get("history", [])) for p in proxies_db.values())
    return {
        "total_checks": total_checks,
        "current_pool_size": len(proxies_db),
        "active_alerts": 1 if active_alert_id else 0,
        "total_alerts": len(alerts_db),
        "webhook_deliveries": len(webhooks_db) + len(slack_integrations) + len(discord_integrations)
    }

# ------------------------------------------------------------------
# 9. Startup & Shutdown events
# ------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    global monitor_task
    monitor_task = asyncio.create_task(monitor_loop())

@app.on_event("shutdown")
async def shutdown():
    if monitor_task:
        monitor_task.cancel()

# ------------------------------------------------------------------
# 10. Run (if executed directly)
# ------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
