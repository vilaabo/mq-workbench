#!/usr/bin/env python3
"""E2E-тесты WireMock-респондера: mq-workbench :18080, WireMock :8888, IBM MQ в Docker."""
import json
import time
import urllib.request
import urllib.error

MQWB = "http://localhost:18080"
WM = "http://localhost:8888"
PASS, FAIL = [], []


def call(base, method, path, body=None, headers=None, timeout=60):
    req = urllib.request.Request(base + path, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def hget(headers, name):
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return None


def check(name, cond, info=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  [{info}]"))


# --- подготовка: чистые очереди, чистый WireMock, стабы ---
for q in ("DEV.QUEUE.1", "DEV.QUEUE.2", "DEV.QUEUE.3"):
    call(MQWB, "DELETE", f"/queues/{q}/messages")
call(WM, "DELETE", "/__admin/mappings")

stub_ok = {
    "request": {"urlPath": "/mq/CreateOrder", "method": "POST"},
    "response": {
        "status": 200,
        "jsonBody": {"result": "approved", "orderId": 42},
        "headers": {
            "Content-Type": "application/json",
            "X-MQ-Properties": '{"operation": "CreateOrderResponse"}'
        },
    },
}
stub_silent = {
    "request": {"urlPath": "/mq/Silence", "method": "POST"},
    "response": {"status": 204},
}
for stub in (stub_ok, stub_silent):
    st, _, bd = call(WM, "POST", "/__admin/mappings", json.dumps(stub).encode(),
                     {"Content-Type": "application/json"})
    assert st == 201, f"стаб не создался: {st} {bd[:200]}"

# 1. Создание респондера
cfg = {"name": "demo", "requestQueue": "DEV.QUEUE.2", "replyQueue": "DEV.QUEUE.3",
       "wiremockUrl": WM}
st, hd, bd = call(MQWB, "POST", "/responders", json.dumps(cfg).encode(),
                  {"Content-Type": "application/json"})
r = json.loads(bd)
check("создание респондера 201", st == 201 and r["name"] == "demo", f"{st} {bd[:300]}")
check("дефолты подставлены", r["pathTemplate"] == "/mq/{operation}"
      and r["operationProperty"] == "operation" and r["correlation"] == "auto", json.dumps(r)[:300])

time.sleep(1.5)
st, hd, bd = call(MQWB, "GET", "/responders/demo")
check("state RUNNING", json.loads(bd)["state"] == "RUNNING", bd[:200])

# 2. Дубликат имени -> 409, кривой конфиг -> 400
st, _, bd = call(MQWB, "POST", "/responders", json.dumps(cfg).encode(),
                 {"Content-Type": "application/json"})
check("дубликат -> 409", st == 409, f"{st} {bd[:200]}")
st, _, bd = call(MQWB, "POST", "/responders", json.dumps({"name": "bad"}).encode(),
                 {"Content-Type": "application/json"})
check("неполный конфиг -> 400", st == 400, f"{st} {bd[:200]}")

# 3. Полный round-trip через наш же /rpc (он играет роль конвейера)
st, hd, bd = call(MQWB, "POST", "/rpc?requestQueue=DEV.QUEUE.2&replyQueue=DEV.QUEUE.3&timeoutSeconds=30",
                  json.dumps({"loanId": 777}).encode(),
                  {"Content-Type": "application/json",
                   "X-MQ-Properties": '{"operation": "CreateOrder"}'})
ok = st == 200 and json.loads(bd) == {"result": "approved", "orderId": 42}
check("rpc через респондер: тело стаба", ok, f"{st} {bd[:300]}")
check("rpc через респондер: MsgType REPLY", hget(hd, "X-MQ-Message-Type") == "REPLY",
      hget(hd, "X-MQ-Message-Type"))
props = hget(hd, "X-MQ-Properties")
check("rpc через респондер: props из заголовка стаба",
      props is not None and json.loads(props).get("operation") == "CreateOrderResponse", props)

# 4. corrId passthrough: rpc ставит MQRO_PASS_CORREL_ID, auto-режим респондера должен его уважить
corr = "BEEF".ljust(48, "0")
st, hd, bd = call(MQWB, "POST",
                  "/rpc?requestQueue=DEV.QUEUE.2&replyQueue=DEV.QUEUE.3&timeoutSeconds=30&correlation=corrId",
                  json.dumps({"loanId": 778}).encode(),
                  {"Content-Type": "application/json",
                   "X-MQ-Correlation-Id": corr,
                   "X-MQ-Properties": '{"operation": "CreateOrder"}'})
check("corrId passthrough (Report-флаги)", st == 200 and hget(hd, "X-MQ-Correlation-Id") == corr,
      f"{st} corr={hget(hd, 'X-MQ-Correlation-Id')}")

# 5. Стаб молчит (204) -> rpc таймаутится, у респондера silenced+1
st, hd, bd = call(MQWB, "POST", "/rpc?requestQueue=DEV.QUEUE.2&replyQueue=DEV.QUEUE.3&timeoutSeconds=3",
                  b"{}", {"Content-Type": "application/json",
                          "X-MQ-Properties": '{"operation": "Silence"}'})
check("204 от стаба -> rpc 504", st == 504, f"{st} {bd[:200]}")

# 6. Нет стаба под операцию -> WireMock 404 -> errors+1
call(MQWB, "POST", "/queues/DEV.QUEUE.2/messages", b'{"x":1}',
     {"Content-Type": "application/json", "X-MQ-Properties": '{"operation": "Nope"}'})
time.sleep(2)
st, hd, bd = call(MQWB, "GET", "/responders/demo")
stat = json.loads(bd)
check("статистика: received=4", stat["received"] == 4, json.dumps(stat)[:300])
check("статистика: replied=2", stat["replied"] == 2, json.dumps(stat)[:300])
check("статистика: silenced=1", stat["silenced"] == 1, json.dumps(stat)[:300])
check("статистика: errors=1 c 404", stat["errors"] == 1 and "404" in (stat["lastError"] or ""),
      json.dumps(stat)[:300])

# 7. DELETE: респондер останавливается и перестаёт читать очередь
st, hd, bd = call(MQWB, "DELETE", "/responders/demo")
check("удаление 200", st == 200, f"{st} {bd[:200]}")
time.sleep(6)  # ждём выхода из MQGET wait (интервал 5 с)
call(MQWB, "POST", "/queues/DEV.QUEUE.2/messages", b'{"after":"delete"}',
     {"Content-Type": "application/json", "X-MQ-Properties": '{"operation": "CreateOrder"}'})
time.sleep(2)
st, hd, bd = call(MQWB, "GET", "/queues/DEV.QUEUE.2/messages")
check("после удаления очередь не читается", json.loads(bd)["depth"] == 1, bd[:200])
st, hd, bd = call(MQWB, "GET", "/responders/demo")
check("удалённый респондер -> 404", st == 404, f"{st}")
st, hd, bd = call(MQWB, "GET", "/responders")
check("список пуст", json.loads(bd) == [], bd[:100])

# --- уборка ---
for q in ("DEV.QUEUE.1", "DEV.QUEUE.2", "DEV.QUEUE.3"):
    call(MQWB, "DELETE", f"/queues/{q}/messages")

print(f"\n=== {len(PASS)} passed, {len(FAIL)} failed ===")
if FAIL:
    print("FAILED:", *FAIL, sep="\n  ")
    raise SystemExit(1)
