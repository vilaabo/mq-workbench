#!/usr/bin/env python3
"""E2E-тесты mq-workbench против IBM MQ (Docker, см. README «Тестирование»).

Запуск: сервис на MQWB_URL (дефолт http://localhost:18080), очереди DEV.QUEUE.1..3 пусты.
"""
import json
import os
import threading
import time
import urllib.request
import urllib.error

BASE = os.environ.get("MQWB_URL", "http://localhost:18080")
PASS, FAIL = [], []


def call(method, path, body=None, headers=None, timeout=40):
    req = urllib.request.Request(BASE + path, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def hget(headers, name):
    """Имена HTTP-заголовков регистронезависимы."""
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return None


def by_id(browse_body, msg_id):
    for m in json.loads(browse_body)["messages"]:
        if m["messageId"] == msg_id:
            return m
    return None


def check(name, cond, info=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  [{info}]"))


# --- очистка перед стартом ---
for q in ("DEV.QUEUE.1", "DEV.QUEUE.2", "DEV.QUEUE.3"):
    call("DELETE", f"/queues/{q}/messages")

# 1. PUT простого JSON
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages",
                  json.dumps({"orderId": 42}).encode(), {"Content-Type": "application/json"})
r1 = json.loads(bd)
check("put json 201", st == 201 and len(r1.get("messageId", "")) == 48, f"{st} {bd[:200]}")

# 2. PUT с RFH2-свойствами (X-MQ-Properties сохраняет регистр имён) + XML-телом + CorrelId
corr = "CAFEBABE".ljust(48, "0")
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages",
                  "<req><customer>Иванов</customer></req>".encode(),
                  {"Content-Type": "application/xml",
                   "X-MQ-Properties": '{"operation": "CreateOrder", "version": "2"}',
                   "X-MQ-Correlation-Id": corr,
                   "X-MQ-Message-Type": "REQUEST",
                   "X-MQ-Reply-To-Queue": "DEV.QUEUE.3",
                   "X-MQ-Priority": "5"})
r2 = json.loads(bd)
check("put rfh2 201", st == 201, f"{st} {bd[:200]}")
check("put rfh2 correlId echoed", r2.get("correlationId") == corr, r2.get("correlationId"))

# 3. BROWSE (порядок может быть приоритетным — ищем по messageId)
st, hd, bd = call("GET", "/queues/DEV.QUEUE.1/messages")
br = json.loads(bd)
check("browse 200, depth 2", st == 200 and br["depth"] == 2 and br["returned"] == 2, bd[:300])
mp = by_id(bd, r1["messageId"])   # простое сообщение
mr = by_id(bd, r2["messageId"])   # сообщение с RFH2
check("browse находит оба", mp is not None and mr is not None, bd[:300])
check("browse plain MQSTR без props", mp["format"] == "MQSTR" and mp["properties"] is None,
      json.dumps(mp)[:200])
check("browse plain preview json", mp["bodyPreview"] == '{"orderId": 42}', mp.get("bodyPreview"))
check("browse rfh2 MQHRF2 -> MQSTR", mr["format"] == "MQHRF2" and mr["bodyFormat"] == "MQSTR",
      f'{mr.get("format")}/{mr.get("bodyFormat")}')
check("browse rfh2 usr props (точный регистр)", mr["properties"] == {"operation": "CreateOrder", "version": "2"},
      json.dumps(mr.get("properties")))
check("browse rfh2 body clean xml", mr["bodyPreview"] == "<req><customer>Иванов</customer></req>",
      mr.get("bodyPreview"))
check("browse rfh2 type REQUEST prio 5", mr["messageType"] == "REQUEST" and mr["priority"] == 5,
      f'{mr.get("messageType")}/{mr.get("priority")}')

# 4. GET одного сообщения по msgId
st, hd, bd = call("GET", f"/queues/DEV.QUEUE.1/messages/{r2['messageId']}")
check("get one 200 xml", st == 200 and bd.decode() == "<req><customer>Иванов</customer></req>", f"{st} {bd[:200]}")
check("get one content-type xml", (hget(hd, "Content-Type") or "").startswith("application/xml"),
      hget(hd, "Content-Type"))
check("get one X-MQ-Usr header", hget(hd, "X-MQ-Usr-operation") == "CreateOrder",
      hget(hd, "X-MQ-Usr-operation"))
props_hdr = hget(hd, "X-MQ-Properties")
check("get one X-MQ-Properties header", props_hdr is not None
      and json.loads(props_hdr) == {"operation": "CreateOrder", "version": "2"}, props_hdr)
check("get one X-MQ-Correlation-Id", hget(hd, "X-MQ-Correlation-Id") == corr, hget(hd, "X-MQ-Correlation-Id"))
check("get one X-MQ-Format = формат тела", hget(hd, "X-MQ-Format") == "MQSTR", hget(hd, "X-MQ-Format"))
check("get one X-MQ-Mqmd-Format = MQHRF2", (hget(hd, "X-MQ-Mqmd-Format") or "").strip() == "MQHRF2",
      hget(hd, "X-MQ-Mqmd-Format"))

# 4b. ?raw=true отдаёт тело без перекодировки (заголовки уже сняты, свойства — в X-MQ-*)
st, hd, bd = call("GET", f"/queues/DEV.QUEUE.1/messages/{r2['messageId']}?raw=true")
check("raw=true отдаёт чистое тело", st == 200 and bd.decode() == "<req><customer>Иванов</customer></req>"
      and (hget(hd, "Content-Type") or "").startswith("application/octet-stream"),
      f"{st} {bd[:60]} ct={hget(hd, 'Content-Type')}")

# 4c. Реплей через properties: dotted-ключи возвращаются в родные папки, не в usr
st, hd, bd = call("POST", "/queues/DEV.QUEUE.3/messages", b"<replay/>",
                  {"Content-Type": "application/xml",
                   "X-MQ-Properties": '{"operation": "CreateOrder", "mcd.Msd": "jms_text"}'})
check("replay c mcd.Msd 201", st == 201, f"{st} {bd[:200]}")
st, hd, bd = call("GET", "/queues/DEV.QUEUE.3/messages")
msgs3 = json.loads(bd)["messages"]
rm = msgs3[0] if msgs3 else {}
check("replay: usr и mcd в своих папках", (rm.get("properties") or {}).get("operation") == "CreateOrder"
      and (rm.get("properties") or {}).get("mcd.Msd") == "jms_text", json.dumps(rm.get("properties")))
call("DELETE", "/queues/DEV.QUEUE.3/messages")

# 4d. Гард: MQHRF2 в формате вместе со свойствами -> 400
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", b"x",
                  {"X-MQ-Format": "MQHRF2", "X-MQ-Properties": '{"a":"b"}'})
check("MQHRF2+props -> 400", st == 400, f"{st} {bd[:200]}")

# 4e. MQHRF2 в формате с телом без валидного RFH2 -> 400 MQRC_HEADER_ERROR (валидирует сам MQ)
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", b"<not-rfh2/>",
                  {"Content-Type": "application/octet-stream", "X-MQ-Format": "MQHRF2"})
check("MQHRF2+битое тело -> 400 HEADER_ERROR", st == 400
      and json.loads(bd).get("details") == "MQRC_HEADER_ERROR", f"{st} {bd[:200]}")

# 5. GET несуществующего msgId -> 404
st, hd, bd = call("GET", "/queues/DEV.QUEUE.1/messages/" + "AB" * 24)
er = json.loads(bd)
check("get missing 404 + reason", st == 404 and er["details"] == "MQRC_NO_MSG_AVAILABLE", f"{st} {bd[:200]}")

# 6. Опечатка в имени очереди -> 404 MQRC_UNKNOWN_OBJECT_NAME
st, hd, bd = call("GET", "/queues/NO.SUCH.QUEUE/messages")
er = json.loads(bd)
check("bad queue 404", st == 404 and er["details"] == "MQRC_UNKNOWN_OBJECT_NAME", f"{st} {bd[:200]}")

# 7. DELETE одного по msgId (destructive) — возвращает тело, затем его больше нет
st, hd, bd = call("DELETE", f"/queues/DEV.QUEUE.1/messages/{r1['messageId']}")
check("delete one returns body", st == 200 and bd.decode() == '{"orderId": 42}', f"{st} {bd[:200]}")
st, hd, bd = call("GET", "/queues/DEV.QUEUE.1/messages")
check("after delete depth 1", json.loads(bd)["depth"] == 1, bd[:200])

# 8. PURGE
st, hd, bd = call("DELETE", "/queues/DEV.QUEUE.1/messages")
pr = json.loads(bd)
check("purge 1 removed, 0 left", st == 200 and pr["purged"] == 1 and pr["remaining"] == 0, bd[:200])

# 9. RPC: имитируем интеграцию — фоновый поток отвечает на запрос
def responder():
    for _ in range(100):
        s, h, b = call("GET", "/queues/DEV.QUEUE.2/messages")
        msgs = json.loads(b)["messages"]
        if msgs:
            req = msgs[0]
            op = (req.get("properties") or {}).get("operation", "?")
            reply = json.dumps({"answer": "ok", "operation": op}).encode()
            call("POST", "/queues/DEV.QUEUE.3/messages", reply,
                 {"Content-Type": "application/json",
                  "X-MQ-Correlation-Id": req["messageId"],
                  "X-MQ-Message-Type": "REPLY"})
            call("DELETE", f"/queues/DEV.QUEUE.2/messages/{req['messageId']}")
            return
        time.sleep(0.3)

t = threading.Thread(target=responder, daemon=True)
t.start()
st, hd, bd = call("POST", "/rpc?requestQueue=DEV.QUEUE.2&replyQueue=DEV.QUEUE.3&timeoutSeconds=30",
                  json.dumps({"ask": "who"}).encode(),
                  {"Content-Type": "application/json",
                   "X-MQ-Properties": '{"operation": "CreateOrder"}'})
t.join(timeout=40)
ok = False
if st == 200:
    rep = json.loads(bd)
    ok = rep.get("answer") == "ok" and rep.get("operation") == "CreateOrder"
check("rpc round-trip c usr.operation", ok, f"{st} {bd[:300]}")
check("rpc request msgid header", len(hget(hd, "X-MQ-Request-Message-Id") or "") == 48,
      hget(hd, "X-MQ-Request-Message-Id"))
check("rpc reply type REPLY", hget(hd, "X-MQ-Message-Type") == "REPLY", hget(hd, "X-MQ-Message-Type"))

# 10. RPC-таймаут -> 504
st, hd, bd = call("POST", "/rpc?requestQueue=DEV.QUEUE.2&replyQueue=DEV.QUEUE.3&timeoutSeconds=2",
                  b"{}", {"Content-Type": "application/json"})
check("rpc timeout 504", st == 504 and "timeout" in json.loads(bd)["error"].lower(), f"{st} {bd[:200]}")
call("DELETE", "/queues/DEV.QUEUE.2/messages")

# 11. Бинарный round-trip
blob = bytes(range(256)) * 4
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", blob,
                  {"Content-Type": "application/octet-stream", "X-MQ-Format": "NONE"})
mid = json.loads(bd)["messageId"]
st, hd, bd = call("GET", f"/queues/DEV.QUEUE.1/messages/{mid}")
check("binary round-trip", st == 200 and bd == blob
      and (hget(hd, "Content-Type") or "").startswith("application/octet-stream"),
      f"{st} len={len(bd)} ct={hget(hd, 'Content-Type')}")
st, hd, bd = call("GET", "/queues/DEV.QUEUE.1/messages")
check("binary browse: no preview", json.loads(bd)["messages"][0]["bodyPreview"] is None, bd[:300])
call("DELETE", "/queues/DEV.QUEUE.1/messages")

# 12. Кириллица UTF-8 round-trip
text = "Привет! ёЁъ".encode()
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", text, {"Content-Type": "text/plain; charset=utf-8"})
mid = json.loads(bd)["messageId"]
st, hd, bd = call("GET", f"/queues/DEV.QUEUE.1/messages/{mid}")
check("cyrillic round-trip", bd.decode() == "Привет! ёЁъ", bd[:100])
call("DELETE", "/queues/DEV.QUEUE.1/messages")

# 13. UTF-16 (CCSID 1200): кладём и читаем обратно
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", "Тест UTF-16".encode(),
                  {"Content-Type": "text/plain; charset=utf-8", "X-MQ-Character-Set": "1200"})
mid = json.loads(bd)["messageId"]
st, hd, bd = call("GET", f"/queues/DEV.QUEUE.1/messages/{mid}")
check("utf-16 put/get", bd.decode() == "Тест UTF-16", f"{st} {bd[:100]}")
check("utf-16 ccsid header", hget(hd, "X-MQ-Character-Set") == "1200", hget(hd, "X-MQ-Character-Set"))
call("DELETE", "/queues/DEV.QUEUE.1/messages")

# 14. Тело больше 1 МБ (старый Javalin-лимит) проходит
big = (b'{"pad":"' + b"x" * (2 * 1024 * 1024) + b'"}')
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", big, {"Content-Type": "application/json"})
check("2MB body accepted", st == 201, f"{st} {bd[:200]}")
call("DELETE", "/queues/DEV.QUEUE.1/messages")

# 15. Невалидный заголовок -> 400 в JSON-формате
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", b"{}",
                  {"Content-Type": "application/json", "X-MQ-Priority": "abc"})
check("bad header 400 json", st == 400 and "X-MQ-Priority" in json.loads(bd)["details"], f"{st} {bd[:200]}")

# 16. Невалидный X-MQ-Properties -> 400
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", b"{}",
                  {"Content-Type": "application/json", "X-MQ-Properties": "not json"})
check("bad properties 400", st == 400 and "X-MQ-Properties" in json.loads(bd)["details"], f"{st} {bd[:200]}")

# 17. Неподдерживаемый CCSID -> 400/201, но не пустой 500
st, hd, bd = call("POST", "/queues/DEV.QUEUE.1/messages", b"hello",
                  {"Content-Type": "text/plain", "X-MQ-Character-Set": "999999"})
check("weird ccsid no bare 500", st in (201, 400), f"{st} {bd[:200]}")
call("DELETE", "/queues/DEV.QUEUE.1/messages")

# 18. limit в browse
for i in range(5):
    call("POST", "/queues/DEV.QUEUE.1/messages", json.dumps({"n": i}).encode(),
         {"Content-Type": "application/json"})
st, hd, bd = call("GET", "/queues/DEV.QUEUE.1/messages?limit=2")
br = json.loads(bd)
check("browse limit=2 moreAvailable", br["returned"] == 2 and br["depth"] == 5 and br["moreAvailable"] is True,
      bd[:200])
st, hd, bd = call("DELETE", "/queues/DEV.QUEUE.1/messages")
check("purge 5", json.loads(bd)["purged"] == 5, bd[:200])

print(f"\n=== {len(PASS)} passed, {len(FAIL)} failed ===")
if FAIL:
    print("FAILED:", *FAIL, sep="\n  ")
    raise SystemExit(1)
