"""无第三方依赖的需求响应协同 HTTP JSON 接口。"""

from __future__ import annotations

import argparse
import json
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from .errors import GridError, ValidationFailed
from .service import GridResponseService
from .storage import connect


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: Mapping[str, Any]


class JsonApplication:
    def __init__(self, service: GridResponseService) -> None:
        self.service = service
        self._lock = threading.Lock()

    def handle(self, method: str, target: str, headers: Mapping[str, str] | None = None, body: bytes = b"") -> Response:
        # 单个 SQLite 连接不能并发使用，按请求串行化。
        with self._lock:
            return self._handle(method, target, headers, body)

    @staticmethod
    def _actor(headers: Mapping[str, str]) -> str:
        actor = headers.get("x-actor-id", "").strip()
        if not actor:
            raise ValidationFailed("缺少 X-Actor-Id")
        return actor

    @staticmethod
    def _json(body: bytes) -> dict[str, Any]:
        if not body:
            return {}
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationFailed("请求体必须是 UTF-8 JSON 对象") from exc
        if not isinstance(value, dict):
            raise ValidationFailed("请求体必须是 JSON 对象")
        return value

    def _handle(self, method: str, target: str, headers: Mapping[str, str] | None = None, body: bytes = b"") -> Response:
        normalized = {key.lower(): value for key, value in (headers or {}).items()}
        parsed = urlparse(target)
        path = parsed.path.rstrip("/") or "/"
        parts = [part for part in path.split("/") if part]
        try:
            if method == "GET" and path == "/health":
                return Response(200, {"status": "ok"})
            payload = self._json(body) if method in {"POST", "PUT", "PATCH"} else {}
            actor = self._actor(normalized)
            if method == "POST" and path == "/users":
                return Response(201, self.service.create_user(payload["user_id"], payload["display_name"], payload["role"]))
            if method == "POST" and path == "/sites":
                return Response(201, self.service.register_site(actor, payload))
            if method == "GET" and len(parts) == 2 and parts[0] == "sites":
                return Response(200, self.service.site_detail(parts[1]))
            if method == "PUT" and len(parts) == 3 and parts[0] == "sites" and parts[2] == "baseline":
                return Response(200, self.service.update_baseline(actor, parts[1], payload))
            if method == "POST" and len(parts) == 3 and parts[0] == "sites" and parts[2] == "tenant-floors":
                return Response(201, self.service.register_tenant_floor(actor, parts[1], payload))
            if method == "POST" and len(parts) == 3 and parts[0] == "sites" and parts[2] == "resources":
                return Response(201, self.service.register_resource(actor, parts[1], payload))
            if method == "GET" and len(parts) == 2 and parts[0] == "resources":
                return Response(200, self.service.resource_detail(parts[1]))
            if method == "POST" and path == "/events":
                return Response(201, self.service.register_event(actor, payload))
            if method == "GET" and len(parts) == 2 and parts[0] == "events":
                return Response(200, self.service.event_detail(parts[1]))
            if method == "POST" and len(parts) == 3 and parts[0] == "events" and parts[2] == "revise":
                return Response(200, self.service.revise_event(actor, parts[1], payload))
            if method == "POST" and len(parts) == 3 and parts[0] == "events" and parts[2] == "candidates":
                return Response(200, self.service.generate_candidates(actor, parts[1]))
            if method == "POST" and len(parts) == 3 and parts[0] == "events" and parts[2] == "confirm":
                return Response(200, self.service.confirm_candidate(actor, parts[1], payload["candidate_id"], int(payload["expected_version"])))
            if method == "POST" and len(parts) == 3 and parts[0] == "events" and parts[2] == "receipts":
                return Response(201, self.service.record_receipt(actor, parts[1], payload))
            if method == "POST" and len(parts) == 3 and parts[0] == "events" and parts[2] == "close":
                return Response(200, self.service.close_event(actor, parts[1]))
            if method == "GET" and len(parts) == 3 and parts[0] == "events" and parts[2] == "report":
                return Response(200, self.service.event_report(actor, parts[1]))
            if method == "GET" and path == "/audit/chain":
                return Response(200, self.service.audit_chain(actor))
            return Response(404, {"error": {"code": "route_not_found", "message": "接口不存在"}})
        except GridError as exc:
            return Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except (KeyError, TypeError, ValueError) as exc:
            return Response(422, {"error": {"code": "invalid_request", "message": str(exc)}})


def make_handler(application: JsonApplication):
    class Handler(BaseHTTPRequestHandler):
        server_version = "GridResponse/1"

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def do_PUT(self) -> None:  # noqa: N802
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            encoded = json.dumps(response.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动电网需求响应协同服务")
    parser.add_argument("--database", type=Path, default=Path("grid_response.sqlite3"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8083)
    args = parser.parse_args(argv)
    connection = connect(args.database)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(JsonApplication(GridResponseService(connection))))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
