import json
import mimetypes
import os
import re
import sys
from cgi import FieldStorage
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse
from uuid import uuid4

from crm_service import CRMService, NotFoundError, ValidationError


ROOT = Path(__file__).resolve().parent.parent


def first_existing_path(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


FRONTEND_DIR = first_existing_path(
    ROOT / "Frontend",
    ROOT / "frontend",
    ROOT / "Frondend",
)
DATA_DIR = ROOT / "data"
if not DATA_DIR.exists():
    DATA_DIR = ROOT / "Data"
DB_PATH = DATA_DIR / "vcc_crm.db"
SHARED_DOCUMENTS_DIR = DATA_DIR / "shared_documents"
SHARED_DOCUMENTS_PREFIX = "/shared-documents/"

service = CRMService(DB_PATH)


class CRMHandler(BaseHTTPRequestHandler):
    server_version = "VCCCRM/2.0"

    @staticmethod
    def pipeline_record_route(path: str, suffix: str = "") -> bool:
        prefixes = ("/api/v1/pipeline-records/", "/api/v1/pipeline_records/")
        for prefix in prefixes:
            if path.startswith(prefix):
                remainder = path[len(prefix):]
                if suffix:
                    return remainder.endswith(suffix) and remainder[:-len(suffix)].rstrip("/").isdigit()
                return remainder.isdigit()
        return False

    @staticmethod
    def pipeline_record_id_from_path(path: str, suffix: str = "") -> int:
        remainder = path.rsplit("/", 1)[0] if suffix else path
        return int(remainder.split("/")[-1])

    @staticmethod
    def pipeline_record_task_route(path: str, suffix: str = "") -> bool:
        prefixes = ("/api/v1/pipeline-record-tasks/", "/api/v1/pipeline_record_tasks/")
        for prefix in prefixes:
            if path.startswith(prefix):
                remainder = path[len(prefix):]
                if suffix:
                    return remainder.endswith(suffix) and remainder[:-len(suffix)].rstrip("/").isdigit()
                return remainder.isdigit()
        return False

    @staticmethod
    def pipeline_record_task_id_from_path(path: str, suffix: str = "") -> int:
        remainder = path.rsplit("/", 1)[0] if suffix else path
        return int(remainder.split("/")[-1])

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith(SHARED_DOCUMENTS_PREFIX):
            self.serve_shared_document(parsed.path)
            return
        if parsed.path.startswith("/api/"):
            self.handle_api_get(parsed)
            return
        self.serve_frontend(parsed.path)

    def do_POST(self):
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self.send_error(404)
            return
        self.handle_api_post(parsed)

    def do_PATCH(self):
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self.send_error(404)
            return
        self.handle_api_patch(parsed)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self.send_error(404)
            return
        self.handle_api_delete(parsed)

    def parse_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def parse_multipart_body(self):
        form = FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE": self.headers.get("Content-Type", ""),
                "CONTENT_LENGTH": self.headers.get("Content-Length", "0"),
            },
            keep_blank_values=True,
        )
        payload = {}
        upload = None
        for key in form.keys():
            field = form[key]
            if isinstance(field, list):
                field = field[0]
            if getattr(field, "filename", None):
                upload = {
                    "filename": Path(field.filename).name,
                    "content_type": field.type or "application/octet-stream",
                    "bytes": field.file.read(),
                }
            else:
                payload[key] = field.value
        if upload:
            payload["_upload"] = upload
        return payload

    @staticmethod
    def safe_upload_name(name: str) -> str:
        original = Path(name or "document").name
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(original).stem).strip(".-") or "document"
        suffix = re.sub(r"[^A-Za-z0-9.]+", "", Path(original).suffix)[:16]
        return f"{stem}-{uuid4().hex[:10]}{suffix}"

    def persist_shared_upload(self, payload: dict) -> dict:
        upload = payload.pop("_upload", None)
        if not upload:
            return payload
        SHARED_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
        stored_name = self.safe_upload_name(upload["filename"])
        target = SHARED_DOCUMENTS_DIR / stored_name
        target.write_bytes(upload["bytes"])
        payload["storage_path"] = f"{SHARED_DOCUMENTS_PREFIX}{stored_name}"
        if not str(payload.get("file_name") or "").strip():
            payload["file_name"] = upload["filename"]
        return payload

    def send_json(self, payload, status=200, content_type="application/json"):
        blob = payload.encode("utf-8") if isinstance(payload, str) else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(blob)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(blob)

    def send_api_error(self, error):
        if isinstance(error, ValidationError):
            self.send_json({"error": {"code": "validation_error", "message": error.message, "fields": error.fields}}, 400)
        elif isinstance(error, NotFoundError):
            self.send_json({"error": {"code": "not_found", "message": str(error)}}, 404)
        else:
            self.send_json({"error": {"code": "server_error", "message": str(error)}}, 500)

    def serve_frontend(self, path: str):
        requested = path.lstrip("/") or "index.html"
        asset = FRONTEND_DIR / requested
        if asset.exists() and asset.is_file():
            content_type = mimetypes.guess_type(str(asset))[0] or "application/octet-stream"
            data = asset.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        index_file = FRONTEND_DIR / "index.html"
        data = index_file.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def serve_shared_document(self, path: str):
        name = Path(unquote(path[len(SHARED_DOCUMENTS_PREFIX):])).name
        if not name:
            self.send_error(404)
            return
        asset = SHARED_DOCUMENTS_DIR / name
        if not asset.exists() or not asset.is_file():
            self.send_error(404)
            return
        content_type = mimetypes.guess_type(str(asset))[0] or "application/octet-stream"
        data = asset.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Disposition", f'inline; filename="{asset.name}"')
        self.end_headers()
        self.wfile.write(data)

    def handle_api_get(self, parsed):
        try:
            path = parsed.path
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            if path == "/api/v1/health":
                self.send_json({"ok": True})
            elif path == "/api/v1/bootstrap":
                self.send_json(service.get_bootstrap())
            elif path == "/api/v1/dashboard":
                self.send_json(service.get_dashboard())
            elif path == "/api/v1/opportunities":
                self.send_json(service.list_opportunities(query))
            elif path == "/api/v1/pipeline/kanban":
                self.send_json(service.list_pipeline_kanban(query))
            elif path.startswith("/api/v1/opportunities/") and path.endswith("/export"):
                opportunity_id = int(path.split("/")[-2])
                self.send_json(service.export_opportunity_report_markdown(opportunity_id), content_type="text/plain; charset=utf-8")
            elif path.startswith("/api/v1/opportunities/"):
                opportunity_id = int(path.split("/")[-1])
                self.send_json(service.get_opportunity(opportunity_id))
            elif path == "/api/v1/tasks":
                self.send_json(service.list_tasks(query))
            elif path == "/api/v1/companies":
                self.send_json(service.list_companies())
            elif path == "/api/v1/users":
                self.send_json(service.list_users())
            elif path == "/api/v1/organizations":
                self.send_json(service.list_organizations(query))
            elif path.startswith("/api/v1/organizations/"):
                organization_id = int(path.split("/")[-1])
                self.send_json(service.get_organization(organization_id))
            elif self.pipeline_record_route(path, "/workflow"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/workflow")
                self.send_json(service.get_pipeline_record_workflow(pipeline_record_id))
            elif self.pipeline_record_route(path, "/notes"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/notes")
                self.send_json(service.list_pipeline_record_notes(pipeline_record_id))
            elif self.pipeline_record_route(path, "/tasks"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/tasks")
                self.send_json(service.list_pipeline_record_tasks(pipeline_record_id, query))
            elif self.pipeline_record_route(path, "/documents"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/documents")
                self.send_json(service.list_pipeline_record_documents(pipeline_record_id))
            elif self.pipeline_record_route(path, "/decisions"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/decisions")
                self.send_json(service.list_pipeline_record_decisions(pipeline_record_id))
            elif self.pipeline_record_route(path, "/export"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/export")
                self.send_json(service.export_pipeline_record_report_markdown(pipeline_record_id), content_type="text/plain; charset=utf-8")
            elif path in {"/api/v1/pipeline-records", "/api/v1/pipeline_records"}:
                self.send_json(service.list_pipeline_records(query))
            elif self.pipeline_record_route(path):
                pipeline_record_id = int(path.split("/")[-1])
                self.send_json(service.get_pipeline_record(pipeline_record_id))
            elif path == "/api/v1/intake-submissions":
                self.send_json(service.list_intake_submissions(query))
            elif path == "/api/v1/relationship-links":
                self.send_json(service.list_relationship_links(query))
            elif path == "/api/v1/reports/reflection":
                self.send_json(service.get_reflection_report())
            elif path == "/api/v1/reports/reflection/export":
                self.send_json(service.export_reflection_report_markdown(), content_type="text/plain; charset=utf-8")
            elif path == "/api/v1/reports/pipeline.csv":
                self.send_json(service.export_pipeline_csv(), content_type="text/csv; charset=utf-8")
            else:
                raise NotFoundError("Unknown API route")
        except Exception as error:
            self.send_api_error(error)

    def handle_api_post(self, parsed):
        try:
            path = parsed.path
            is_document_route = self.pipeline_record_route(path, "/documents") or path.endswith("/documents")
            if is_document_route and self.headers.get("Content-Type", "").startswith("multipart/form-data"):
                body = self.persist_shared_upload(self.parse_multipart_body())
            else:
                body = self.parse_body()
            if path == "/api/v1/opportunities":
                self.send_json(service.create_opportunity(body), 201)
            elif path == "/api/v1/organizations":
                self.send_json(service.create_organization(body), 201)
            elif path == "/api/v1/shared-documents":
                if self.headers.get("Content-Type", "").startswith("multipart/form-data"):
                    self.send_json(self.persist_shared_upload(self.parse_multipart_body()), 201)
                else:
                    self.send_json(body, 201)
            elif path == "/api/v1/intake-autofill-preview":
                self.send_json(service.preview_intake_autofill(body))
            elif path in {"/api/v1/pipeline-records", "/api/v1/pipeline_records"}:
                self.send_json(service.create_pipeline_record(body), 201)
            elif path == "/api/v1/intake-submissions":
                self.send_json(service.create_intake_submission(body), 201)
            elif path == "/api/v1/relationship-links":
                self.send_json(service.create_relationship_link(body), 201)
            elif self.pipeline_record_route(path, "/change-stage"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/change-stage")
                self.send_json(service.change_pipeline_record_stage(pipeline_record_id, body))
            elif self.pipeline_record_route(path, "/notes"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/notes")
                self.send_json(service.add_pipeline_record_note(pipeline_record_id, body), 201)
            elif self.pipeline_record_route(path, "/tasks"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/tasks")
                self.send_json(service.create_pipeline_record_task(pipeline_record_id, body), 201)
            elif self.pipeline_record_route(path, "/documents"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/documents")
                self.send_json(service.add_pipeline_record_document(pipeline_record_id, body), 201)
            elif self.pipeline_record_route(path, "/autofill"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/autofill")
                self.send_json(service.autofill_pipeline_record_from_sources(pipeline_record_id, body))
            elif self.pipeline_record_route(path, "/decision"):
                pipeline_record_id = self.pipeline_record_id_from_path(path, "/decision")
                self.send_json(service.add_pipeline_record_decision(pipeline_record_id, body), 201)
            elif self.pipeline_record_task_route(path, "/comments"):
                pipeline_record_task_id = self.pipeline_record_task_id_from_path(path, "/comments")
                self.send_json(service.add_pipeline_record_task_comment(pipeline_record_task_id, body), 201)
            elif path.endswith("/change-stage"):
                opportunity_id = int(path.split("/")[-2])
                self.send_json(service.change_stage(opportunity_id, body))
            elif path.endswith("/notes"):
                opportunity_id = int(path.split("/")[-2])
                self.send_json(service.add_note(opportunity_id, body), 201)
            elif path.endswith("/tasks"):
                body["opportunity_id"] = int(path.split("/")[-2])
                self.send_json(service.create_task(body), 201)
            elif path.endswith("/documents"):
                opportunity_id = int(path.split("/")[-2])
                self.send_json(service.add_document(opportunity_id, body), 201)
            elif path.endswith("/decision"):
                opportunity_id = int(path.split("/")[-2])
                self.send_json(service.add_decision(opportunity_id, body), 201)
            elif path.startswith("/api/v1/tasks/") and path.endswith("/comments"):
                task_id = int(path.split("/")[-2])
                self.send_json(service.add_task_comment(task_id, body), 201)
            else:
                raise NotFoundError("Unknown API route")
        except Exception as error:
            self.send_api_error(error)

    def handle_api_patch(self, parsed):
        try:
            path = parsed.path
            body = self.parse_body()
            if path.startswith("/api/v1/opportunities/"):
                opportunity_id = int(path.split("/")[-1])
                self.send_json(service.update_opportunity(opportunity_id, body))
            elif path.startswith("/api/v1/organizations/"):
                organization_id = int(path.split("/")[-1])
                self.send_json(service.update_organization(organization_id, body))
            elif self.pipeline_record_route(path):
                pipeline_record_id = int(path.split("/")[-1])
                self.send_json(service.update_pipeline_record(pipeline_record_id, body))
            elif self.pipeline_record_task_route(path):
                pipeline_record_task_id = int(path.split("/")[-1])
                self.send_json(service.update_pipeline_record_task(pipeline_record_task_id, body))
            elif path.startswith("/api/v1/tasks/"):
                task_id = int(path.split("/")[-1])
                self.send_json(service.update_task(task_id, body))
            elif path.startswith("/api/v1/notes/"):
                note_id = int(path.split("/")[-1])
                self.send_json(service.update_note(note_id, body))
            else:
                raise NotFoundError("Unknown API route")
        except Exception as error:
            self.send_api_error(error)

    def handle_api_delete(self, parsed):
        try:
            path = parsed.path
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            actor_id = int(query.get("actor_user_id", "2"))
            if path.startswith("/api/v1/opportunities/"):
                opportunity_id = int(path.split("/")[-1])
                self.send_json(service.delete_opportunity(opportunity_id, actor_id))
            elif self.pipeline_record_task_route(path):
                pipeline_record_task_id = int(path.split("/")[-1])
                self.send_json(service.delete_pipeline_record_task(pipeline_record_task_id, actor_id))
            elif path.startswith("/api/v1/tasks/"):
                task_id = int(path.split("/")[-1])
                self.send_json(service.delete_task(task_id, actor_id))
            elif path.startswith("/api/v1/notes/"):
                note_id = int(path.split("/")[-1])
                self.send_json(service.delete_note(note_id, actor_id))
            else:
                raise NotFoundError("Unknown API route")
        except Exception as error:
            self.send_api_error(error)


def run():
    service.init_db()
    port = int(os.environ.get("PORT") or (sys.argv[1] if len(sys.argv) > 1 else 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    server = ThreadingHTTPServer((host, port), CRMHandler)
    print(f"VCC CRM running at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
