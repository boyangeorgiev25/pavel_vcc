import csv
import io
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from autofill_pipeline import run_pipeline_record_autofill


STAGES = [
    {"id": "new", "label": "New", "rank": 1},
    {"id": "screening", "label": "Screening", "rank": 2},
    {"id": "qualified", "label": "Qualified", "rank": 3},
    {"id": "ic_review", "label": "IC Review", "rank": 4},
    {"id": "term_sheet", "label": "Term Sheet", "rank": 5},
    {"id": "closed_won", "label": "Closed Won", "rank": 6},
    {"id": "closed_lost", "label": "Closed Lost", "rank": 7},
]

STAGE_LABELS = {item["id"]: item["label"] for item in STAGES}
STAGE_RANKS = {item["id"]: item["rank"] for item in STAGES}
ACTIVE_STAGES = {"new", "screening", "qualified", "ic_review", "term_sheet"}
DECISION_STAGES = {"qualified", "ic_review", "term_sheet"}
PRIORITIES = ["low", "medium", "high", "critical"]
PRIORITY_SCORES = {"low": 25, "medium": 50, "high": 75, "critical": 90}
PRIORITY_LABELS = {
    "low": "P4",
    "medium": "P3",
    "high": "P2",
    "critical": "P1",
}
TASK_PRIORITIES = ["low", "medium", "high", "urgent"]
TASK_STATUSES = ["todo", "in_progress", "blocked", "done", "canceled"]
OPPORTUNITY_STATUSES = ["active", "on_hold", "closed_won", "closed_lost"]
USER_ROLES = ["analyst", "associate", "partner", "admin"]
NOTE_TYPES = ["general", "research", "meeting", "concern", "decision_prep"]
INTAKE_STATUSES = ["new", "triaged", "converted", "archived"]
INTAKE_OUTCOMES = ["pending", "direct_deal", "co_investment", "relationship_first", "duplicate_or_noise"]
RELATIONSHIP_LINK_TYPES = ["introduced_by", "co_investor", "advisor", "internal_sponsor", "service_provider"]
RELATIONSHIP_STATUSES = ["active", "warm", "watch", "do_not_engage"]
ORGANIZATION_TYPES = ["company", "investor", "fund", "bank", "advisor", "service_provider", "other"]
PIPELINE_RECORD_STAGES = [
    "intake",
    "triage",
    "screening",
    "qualified",
    "diligence",
    "ic_preparation",
    "ic_decision",
    "closing",
    "invested",
    "passed",
    "archived",
]
PIPELINE_RECORD_STATUSES = ["active", "on_hold", "waiting_external", "approved", "passed", "archived"]
DECISION_OUTCOMES = ["pending", "approved", "approved_with_conditions", "recycle", "declined"]

REQUIRED_FIELDS_BY_STAGE = {
    "new": ["company_name", "source_detail", "owner_user_id", "sector", "geography", "primary_contact_name"],
    "screening": [
        "company_name",
        "source_detail",
        "owner_user_id",
        "sector",
        "geography",
        "primary_contact_name",
        "round_name",
        "fund_fit",
        "next_step",
    ],
    "qualified": [
        "company_name",
        "source_detail",
        "owner_user_id",
        "sector",
        "geography",
        "primary_contact_name",
        "round_name",
        "fund_fit",
        "next_step",
        "ticket_size_target",
        "investment_thesis",
        "key_concerns",
        "ownership_target_pct",
    ],
    "ic_review": [
        "company_name",
        "source_detail",
        "owner_user_id",
        "sector",
        "geography",
        "primary_contact_name",
        "round_name",
        "fund_fit",
        "next_step",
        "ticket_size_target",
        "investment_thesis",
        "key_concerns",
        "ownership_target_pct",
        "decision_due_at",
    ],
    "term_sheet": [
        "company_name",
        "source_detail",
        "owner_user_id",
        "sector",
        "geography",
        "primary_contact_name",
        "round_name",
        "fund_fit",
        "ticket_size_target",
        "investment_thesis",
        "key_concerns",
        "ownership_target_pct",
        "decision_due_at",
        "nda_status",
    ],
}


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    website TEXT,
    sector TEXT,
    subsector TEXT,
    geography TEXT,
    hq_city TEXT,
    hq_country TEXT,
    business_model TEXT,
    description TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    title TEXT,
    email TEXT,
    phone TEXT,
    linkedin_url TEXT,
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_code TEXT NOT NULL UNIQUE,
    company_id INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    source_detail TEXT NOT NULL,
    owner_user_id INTEGER,
    stage TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    priority TEXT NOT NULL DEFAULT 'medium',
    priority_score INTEGER NOT NULL DEFAULT 50,
    fund_fit TEXT,
    fund_fit_score INTEGER NOT NULL DEFAULT 0,
    market_score INTEGER NOT NULL DEFAULT 0,
    team_score INTEGER NOT NULL DEFAULT 0,
    traction_score INTEGER NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'EUR',
    round_name TEXT,
    ticket_size_target REAL,
    ticket_size_min REAL,
    ticket_size_max REAL,
    valuation_min REAL,
    valuation_max REAL,
    ownership_target_pct REAL,
    next_step TEXT,
    next_step_due_at TEXT,
    last_contacted_at TEXT,
    decision_due_at TEXT,
    investment_thesis TEXT,
    key_concerns TEXT,
    relationship_notes TEXT,
    nda_required INTEGER NOT NULL DEFAULT 0,
    nda_status TEXT,
    workflow_flags TEXT NOT NULL DEFAULT '[]',
    risk_flags TEXT NOT NULL DEFAULT '[]',
    missing_fields TEXT NOT NULL DEFAULT '[]',
    tags TEXT NOT NULL DEFAULT '[]',
    last_activity_at TEXT,
    stage_entered_at TEXT NOT NULL,
    created_by_user_id INTEGER,
    updated_by_user_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deleted_at TEXT,
    deleted_by_user_id INTEGER,
    FOREIGN KEY(company_id) REFERENCES companies(id),
    FOREIGN KEY(owner_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS stage_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id INTEGER NOT NULL,
    from_stage TEXT,
    to_stage TEXT NOT NULL,
    changed_by_user_id INTEGER,
    reason TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE,
    FOREIGN KEY(changed_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id INTEGER NOT NULL,
    author_user_id INTEGER,
    note_type TEXT NOT NULL,
    body TEXT NOT NULL,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE,
    FOREIGN KEY(author_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    assigned_user_id INTEGER,
    created_by_user_id INTEGER,
    due_at TEXT,
    completed_at TEXT,
    is_blocking INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE,
    FOREIGN KEY(assigned_user_id) REFERENCES users(id),
    FOREIGN KEY(created_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS task_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    user_id INTEGER,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    document_category TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    uploaded_by_user_id INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE,
    FOREIGN KEY(uploaded_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS decision_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id INTEGER NOT NULL,
    decision_type TEXT NOT NULL,
    decision_summary TEXT NOT NULL,
    rationale TEXT,
    decided_by_user_id INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE,
    FOREIGN KEY(decided_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS pipeline_record_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_record_id INTEGER NOT NULL,
    author_user_id INTEGER,
    note_type TEXT NOT NULL,
    body TEXT NOT NULL,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(pipeline_record_id) REFERENCES pipeline_records(id) ON DELETE CASCADE,
    FOREIGN KEY(author_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS pipeline_record_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_record_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    assigned_user_id INTEGER,
    created_by_user_id INTEGER,
    due_at TEXT,
    completed_at TEXT,
    is_blocking INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(pipeline_record_id) REFERENCES pipeline_records(id) ON DELETE CASCADE,
    FOREIGN KEY(assigned_user_id) REFERENCES users(id),
    FOREIGN KEY(created_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS pipeline_record_task_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_record_task_id INTEGER NOT NULL,
    user_id INTEGER,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(pipeline_record_task_id) REFERENCES pipeline_record_tasks(id) ON DELETE CASCADE,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS pipeline_record_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_record_id INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    document_category TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    uploaded_by_user_id INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(pipeline_record_id) REFERENCES pipeline_records(id) ON DELETE CASCADE,
    FOREIGN KEY(uploaded_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS pipeline_record_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_record_id INTEGER NOT NULL,
    decision_type TEXT NOT NULL,
    decision_summary TEXT NOT NULL,
    rationale TEXT,
    decided_by_user_id INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY(pipeline_record_id) REFERENCES pipeline_records(id) ON DELETE CASCADE,
    FOREIGN KEY(decided_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS activities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id INTEGER,
    task_id INTEGER,
    user_id INTEGER,
    activity_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id INTEGER,
    summary TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE,
    FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    actor_user_id INTEGER,
    before_json TEXT,
    after_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(actor_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS organizations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    legacy_company_id INTEGER UNIQUE,
    organization_type TEXT NOT NULL DEFAULT 'company',
    name TEXT NOT NULL,
    website TEXT,
    sector_primary TEXT,
    subsector TEXT,
    geography TEXT,
    hq_city TEXT,
    hq_country TEXT,
    business_model TEXT,
    description TEXT,
    owner_user_id INTEGER,
    relationship_status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    archived_at TEXT,
    FOREIGN KEY(legacy_company_id) REFERENCES companies(id),
    FOREIGN KEY(owner_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS organization_contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    title TEXT,
    email TEXT,
    phone TEXT,
    linkedin_url TEXT,
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(organization_id) REFERENCES organizations(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pipeline_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id INTEGER NOT NULL,
    legacy_opportunity_id INTEGER UNIQUE,
    record_code TEXT NOT NULL UNIQUE,
    stage TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    fund_fit TEXT,
    source_type TEXT,
    source_detail TEXT,
    priority TEXT NOT NULL DEFAULT 'medium',
    owner_user_id INTEGER,
    next_step TEXT,
    next_step_due_at TEXT,
    round_name TEXT,
    ticket_size_target TEXT,
    ownership_target_pct TEXT,
    valuation_min TEXT,
    valuation_max TEXT,
    annual_recurring_revenue TEXT,
    revenue_growth_pct TEXT,
    gross_margin_pct TEXT,
    ebitda_margin_pct TEXT,
    rule_of_40_pct TEXT,
    monthly_burn TEXT,
    cash_runway_months TEXT,
    financials_updated_at TEXT,
    investment_thesis TEXT,
    key_concerns TEXT,
    relationship_notes TEXT,
    nda_required INTEGER NOT NULL DEFAULT 0,
    nda_status TEXT NOT NULL DEFAULT 'not_required',
    risk_flags TEXT NOT NULL DEFAULT '[]',
    tags TEXT NOT NULL DEFAULT '[]',
    decision_due_at TEXT,
    decision_outcome TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(organization_id) REFERENCES organizations(id) ON DELETE CASCADE,
    FOREIGN KEY(legacy_opportunity_id) REFERENCES opportunities(id),
    FOREIGN KEY(owner_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS pipeline_record_stage_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_record_id INTEGER NOT NULL,
    from_stage TEXT,
    to_stage TEXT NOT NULL,
    changed_by_user_id INTEGER,
    reason TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(pipeline_record_id) REFERENCES pipeline_records(id) ON DELETE CASCADE,
    FOREIGN KEY(changed_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS pipeline_record_activities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_record_id INTEGER NOT NULL,
    task_id INTEGER,
    user_id INTEGER,
    activity_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id INTEGER,
    summary TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    FOREIGN KEY(pipeline_record_id) REFERENCES pipeline_records(id) ON DELETE CASCADE,
    FOREIGN KEY(task_id) REFERENCES pipeline_record_tasks(id) ON DELETE SET NULL,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS intake_submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    submitted_by_name TEXT,
    submitted_by_email TEXT,
    company_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    intake_kind TEXT NOT NULL DEFAULT 'direct_deal',
    status TEXT NOT NULL DEFAULT 'new',
    outcome TEXT NOT NULL DEFAULT 'pending',
    owner_user_id INTEGER,
    summary TEXT,
    raw_payload TEXT NOT NULL DEFAULT '{}',
    notes TEXT,
    converted_company_id INTEGER,
    converted_opportunity_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(owner_user_id) REFERENCES users(id),
    FOREIGN KEY(converted_company_id) REFERENCES companies(id),
    FOREIGN KEY(converted_opportunity_id) REFERENCES opportunities(id)
);

CREATE TABLE IF NOT EXISTS relationship_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER,
    contact_id INTEGER,
    opportunity_id INTEGER,
    related_company_id INTEGER,
    related_contact_id INTEGER,
    link_type TEXT NOT NULL,
    relationship_status TEXT NOT NULL DEFAULT 'active',
    owner_user_id INTEGER,
    warmth TEXT,
    notes TEXT,
    last_touch_at TEXT,
    next_touch_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE,
    FOREIGN KEY(contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
    FOREIGN KEY(opportunity_id) REFERENCES opportunities(id) ON DELETE CASCADE,
    FOREIGN KEY(related_company_id) REFERENCES companies(id) ON DELETE CASCADE,
    FOREIGN KEY(related_contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
    FOREIGN KEY(owner_user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_opportunities_stage ON opportunities(stage);
CREATE INDEX IF NOT EXISTS idx_opportunities_owner ON opportunities(owner_user_id);
CREATE INDEX IF NOT EXISTS idx_opportunities_priority ON opportunities(priority);
CREATE INDEX IF NOT EXISTS idx_opportunities_updated ON opportunities(updated_at);
CREATE INDEX IF NOT EXISTS idx_tasks_assignee_due ON tasks(assigned_user_id, status, due_at);
CREATE INDEX IF NOT EXISTS idx_activities_opportunity_created ON activities(opportunity_id, created_at);
CREATE INDEX IF NOT EXISTS idx_stage_history_opportunity_created ON stage_history(opportunity_id, created_at);
CREATE INDEX IF NOT EXISTS idx_organizations_type_name ON organizations(organization_type, name);
CREATE INDEX IF NOT EXISTS idx_pipeline_records_stage_owner ON pipeline_records(stage, owner_user_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_records_status_stage ON pipeline_records(status, stage);
CREATE INDEX IF NOT EXISTS idx_pipeline_record_notes_record_created ON pipeline_record_notes(pipeline_record_id, created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_record_tasks_record_status_due ON pipeline_record_tasks(pipeline_record_id, status, due_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_record_documents_record_created ON pipeline_record_documents(pipeline_record_id, created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_record_decisions_record_created ON pipeline_record_decisions(pipeline_record_id, created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_record_stage_history_record_created ON pipeline_record_stage_history(pipeline_record_id, created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_record_activities_record_created ON pipeline_record_activities(pipeline_record_id, created_at);
CREATE INDEX IF NOT EXISTS idx_organization_contacts_org_primary ON organization_contacts(organization_id, is_primary);
CREATE INDEX IF NOT EXISTS idx_intake_status_created ON intake_submissions(status, created_at);
CREATE INDEX IF NOT EXISTS idx_intake_outcome_created ON intake_submissions(outcome, created_at);
CREATE INDEX IF NOT EXISTS idx_relationship_links_owner_type ON relationship_links(owner_user_id, link_type);
"""


SEED_USERS = [
    {"name": "Marc Debois", "email": "marc.debois@fo-next.com", "role": "partner"},
    {"name": "Pavel Vassilev", "email": "pavel@fo-next.com", "role": "analyst"},
    {"name": "Sebastian Brocker", "email": "sebastian@fo-next.com", "role": "associate"},
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def to_json(value) -> str:
    return json.dumps(value or [])


def from_json(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


class ValidationError(Exception):
    def __init__(self, message: str, fields: Optional[Dict[str, str]] = None):
        super().__init__(message)
        self.message = message
        self.fields = fields or {}


class NotFoundError(Exception):
    pass


class CRMService:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    @contextmanager
    def connect(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)
            self._ensure_schema_updates(conn)
            count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if count == 0:
                self._seed(conn)
            self._cleanup_seed_users(conn)
            self._backfill_operating_system_entities(conn)
            self._sync_all_legacy_pipeline_records(conn)

    def _ensure_table_columns(self, conn, table_name: str, columns: Dict[str, str]) -> None:
        existing = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        for column_name, column_sql in columns.items():
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

    def _ensure_schema_updates(self, conn) -> None:
        self._ensure_table_columns(
            conn,
            "pipeline_records",
            {
                "round_name": "TEXT",
                "ticket_size_target": "TEXT",
                "ownership_target_pct": "TEXT",
                "valuation_min": "TEXT",
                "valuation_max": "TEXT",
                "annual_recurring_revenue": "TEXT",
                "revenue_growth_pct": "TEXT",
                "gross_margin_pct": "TEXT",
                "ebitda_margin_pct": "TEXT",
                "rule_of_40_pct": "TEXT",
                "monthly_burn": "TEXT",
                "cash_runway_months": "TEXT",
                "financials_updated_at": "TEXT",
                "relationship_notes": "TEXT",
                "nda_required": "INTEGER NOT NULL DEFAULT 0",
                "nda_status": "TEXT NOT NULL DEFAULT 'not_required'",
                "risk_flags": "TEXT NOT NULL DEFAULT '[]'",
                "tags": "TEXT NOT NULL DEFAULT '[]'",
                "decision_due_at": "TEXT",
            },
        )

    def _sync_all_legacy_pipeline_records(self, conn) -> None:
        rows = conn.execute("SELECT id FROM opportunities").fetchall()
        for row in rows:
            self._sync_opportunity_to_pipeline_record(conn, row["id"])
        org_rows = conn.execute("SELECT id FROM organizations").fetchall()
        for row in org_rows:
            self._sync_legacy_contacts_to_organization(conn, row["id"])

    def _fetch_primary_organization_contact(self, conn, organization_id: int) -> Optional[dict]:
        row = conn.execute(
            """
            SELECT * FROM organization_contacts
            WHERE organization_id = ?
            ORDER BY is_primary DESC, id ASC
            LIMIT 1
            """,
            (organization_id,),
        ).fetchone()
        return self._row(row) if row else None

    def _upsert_primary_organization_contact(self, conn, organization_id: int, payload: dict) -> Optional[int]:
        name = (payload.get("primary_contact_name") or "").strip()
        title = (payload.get("primary_contact_title") or "").strip()
        email = (payload.get("primary_contact_email") or "").strip()
        phone = (payload.get("primary_contact_phone") or "").strip()
        linkedin_url = (payload.get("primary_contact_linkedin_url") or "").strip()
        if not any([name, title, email, phone, linkedin_url]):
            return None
        primary = self._fetch_primary_organization_contact(conn, organization_id)
        now = iso_now()
        if primary:
            conn.execute(
                """
                UPDATE organization_contacts
                SET name = ?, title = ?, email = ?, phone = ?, linkedin_url = ?, is_primary = 1, updated_at = ?
                WHERE id = ?
                """,
                (
                    name or primary.get("name") or "",
                    title or primary.get("title") or "",
                    email or primary.get("email") or "",
                    phone or primary.get("phone") or "",
                    linkedin_url or primary.get("linkedin_url") or "",
                    now,
                    primary["id"],
                ),
            )
            return primary["id"]
        return conn.execute(
            """
            INSERT INTO organization_contacts(
                organization_id, name, title, email, phone, linkedin_url, is_primary, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                organization_id,
                name,
                title,
                email,
                phone,
                linkedin_url,
                now,
                now,
            ),
        ).lastrowid

    def _sync_legacy_contacts_to_organization(self, conn, organization_id: int) -> Optional[int]:
        organization = conn.execute(
            "SELECT legacy_company_id FROM organizations WHERE id = ?",
            (organization_id,),
        ).fetchone()
        if not organization or not organization["legacy_company_id"]:
            return None
        primary = self._fetch_primary_contact(conn, organization["legacy_company_id"])
        if not primary:
            return None
        return self._upsert_primary_organization_contact(
            conn,
            organization_id,
            {
                "primary_contact_name": primary.get("name"),
                "primary_contact_title": primary.get("title"),
                "primary_contact_email": primary.get("email"),
                "primary_contact_phone": primary.get("phone"),
                "primary_contact_linkedin_url": primary.get("linkedin_url"),
            },
        )

    def _seed(self, conn: sqlite3.Connection) -> None:
        now = iso_now()
        for user in SEED_USERS:
            conn.execute(
                "INSERT INTO users(name, email, role, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (user["name"], user["email"], user["role"], now, now),
            )
        companies = [
            {
                "name": "Sandra Labs",
                "website": "https://sandralabs.example",
                "sector": "AI",
                "subsector": "Human interface",
                "geography": "Europe",
                "hq_city": "Amsterdam",
                "hq_country": "Netherlands",
                "business_model": "Deeptech platform",
                "description": "AI-human interface company discussed after founder call; promising but timing-sensitive.",
                "contact": ("Lea Sanders", "CEO", "lea@sandralabs.example", "+31 555 1000"),
            },
            {
                "name": "Solve Intelligence",
                "website": "https://solveintelligence.com",
                "sector": "AI",
                "subsector": "IP workflow",
                "geography": "UK",
                "hq_city": "London",
                "hq_country": "United Kingdom",
                "business_model": "B2B SaaS",
                "description": "Later-stage software opportunity with strong ARR and fit.",
                "contact": ("Dan Morgan", "CFO", "dan@solveintelligence.com", "+44 20 1000 1000"),
            },
            {
                "name": "Aikido Security",
                "website": "https://aikido.dev",
                "sector": "Cybersecurity",
                "subsector": "Developer security",
                "geography": "Belgium",
                "hq_city": "Ghent",
                "hq_country": "Belgium",
                "business_model": "B2B SaaS",
                "description": "Fast-growing security platform, but ownership and valuation still unclear.",
                "contact": ("Willem De Vos", "Founder", "willem@aikido.dev", "+32 9 100 100"),
            },
            {
                "name": "Ypsilon",
                "website": "https://ypsilon.example",
                "sector": "Confidential",
                "subsector": "Under NDA",
                "geography": "Europe",
                "hq_city": "Zurich",
                "hq_country": "Switzerland",
                "business_model": "Enterprise software",
                "description": "Opportunity blocked on NDA and open questions.",
                "contact": ("Confidential Contact", "Founder", "nda@ypsilon.example", ""),
            },
            {
                "name": "Triple Helix",
                "website": "https://triplehelix.example",
                "sector": "Healthtech",
                "subsector": "Advanced systems",
                "geography": "Europe",
                "hq_city": "Munich",
                "hq_country": "Germany",
                "business_model": "Platform",
                "description": "Still information-light; requires more diligence than the deck alone.",
                "contact": ("Anna Richter", "Founder", "anna@triplehelix.example", "+49 89 100 100"),
            },
            {
                "name": "North Sea Optics",
                "website": "https://northseaoptics.example",
                "sector": "Photonics",
                "subsector": "Industrial sensing",
                "geography": "Benelux",
                "hq_city": "Rotterdam",
                "hq_country": "Netherlands",
                "business_model": "Hardware + software",
                "description": "Partner-sourced precision deal with strong strategic fit but missing economics.",
                "contact": ("Aline Vermeer", "CEO", "aline@northseaoptics.example", "+31 10 444 1000"),
            },
            {
                "name": "Formalize",
                "website": "https://formalize.com",
                "sector": "Compliance",
                "subsector": "GRC",
                "geography": "Nordics",
                "hq_city": "Copenhagen",
                "hq_country": "Denmark",
                "business_model": "B2B SaaS",
                "description": "High-quality growth opportunity that needs investment committee discussion.",
                "contact": ("Lise Holm", "VP Finance", "lise@formalize.com", "+45 30 10 10"),
            },
            {
                "name": "QLM Technology",
                "website": "https://qlmtec.com",
                "sector": "Climate Infra",
                "subsector": "Methane sensing",
                "geography": "UK",
                "hq_city": "Paignton",
                "hq_country": "United Kingdom",
                "business_model": "Hardware + recurring service",
                "description": "Strong precision fit, closer to term-sheet-ready than most deals.",
                "contact": ("Harriet Cole", "COO", "harriet@qlmtec.com", "+44 1803 100100"),
            },
        ]

        company_ids: Dict[str, int] = {}
        for company in companies:
            cursor = conn.execute(
                """
                INSERT INTO companies(
                    name, website, sector, subsector, geography, hq_city,
                    hq_country, business_model, description, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company["name"],
                    company["website"],
                    company["sector"],
                    company["subsector"],
                    company["geography"],
                    company["hq_city"],
                    company["hq_country"],
                    company["business_model"],
                    company["description"],
                    now,
                    now,
                ),
            )
            company_id = cursor.lastrowid
            company_ids[company["name"]] = company_id
            contact = company["contact"]
            conn.execute(
                """
                INSERT INTO contacts(company_id, name, title, email, phone, is_primary, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (company_id, contact[0], contact[1], contact[2], contact[3], now, now),
            )

        opportunities = [
            {
                "deal_code": "VCC-001",
                "company_name": "Solve Intelligence",
                "source_type": "desk_research",
                "source_detail": "Website, LinkedIn, and company blog review",
                "owner_user_id": 2,
                "stage": "qualified",
                "status": "active",
                "priority": "high",
                "priority_score": 78,
                "fund_fit": "Growth",
                "fund_fit_score": 18,
                "market_score": 15,
                "team_score": 13,
                "traction_score": 16,
                "round_name": "Series B",
                "ticket_size_target": 12000000,
                "ticket_size_min": 10000000,
                "ticket_size_max": 14000000,
                "valuation_min": 160000000,
                "valuation_max": 190000000,
                "ownership_target_pct": 10.0,
                "next_step": "Validate current round access and confirm customer concentration",
                "next_step_due_at": (utc_now() + timedelta(days=3)).date().isoformat(),
                "last_contacted_at": (utc_now() - timedelta(days=2)).date().isoformat(),
                "decision_due_at": (utc_now() + timedelta(days=5)).date().isoformat(),
                "investment_thesis": "Compelling later-stage software fit with visible ARR scale, product depth, and defendable workflow embedding.",
                "key_concerns": "Need more clarity on concentration and allocation room.",
                "relationship_notes": "No conflicts flagged.",
                "nda_required": 0,
                "nda_status": "not_required",
                "workflow_flags": ["ready_for_review"],
                "risk_flags": ["allocation_room"],
                "tags": ["ai", "growth", "software"],
                "created_by_user_id": 2,
                "updated_by_user_id": 2,
                "age_days": 11,
            },
            {
                "deal_code": "VCC-002",
                "company_name": "Aikido Security",
                "source_type": "network",
                "source_detail": "Tech.eu article forwarded through network",
                "owner_user_id": 3,
                "stage": "screening",
                "status": "active",
                "priority": "medium",
                "priority_score": 61,
                "fund_fit": "Growth",
                "fund_fit_score": 15,
                "market_score": 13,
                "team_score": 12,
                "traction_score": 11,
                "round_name": "Series B",
                "ticket_size_target": 9000000,
                "ticket_size_min": 8000000,
                "ticket_size_max": 12000000,
                "valuation_min": None,
                "valuation_max": None,
                "ownership_target_pct": None,
                "next_step": "Pressure-test ownership math and current pricing",
                "next_step_due_at": (utc_now() + timedelta(days=1)).date().isoformat(),
                "last_contacted_at": (utc_now() - timedelta(days=3)).date().isoformat(),
                "decision_due_at": None,
                "investment_thesis": "Fast growth, relevant vertical, high strategic fit if economics work.",
                "key_concerns": "Ownership room and valuation discipline still unclear.",
                "relationship_notes": "",
                "nda_required": 0,
                "nda_status": "not_required",
                "workflow_flags": ["needs_input"],
                "risk_flags": ["valuation_risk"],
                "tags": ["cybersecurity", "growth"],
                "created_by_user_id": 3,
                "updated_by_user_id": 3,
                "age_days": 8,
            },
            {
                "deal_code": "VCC-003",
                "company_name": "Sandra Labs",
                "source_type": "partner_referral",
                "source_detail": "Founder call and internal follow-up research",
                "owner_user_id": 3,
                "stage": "screening",
                "status": "active",
                "priority": "medium",
                "priority_score": 59,
                "fund_fit": "Precise",
                "fund_fit_score": 17,
                "market_score": 12,
                "team_score": 10,
                "traction_score": 8,
                "round_name": "Strategic round",
                "ticket_size_target": 5000000,
                "ticket_size_min": 4000000,
                "ticket_size_max": 6000000,
                "valuation_min": None,
                "valuation_max": None,
                "ownership_target_pct": 12.0,
                "next_step": "Finish competition analysis and assess whether to engage before June round",
                "next_step_due_at": (utc_now() + timedelta(days=2)).date().isoformat(),
                "last_contacted_at": (utc_now() - timedelta(days=1)).date().isoformat(),
                "decision_due_at": None,
                "investment_thesis": "Interesting AI-human interface wedge with strong technical signal and strategic optionality.",
                "key_concerns": "Pre-revenue, unclear acquisition logic, and timing may be wrong.",
                "relationship_notes": "Marc wants broader internal visibility once the NDA path is clearer.",
                "nda_required": 0,
                "nda_status": "not_required",
                "workflow_flags": ["needs_research"],
                "risk_flags": ["timing_risk", "strategy_risk"],
                "tags": ["deeptech", "ai", "precision"],
                "created_by_user_id": 3,
                "updated_by_user_id": 3,
                "age_days": 7,
            },
            {
                "deal_code": "VCC-004",
                "company_name": "Ypsilon",
                "source_type": "inbound",
                "source_detail": "Inbound opportunity awaiting NDA package",
                "owner_user_id": 2,
                "stage": "new",
                "status": "on_hold",
                "priority": "medium",
                "priority_score": 52,
                "fund_fit": "Growth",
                "fund_fit_score": 10,
                "market_score": 9,
                "team_score": 9,
                "traction_score": 8,
                "round_name": "Unknown",
                "ticket_size_target": None,
                "ticket_size_min": None,
                "ticket_size_max": None,
                "valuation_min": None,
                "valuation_max": None,
                "ownership_target_pct": None,
                "next_step": "Send 4-5 key questions with signed NDA",
                "next_step_due_at": (utc_now() + timedelta(days=2)).date().isoformat(),
                "last_contacted_at": utc_now().date().isoformat(),
                "decision_due_at": None,
                "investment_thesis": "",
                "key_concerns": "Zero materials shared yet; blocked on NDA.",
                "relationship_notes": "",
                "nda_required": 1,
                "nda_status": "awaiting_signature",
                "workflow_flags": ["waiting_on_external", "missing_critical_fields"],
                "risk_flags": ["nda_blocked"],
                "tags": ["confidential"],
                "created_by_user_id": 2,
                "updated_by_user_id": 2,
                "age_days": 4,
            },
            {
                "deal_code": "VCC-005",
                "company_name": "Triple Helix",
                "source_type": "network",
                "source_detail": "Marc intro and pipeline handoff to Pavel",
                "owner_user_id": 2,
                "stage": "screening",
                "status": "active",
                "priority": "medium",
                "priority_score": 57,
                "fund_fit": "Precise",
                "fund_fit_score": 15,
                "market_score": 10,
                "team_score": 11,
                "traction_score": 9,
                "round_name": "Series A",
                "ticket_size_target": 4000000,
                "ticket_size_min": 3000000,
                "ticket_size_max": 5000000,
                "valuation_min": None,
                "valuation_max": None,
                "ownership_target_pct": 15.0,
                "next_step": "Extend research beyond pitch deck and map technical diligence gaps",
                "next_step_due_at": (utc_now() + timedelta(days=4)).date().isoformat(),
                "last_contacted_at": (utc_now() - timedelta(days=2)).date().isoformat(),
                "decision_due_at": None,
                "investment_thesis": "Potential strong precision fit if the technical moat and buyer demand hold up.",
                "key_concerns": "Still too deck-driven; not enough independent validation.",
                "relationship_notes": "",
                "nda_required": 0,
                "nda_status": "not_required",
                "workflow_flags": ["needs_research"],
                "risk_flags": ["technical_risk"],
                "tags": ["healthtech", "precision"],
                "created_by_user_id": 2,
                "updated_by_user_id": 2,
                "age_days": 6,
            },
            {
                "deal_code": "VCC-006",
                "company_name": "North Sea Optics",
                "source_type": "conference",
                "source_detail": "Partner intake from conference follow-up",
                "owner_user_id": 1,
                "stage": "new",
                "status": "active",
                "priority": "high",
                "priority_score": 67,
                "fund_fit": "Precise",
                "fund_fit_score": 18,
                "market_score": 11,
                "team_score": 12,
                "traction_score": 8,
                "round_name": "Series A",
                "ticket_size_target": None,
                "ticket_size_min": None,
                "ticket_size_max": None,
                "valuation_min": None,
                "valuation_max": None,
                "ownership_target_pct": None,
                "next_step": "Triage intake and request core financial fields",
                "next_step_due_at": (utc_now() + timedelta(days=1)).date().isoformat(),
                "last_contacted_at": None,
                "decision_due_at": None,
                "investment_thesis": "Strong photonics pattern, but economics and contact depth are incomplete.",
                "key_concerns": "Too little financial data and no full market map yet.",
                "relationship_notes": "Introduced by VCC partner; wants quick first answer.",
                "nda_required": 0,
                "nda_status": "not_required",
                "workflow_flags": ["needs_input", "missing_critical_fields"],
                "risk_flags": ["missing_financials"],
                "tags": ["photonics", "intake"],
                "created_by_user_id": 1,
                "updated_by_user_id": 1,
                "age_days": 3,
            },
            {
                "deal_code": "VCC-007",
                "company_name": "Formalize",
                "source_type": "desk_research",
                "source_detail": "Internal sourcing after growth market map review",
                "owner_user_id": 3,
                "stage": "ic_review",
                "status": "active",
                "priority": "critical",
                "priority_score": 84,
                "fund_fit": "Growth",
                "fund_fit_score": 19,
                "market_score": 15,
                "team_score": 14,
                "traction_score": 17,
                "round_name": "Series B",
                "ticket_size_target": 15000000,
                "ticket_size_min": 12000000,
                "ticket_size_max": 18000000,
                "valuation_min": 250000000,
                "valuation_max": 310000000,
                "ownership_target_pct": 8.0,
                "next_step": "Prepare IC memo and partner recommendation",
                "next_step_due_at": utc_now().date().isoformat(),
                "last_contacted_at": (utc_now() - timedelta(days=1)).date().isoformat(),
                "decision_due_at": (utc_now() + timedelta(days=2)).date().isoformat(),
                "investment_thesis": "High quality compliance infrastructure with strong growth profile and market timing.",
                "key_concerns": "Need cleaner ownership math and confirmation that round access is real.",
                "relationship_notes": "No conflict issues found.",
                "nda_required": 0,
                "nda_status": "not_required",
                "workflow_flags": ["decision_needed_this_week"],
                "risk_flags": ["ownership_risk"],
                "tags": ["growth", "compliance", "ic"],
                "created_by_user_id": 3,
                "updated_by_user_id": 3,
                "age_days": 16,
            },
            {
                "deal_code": "VCC-008",
                "company_name": "QLM Technology",
                "source_type": "network",
                "source_detail": "Relationship-led sourcing through industrial network",
                "owner_user_id": 2,
                "stage": "term_sheet",
                "status": "active",
                "priority": "critical",
                "priority_score": 88,
                "fund_fit": "Precise",
                "fund_fit_score": 20,
                "market_score": 14,
                "team_score": 13,
                "traction_score": 16,
                "round_name": "Series A Extension",
                "ticket_size_target": 7000000,
                "ticket_size_min": 6000000,
                "ticket_size_max": 8000000,
                "valuation_min": 60000000,
                "valuation_max": 75000000,
                "ownership_target_pct": 11.0,
                "next_step": "Close confirmatory diligence and align investment committee final sign-off",
                "next_step_due_at": (utc_now() + timedelta(days=2)).date().isoformat(),
                "last_contacted_at": utc_now().date().isoformat(),
                "decision_due_at": (utc_now() + timedelta(days=2)).date().isoformat(),
                "investment_thesis": "Strong precision fit with strategic channel support and recurring-service upside.",
                "key_concerns": "Confirm service revenue quality and final legal review.",
                "relationship_notes": "Well-supported through network references.",
                "nda_required": 1,
                "nda_status": "signed",
                "workflow_flags": ["ready_for_review", "decision_needed_this_week"],
                "risk_flags": ["legal_risk"],
                "tags": ["precision", "climate", "term-sheet"],
                "created_by_user_id": 2,
                "updated_by_user_id": 2,
                "age_days": 20,
            },
        ]

        for seed in opportunities:
            created_at = (utc_now() - timedelta(days=seed.pop("age_days"))).replace(microsecond=0)
            created_iso = created_at.isoformat().replace("+00:00", "Z")
            stage_entered = (created_at + timedelta(days=2)).isoformat().replace("+00:00", "Z")
            company_id = company_ids[seed.pop("company_name")]
            cursor = conn.execute(
                """
                INSERT INTO opportunities(
                    deal_code, company_id, source_type, source_detail, owner_user_id, stage, status, priority,
                    priority_score, fund_fit, fund_fit_score, market_score, team_score, traction_score, currency,
                    round_name, ticket_size_target, ticket_size_min, ticket_size_max, valuation_min, valuation_max,
                    ownership_target_pct, next_step, next_step_due_at, last_contacted_at, decision_due_at,
                    investment_thesis, key_concerns, relationship_notes, nda_required, nda_status,
                    workflow_flags, risk_flags, missing_fields, tags, last_activity_at, stage_entered_at,
                    created_by_user_id, updated_by_user_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'EUR', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    seed["deal_code"],
                    company_id,
                    seed["source_type"],
                    seed["source_detail"],
                    seed["owner_user_id"],
                    seed["stage"],
                    seed["status"],
                    seed["priority"],
                    seed["priority_score"],
                    seed["fund_fit"],
                    seed["fund_fit_score"],
                    seed["market_score"],
                    seed["team_score"],
                    seed["traction_score"],
                    seed["round_name"],
                    seed["ticket_size_target"],
                    seed["ticket_size_min"],
                    seed["ticket_size_max"],
                    seed["valuation_min"],
                    seed["valuation_max"],
                    seed["ownership_target_pct"],
                    seed["next_step"],
                    seed["next_step_due_at"],
                    seed["last_contacted_at"],
                    seed["decision_due_at"],
                    seed["investment_thesis"],
                    seed["key_concerns"],
                    seed["relationship_notes"],
                    seed["nda_required"],
                    seed["nda_status"],
                    to_json(seed["workflow_flags"]),
                    to_json(seed["risk_flags"]),
                    "[]",
                    to_json(seed["tags"]),
                    created_iso,
                    stage_entered,
                    seed["created_by_user_id"],
                    seed["updated_by_user_id"],
                    created_iso,
                    created_iso,
                ),
            )
            opportunity_id = cursor.lastrowid
            self._seed_related(conn, opportunity_id, seed["stage"], seed["owner_user_id"], created_iso)
            detail = self.get_opportunity(opportunity_id, conn=conn)
            conn.execute(
                "UPDATE opportunities SET missing_fields = ? WHERE id = ?",
                (to_json(detail["missing_fields"]), opportunity_id),
            )

    def _seed_related(self, conn, opportunity_id: int, stage: str, owner_user_id: int, created_iso: str) -> None:
        updates = {
            1: [
                ("general", "Initial investment read captured and ready for partner review."),
                ("meeting", "Reviewed external materials and highlighted concentration questions."),
            ],
            2: [
                ("general", "Need cleaner ownership math before this deserves Qualified."),
            ],
            3: [
                ("research", "Competition analysis is promising, but timing around the June round is still tricky."),
            ],
            4: [
                ("concern", "Blocked until NDA signature and key questions are finalized."),
            ],
            5: [
                ("research", "Pavel needs more than the deck to keep work moving."),
            ],
            6: [
                ("general", "Partner intake is good, but financials and proper contacts are still missing."),
            ],
            7: [
                ("decision_prep", "Strong candidate for weekly review. IC memo draft in progress."),
            ],
            8: [
                ("decision_prep", "Confirmatory diligence and legal review remain before final close."),
            ],
        }
        tasks = {
            1: [
                ("Confirm concentration and round lead", "todo", "high", 2, 0),
            ],
            2: [
                ("Fill ownership target and valuation context", "in_progress", "high", 3, 0),
            ],
            3: [
                ("Finish competition analysis", "todo", "high", 3, 0),
            ],
            4: [
                ("Prepare NDA questions", "blocked", "high", 2, 1),
            ],
            5: [
                ("Get independent technical diligence inputs", "todo", "medium", 2, 0),
            ],
            6: [
                ("Request missing financial package", "todo", "urgent", 1, 1),
            ],
            7: [
                ("Write IC memo", "in_progress", "urgent", 3, 1),
            ],
            8: [
                ("Close final diligence items", "in_progress", "urgent", 2, 1),
            ],
        }
        decisions = {
            7: [("advance", "Move into IC Review", "Strong growth profile and management wants a decision this week.")],
            8: [("term_sheet", "Proceed to term sheet", "Precision fit and channel support justify diligence sprint.")],
        }

        for note_type, body in updates.get(opportunity_id, []):
            conn.execute(
                """
                INSERT INTO notes(opportunity_id, author_user_id, note_type, body, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (opportunity_id, owner_user_id, note_type, body, created_iso, created_iso),
            )

        for title, status, priority, assignee_id, is_blocking in tasks.get(opportunity_id, []):
            due = (parse_iso(created_iso) + timedelta(days=4)).date().isoformat()
            task_id = conn.execute(
                """
                INSERT INTO tasks(
                    opportunity_id, title, description, status, priority, assigned_user_id,
                    created_by_user_id, due_at, is_blocking, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    opportunity_id,
                    title,
                    "Seeded task from opportunity workflow",
                    status,
                    priority,
                    assignee_id,
                    owner_user_id,
                    due,
                    is_blocking,
                    created_iso,
                    created_iso,
                ),
            ).lastrowid
            if status in {"in_progress", "blocked"}:
                conn.execute(
                    """
                    INSERT INTO task_comments(task_id, user_id, body, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (task_id, assignee_id, "Need follow-up before we can clear this item.", created_iso),
                )

        for decision_type, summary, rationale in decisions.get(opportunity_id, []):
            conn.execute(
                """
                INSERT INTO decision_logs(opportunity_id, decision_type, decision_summary, rationale, decided_by_user_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (opportunity_id, decision_type, summary, rationale, 1, created_iso),
            )

        conn.execute(
            """
            INSERT INTO stage_history(opportunity_id, from_stage, to_stage, changed_by_user_id, reason, created_at)
            VALUES (?, NULL, ?, ?, ?, ?)
            """,
            (opportunity_id, stage, owner_user_id, "Seeded initial stage", created_iso),
        )

        self._log_activity(
            conn,
            opportunity_id=opportunity_id,
            task_id=None,
            user_id=owner_user_id,
            activity_type="opportunity_created",
            entity_type="opportunity",
            entity_id=opportunity_id,
            summary="Opportunity added to the pipeline.",
            metadata={"seeded": True},
            created_at=created_iso,
        )

    def _cleanup_seed_users(self, conn: sqlite3.Connection) -> None:
        now = iso_now()
        conn.execute(
            """
            UPDATE users
            SET email = ?, updated_at = ?
            WHERE name = ? AND email = ?
            """,
            ( "marc.debois@fo-next.com", now, "Marc Debois", "marc@fo-next.com"),
        )
        conn.execute(
            """
            UPDATE users
            SET is_active = 0, updated_at = ?
            WHERE is_active = 1 AND (email = ? OR name = ?)
            """,
            (now, "jamie@vcc.local", "Jamie VCC"),
        )

    def _backfill_operating_system_entities(self, conn) -> None:
        org_count = conn.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
        if org_count == 0:
            rows = conn.execute(
                """
                SELECT c.*, o.owner_user_id
                FROM companies c
                LEFT JOIN opportunities o ON o.company_id = c.id
                GROUP BY c.id
                ORDER BY c.id
                """
            ).fetchall()
            for row in rows:
                item = self._row(row)
                now = item.get("updated_at") or item.get("created_at") or iso_now()
                conn.execute(
                    """
                    INSERT INTO organizations(
                        legacy_company_id, organization_type, name, website, sector_primary, subsector,
                        geography, hq_city, hq_country, business_model, description, owner_user_id,
                        relationship_status, created_at, updated_at
                    ) VALUES (?, 'company', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
                    """,
                    (
                        item["id"],
                        item["name"],
                        item.get("website"),
                        item.get("sector"),
                        item.get("subsector"),
                        item.get("geography"),
                        item.get("hq_city"),
                        item.get("hq_country"),
                        item.get("business_model"),
                        item.get("description"),
                        item.get("owner_user_id"),
                        item.get("created_at") or now,
                        now,
                    ),
                )

        record_count = conn.execute("SELECT COUNT(*) FROM pipeline_records").fetchone()[0]
        if record_count == 0:
            rows = conn.execute(
                """
                SELECT o.*, org.id AS organization_id
                FROM opportunities o
                JOIN organizations org ON org.legacy_company_id = o.company_id
                WHERE o.deleted_at IS NULL
                ORDER BY o.id
                """
            ).fetchall()
            for row in rows:
                item = self._row(row)
                conn.execute(
                    """
                    INSERT INTO pipeline_records(
                        organization_id, legacy_opportunity_id, record_code, stage, status, fund_fit,
                        source_type, source_detail, priority, owner_user_id, next_step, next_step_due_at,
                        investment_thesis, key_concerns, decision_outcome, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["organization_id"],
                        item["id"],
                        item["deal_code"],
                        self._map_legacy_stage_to_pipeline_record_stage(item.get("stage")),
                        self._map_legacy_status_to_pipeline_record_status(item.get("status")),
                        item.get("fund_fit"),
                        item.get("source_type"),
                        item.get("source_detail"),
                        item.get("priority") or "medium",
                        item.get("owner_user_id"),
                        item.get("next_step"),
                        item.get("next_step_due_at"),
                        item.get("investment_thesis"),
                        item.get("key_concerns"),
                        self._map_legacy_decision_outcome(item.get("stage"), item.get("status")),
                        item.get("created_at") or iso_now(),
                        item.get("updated_at") or item.get("created_at") or iso_now(),
                    ),
                )

    def _row(self, row: sqlite3.Row) -> dict:
        data = dict(row)
        json_defaults = {
            "workflow_flags": [],
            "risk_flags": [],
            "missing_fields": [],
            "tags": [],
            "metadata_json": {},
            "raw_payload": {},
        }
        for key, default in json_defaults.items():
            if key in data:
                data[key] = from_json(data[key], default)
        return data

    def _error(self, message: str, fields: Optional[Dict[str, str]] = None):
        raise ValidationError(message, fields)

    def _list_user_ids(self, conn) -> set:
        return {row["id"] for row in conn.execute("SELECT id FROM users WHERE is_active = 1")}

    def _validate_stage(self, stage: str) -> None:
        if stage not in STAGE_LABELS:
            self._error("Invalid stage", {"stage": "Unknown stage"})

    def _validate_priority(self, priority: str) -> None:
        if priority not in PRIORITIES:
            self._error("Invalid priority", {"priority": "Unknown priority"})

    def _validate_task_priority(self, priority: str) -> None:
        if priority not in TASK_PRIORITIES:
            self._error("Invalid task priority", {"priority": "Unknown task priority"})

    def _validate_task_status(self, status: str) -> None:
        if status not in TASK_STATUSES:
            self._error("Invalid task status", {"status": "Unknown task status"})

    def _validate_opportunity_status(self, status: str) -> None:
        if status not in OPPORTUNITY_STATUSES:
            self._error("Invalid opportunity status", {"status": "Unknown opportunity status"})

    def _validate_note_type(self, note_type: str) -> None:
        if note_type not in NOTE_TYPES:
            self._error("Invalid note type", {"note_type": "Unknown note type"})

    def _validate_intake_status(self, status: str) -> None:
        if status not in INTAKE_STATUSES:
            self._error("Invalid intake status", {"status": "Unknown intake status"})

    def _validate_intake_outcome(self, outcome: str) -> None:
        if outcome not in INTAKE_OUTCOMES:
            self._error("Invalid intake outcome", {"outcome": "Unknown intake outcome"})

    def _validate_relationship_link_type(self, link_type: str) -> None:
        if link_type not in RELATIONSHIP_LINK_TYPES:
            self._error("Invalid relationship link type", {"link_type": "Unknown relationship link type"})

    def _validate_relationship_status(self, status: str) -> None:
        if status not in RELATIONSHIP_STATUSES:
            self._error("Invalid relationship status", {"relationship_status": "Unknown relationship status"})

    def _validate_organization_type(self, organization_type: str) -> None:
        if organization_type not in ORGANIZATION_TYPES:
            self._error("Invalid organization type", {"organization_type": "Unknown organization type"})

    def _validate_pipeline_record_stage(self, stage: str) -> None:
        if stage not in PIPELINE_RECORD_STAGES:
            self._error("Invalid pipeline record stage", {"stage": "Unknown pipeline record stage"})

    def _validate_pipeline_record_status(self, status: str) -> None:
        if status not in PIPELINE_RECORD_STATUSES:
            self._error("Invalid pipeline record status", {"status": "Unknown pipeline record status"})

    def _validate_decision_outcome(self, outcome: str) -> None:
        if outcome not in DECISION_OUTCOMES:
            self._error("Invalid decision outcome", {"decision_outcome": "Unknown decision outcome"})

    def _validate_optional_user_id(self, value, field_name: str, fields: Dict[str, str]) -> None:
        if value in ("", None):
            return
        try:
            int(value)
        except (TypeError, ValueError):
            fields[field_name] = "Must be a user id"

    def _map_legacy_stage_to_pipeline_record_stage(self, legacy_stage: Optional[str]) -> str:
        mapping = {
            "new": "triage",
            "screening": "screening",
            "qualified": "qualified",
            "ic_review": "diligence",
            "term_sheet": "closing",
            "closed_won": "invested",
            "closed_lost": "passed",
        }
        return mapping.get(legacy_stage or "", "triage")

    def _map_legacy_status_to_pipeline_record_status(self, legacy_status: Optional[str]) -> str:
        mapping = {
            "active": "active",
            "on_hold": "on_hold",
            "closed_won": "approved",
            "closed_lost": "passed",
        }
        return mapping.get(legacy_status or "", "active")

    def _map_legacy_decision_outcome(self, legacy_stage: Optional[str], legacy_status: Optional[str]) -> str:
        if legacy_stage == "closed_won" or legacy_status == "closed_won":
            return "approved"
        if legacy_stage == "closed_lost" or legacy_status == "closed_lost":
            return "declined"
        return "pending"

    def _sync_company_to_organization(self, conn, company_id: int, owner_user_id: Optional[int] = None) -> int:
        row = conn.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()
        if not row:
            raise NotFoundError("Company not found")
        company = self._row(row)
        existing = conn.execute(
            "SELECT id, organization_type, relationship_status, owner_user_id, archived_at FROM organizations WHERE legacy_company_id = ?",
            (company_id,),
        ).fetchone()
        now = company.get("updated_at") or company.get("created_at") or iso_now()
        resolved_owner_id = owner_user_id if owner_user_id not in ("", None) else (existing["owner_user_id"] if existing else None)
        if existing:
            conn.execute(
                """
                UPDATE organizations
                SET organization_type = ?, name = ?, website = ?, sector_primary = ?, subsector = ?,
                    geography = ?, hq_city = ?, hq_country = ?, business_model = ?, description = ?,
                    owner_user_id = ?, relationship_status = ?, updated_at = ?, archived_at = ?
                WHERE id = ?
                """,
                (
                    existing["organization_type"] or "company",
                    company.get("name") or "",
                    company.get("website") or "",
                    company.get("sector") or "",
                    company.get("subsector") or "",
                    company.get("geography") or "",
                    company.get("hq_city") or "",
                    company.get("hq_country") or "",
                    company.get("business_model") or "",
                    company.get("description") or "",
                    resolved_owner_id,
                    existing["relationship_status"] or "active",
                    now,
                    existing["archived_at"],
                    existing["id"],
                ),
            )
            return existing["id"]
        return conn.execute(
            """
            INSERT INTO organizations(
                legacy_company_id, organization_type, name, website, sector_primary, subsector,
                geography, hq_city, hq_country, business_model, description, owner_user_id,
                relationship_status, created_at, updated_at
            ) VALUES (?, 'company', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                company_id,
                company.get("name") or "",
                company.get("website") or "",
                company.get("sector") or "",
                company.get("subsector") or "",
                company.get("geography") or "",
                company.get("hq_city") or "",
                company.get("hq_country") or "",
                company.get("business_model") or "",
                company.get("description") or "",
                resolved_owner_id,
                company.get("created_at") or now,
                now,
            ),
        ).lastrowid

    def _sync_opportunity_to_pipeline_record(self, conn, opportunity_id: int) -> Optional[int]:
        row = conn.execute(
            """
            SELECT *
            FROM opportunities
            WHERE id = ?
            """,
            (opportunity_id,),
        ).fetchone()
        if not row:
            existing = conn.execute(
                "SELECT id FROM pipeline_records WHERE legacy_opportunity_id = ?",
                (opportunity_id,),
            ).fetchone()
            if existing:
                conn.execute("DELETE FROM pipeline_records WHERE id = ?", (existing["id"],))
                return existing["id"]
            return None
        opportunity = self._row(row)
        organization_id = self._sync_company_to_organization(
            conn,
            opportunity["company_id"],
            owner_user_id=opportunity.get("owner_user_id"),
        )
        self._sync_legacy_contacts_to_organization(conn, organization_id)
        existing = conn.execute(
            "SELECT id FROM pipeline_records WHERE legacy_opportunity_id = ?",
            (opportunity_id,),
        ).fetchone()
        stage = self._map_legacy_stage_to_pipeline_record_stage(opportunity.get("stage"))
        status = self._map_legacy_status_to_pipeline_record_status(opportunity.get("status"))
        decision_outcome = self._map_legacy_decision_outcome(opportunity.get("stage"), opportunity.get("status"))
        if opportunity.get("deleted_at"):
            stage = "passed"
            status = "passed"
            decision_outcome = "declined"
        now = opportunity.get("updated_at") or opportunity.get("created_at") or iso_now()
        values = (
            organization_id,
            opportunity.get("deal_code") or f"LEGACY-{opportunity_id}",
            stage,
            status,
            opportunity.get("fund_fit") or "",
            opportunity.get("source_type") or "",
            opportunity.get("source_detail") or "",
            opportunity.get("priority") or "medium",
            opportunity.get("owner_user_id"),
            opportunity.get("next_step") or "",
            opportunity.get("next_step_due_at"),
            opportunity.get("round_name") or "",
            opportunity.get("ticket_size_target"),
            opportunity.get("ownership_target_pct"),
            opportunity.get("valuation_min"),
            opportunity.get("valuation_max"),
            opportunity.get("annual_recurring_revenue"),
            opportunity.get("revenue_growth_pct"),
            opportunity.get("gross_margin_pct"),
            opportunity.get("ebitda_margin_pct"),
            opportunity.get("rule_of_40_pct"),
            opportunity.get("monthly_burn"),
            opportunity.get("cash_runway_months"),
            opportunity.get("financials_updated_at") or opportunity.get("last_financials_at"),
            opportunity.get("investment_thesis") or "",
            opportunity.get("key_concerns") or "",
            opportunity.get("relationship_notes") or "",
            1 if opportunity.get("nda_required") else 0,
            opportunity.get("nda_status") or "not_required",
            to_json(opportunity.get("risk_flags") or []),
            to_json(opportunity.get("tags") or []),
            opportunity.get("decision_due_at"),
            decision_outcome,
            now,
        )
        if existing:
            conn.execute(
                """
                UPDATE pipeline_records
                SET organization_id = ?, record_code = ?, stage = ?, status = ?, fund_fit = ?, source_type = ?,
                    source_detail = ?, priority = ?, owner_user_id = ?, next_step = ?, next_step_due_at = ?,
                    round_name = ?, ticket_size_target = ?, ownership_target_pct = ?, valuation_min = ?, valuation_max = ?,
                    annual_recurring_revenue = ?, revenue_growth_pct = ?, gross_margin_pct = ?, ebitda_margin_pct = ?,
                    rule_of_40_pct = ?, monthly_burn = ?, cash_runway_months = ?, financials_updated_at = ?,
                    investment_thesis = ?, key_concerns = ?, relationship_notes = ?, nda_required = ?, nda_status = ?,
                    risk_flags = ?, tags = ?, decision_due_at = ?, decision_outcome = ?, updated_at = ?
                WHERE id = ?
                """,
                (*values, existing["id"]),
            )
            return existing["id"]
        return conn.execute(
            """
            INSERT INTO pipeline_records(
                organization_id, legacy_opportunity_id, record_code, stage, status, fund_fit,
                source_type, source_detail, priority, owner_user_id, next_step, next_step_due_at,
                round_name, ticket_size_target, ownership_target_pct, valuation_min, valuation_max,
                annual_recurring_revenue, revenue_growth_pct, gross_margin_pct, ebitda_margin_pct,
                rule_of_40_pct, monthly_burn, cash_runway_months, financials_updated_at,
                investment_thesis, key_concerns, relationship_notes, nda_required, nda_status,
                risk_flags, tags, decision_due_at, decision_outcome, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                organization_id,
                opportunity_id,
                opportunity.get("deal_code") or f"LEGACY-{opportunity_id}",
                stage,
                status,
                opportunity.get("fund_fit") or "",
                opportunity.get("source_type") or "",
                opportunity.get("source_detail") or "",
                opportunity.get("priority") or "medium",
                opportunity.get("owner_user_id"),
                opportunity.get("next_step") or "",
                opportunity.get("next_step_due_at"),
                opportunity.get("round_name") or "",
                opportunity.get("ticket_size_target"),
                opportunity.get("ownership_target_pct"),
                opportunity.get("valuation_min"),
                opportunity.get("valuation_max"),
                opportunity.get("investment_thesis") or "",
                opportunity.get("key_concerns") or "",
                opportunity.get("relationship_notes") or "",
                1 if opportunity.get("nda_required") else 0,
                opportunity.get("nda_status") or "not_required",
                to_json(opportunity.get("risk_flags") or []),
                to_json(opportunity.get("tags") or []),
                opportunity.get("decision_due_at"),
                decision_outcome,
                opportunity.get("created_at") or now,
                now,
            ),
        ).lastrowid

    def _build_flags(self, opportunity: dict) -> List[str]:
        flags = []
        if opportunity["missing_fields"]:
            flags.append("missing_critical_fields")
        if opportunity.get("next_step_due_at"):
            due = parse_iso(opportunity["next_step_due_at"])
            if due and due.date() < utc_now().date() and opportunity["stage"] in ACTIVE_STAGES:
                flags.append("overdue_next_step")
        if opportunity["stage"] in DECISION_STAGES and not opportunity["recent_decision"]:
            flags.append("decision_needed_this_week")
        if opportunity["status"] == "on_hold":
            flags.append("waiting_on_external")
        return sorted(set(flags + opportunity.get("workflow_flags", [])))

    def _required_missing_fields(self, detail: dict) -> List[str]:
        required = REQUIRED_FIELDS_BY_STAGE.get(detail["stage"], [])
        missing = []
        for field in required:
            value = detail.get(field)
            if value is None or value == "" or value == []:
                missing.append(field)
        if detail["stage"] in {"screening", "qualified", "ic_review", "term_sheet"} and not detail.get("valuation_max"):
            missing.append("valuation_max")
        if detail["stage"] in {"screening", "qualified", "ic_review", "term_sheet"} and not detail.get("ticket_size_target"):
            missing.append("ticket_size_target")
        if detail.get("nda_required") and detail.get("nda_status") != "signed":
            missing.append("nda_status")
        if "missing_financials" in (detail.get("risk_flags") or []):
            missing.append("financial_package")
        return missing

    def _compute_priority_score(self, payload: dict) -> int:
        if payload.get("priority_score") is not None:
            return int(payload["priority_score"])
        base = PRIORITY_SCORES.get(payload.get("priority", "medium"), 50)
        base += int(payload.get("fund_fit_score") or 0)
        base += int(payload.get("traction_score") or 0) // 2
        return max(0, min(base, 100))

    def _fetch_primary_contact(self, conn, company_id: int) -> Optional[dict]:
        row = conn.execute(
            """
            SELECT * FROM contacts
            WHERE company_id = ?
            ORDER BY is_primary DESC, id ASC
            LIMIT 1
            """,
            (company_id,),
        ).fetchone()
        return self._row(row) if row else None

    def _base_opportunity(self, conn, opportunity_id: int) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT
                o.*,
                c.name AS company_name,
                c.website,
                c.sector,
                c.subsector,
                c.geography,
                c.hq_city,
                c.hq_country,
                c.business_model,
                c.description AS company_description,
                u.name AS owner_name,
                u.role AS owner_role
            FROM opportunities o
            JOIN companies c ON c.id = o.company_id
            LEFT JOIN users u ON u.id = o.owner_user_id
            WHERE o.id = ? AND o.deleted_at IS NULL
            """,
            (opportunity_id,),
        ).fetchone()
        if not row:
            raise NotFoundError("Opportunity not found")
        return row

    def _enrich_opportunity(self, conn, row: sqlite3.Row) -> dict:
        base = self._row(row)
        primary = self._fetch_primary_contact(conn, base["company_id"])
        base["primary_contact_name"] = primary["name"] if primary else ""
        base["primary_contact_email"] = primary["email"] if primary else ""
        base["primary_contact_phone"] = primary["phone"] if primary else ""
        base["primary_contact_title"] = primary["title"] if primary else ""
        base["stage_label"] = STAGE_LABELS.get(base["stage"], base["stage"])
        base["priority_label"] = PRIORITY_LABELS.get(base["priority"], "P3")
        base["open_tasks"] = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE opportunity_id = ? AND status NOT IN ('done', 'canceled')",
            (base["id"],),
        ).fetchone()[0]
        base["overdue_tasks"] = conn.execute(
            """
            SELECT COUNT(*) FROM tasks
            WHERE opportunity_id = ? AND status NOT IN ('done', 'canceled')
              AND due_at IS NOT NULL AND date(due_at) < date('now')
            """,
            (base["id"],),
        ).fetchone()[0]
        recent_decision = conn.execute(
            """
            SELECT * FROM decision_logs
            WHERE opportunity_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (base["id"],),
        ).fetchone()
        base["recent_decision"] = self._row(recent_decision) if recent_decision else None
        base["missing_fields"] = self._required_missing_fields(base)
        base["workflow_flags"] = self._build_flags(base)
        base["data_completeness"] = round(
            100 * (1 - (len(base["missing_fields"]) / max(1, len(REQUIRED_FIELDS_BY_STAGE.get(base["stage"], []) or [1])))),
            0,
        )
        return base

    def _base_organization(self, conn, organization_id: int) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT
                org.*,
                u.name AS owner_name
            FROM organizations org
            LEFT JOIN users u ON u.id = org.owner_user_id
            WHERE org.id = ?
            """,
            (organization_id,),
        ).fetchone()
        if not row:
            raise NotFoundError("Organization not found")
        return row

    def _enrich_organization(self, conn, row: sqlite3.Row) -> dict:
        base = self._row(row)
        counts = conn.execute(
            """
            SELECT
                COUNT(*) AS pipeline_record_count,
                SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_pipeline_record_count
            FROM pipeline_records
            WHERE organization_id = ?
            """,
            (base["id"],),
        ).fetchone()
        latest_record = conn.execute(
            """
            SELECT
                id,
                record_code,
                stage,
                status,
                priority,
                owner_user_id,
                updated_at
            FROM pipeline_records
            WHERE organization_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (base["id"],),
        ).fetchone()
        base["pipeline_record_count"] = counts["pipeline_record_count"] if counts else 0
        base["active_pipeline_record_count"] = counts["active_pipeline_record_count"] if counts and counts["active_pipeline_record_count"] is not None else 0
        base["latest_pipeline_record"] = self._row(latest_record) if latest_record else None
        base["primary_contact"] = self._fetch_primary_organization_contact(conn, base["id"])
        base["legacy_company"] = None
        if base.get("legacy_company_id"):
            legacy_company = conn.execute(
                """
                SELECT
                    c.id,
                    c.name,
                    c.website,
                    c.sector,
                    c.subsector,
                    c.geography,
                    c.hq_city,
                    c.hq_country,
                    c.business_model,
                    c.description
                FROM companies c
                WHERE c.id = ?
                """,
                (base["legacy_company_id"],),
            ).fetchone()
            if legacy_company:
                base["legacy_company"] = self._row(legacy_company)
                if not base["primary_contact"]:
                    base["primary_contact"] = self._fetch_primary_contact(conn, base["legacy_company_id"])
        return base

    def _base_pipeline_record(self, conn, pipeline_record_id: int) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT
                pr.*,
                org.name AS organization_name,
                org.organization_type,
                org.relationship_status AS organization_relationship_status,
                org.legacy_company_id,
                u.name AS owner_name
            FROM pipeline_records pr
            JOIN organizations org ON org.id = pr.organization_id
            LEFT JOIN users u ON u.id = pr.owner_user_id
            WHERE pr.id = ?
            """,
            (pipeline_record_id,),
        ).fetchone()
        if not row:
            raise NotFoundError("Pipeline record not found")
        return row

    def _enrich_pipeline_record(self, conn, row: sqlite3.Row) -> dict:
        base = self._row(row)
        base["organization"] = {
            "id": base["organization_id"],
            "name": base.get("organization_name"),
            "organization_type": base.get("organization_type"),
            "relationship_status": base.get("organization_relationship_status"),
            "legacy_company_id": base.get("legacy_company_id"),
        }
        base["primary_contact"] = self._fetch_primary_organization_contact(conn, base["organization_id"])
        if not base["primary_contact"] and base.get("legacy_company_id"):
            base["primary_contact"] = self._fetch_primary_contact(conn, base["legacy_company_id"])
        base["legacy_opportunity"] = None
        base["open_task_count"] = 0
        base["overdue_task_count"] = 0
        if base.get("legacy_opportunity_id"):
            legacy = conn.execute(
                """
                SELECT
                    o.id,
                    o.deal_code,
                    o.stage,
                    o.status,
                    o.priority,
                    o.round_name,
                    o.ticket_size_target,
                    o.decision_due_at,
                    o.last_activity_at,
                    c.name AS company_name
                FROM opportunities o
                JOIN companies c ON c.id = o.company_id
                WHERE o.id = ? AND o.deleted_at IS NULL
                """,
                (base["legacy_opportunity_id"],),
            ).fetchone()
            if legacy:
                base["legacy_opportunity"] = self._row(legacy)
                base["open_task_count"] = conn.execute(
                    "SELECT COUNT(*) FROM tasks WHERE opportunity_id = ? AND status NOT IN ('done', 'canceled')",
                    (base["legacy_opportunity_id"],),
                ).fetchone()[0]
                base["overdue_task_count"] = conn.execute(
                    """
                    SELECT COUNT(*) FROM tasks
                    WHERE opportunity_id = ? AND status NOT IN ('done', 'canceled')
                      AND due_at IS NOT NULL AND date(due_at) < date('now')
                    """,
                    (base["legacy_opportunity_id"],),
                ).fetchone()[0]
        else:
            base["open_task_count"] = conn.execute(
                "SELECT COUNT(*) FROM pipeline_record_tasks WHERE pipeline_record_id = ? AND status NOT IN ('done', 'canceled')",
                (base["id"],),
            ).fetchone()[0]
            base["overdue_task_count"] = conn.execute(
                """
                SELECT COUNT(*) FROM pipeline_record_tasks
                WHERE pipeline_record_id = ? AND status NOT IN ('done', 'canceled')
                  AND due_at IS NOT NULL AND date(due_at) < date('now')
                """,
                (base["id"],),
            ).fetchone()[0]
        primary = base.get("primary_contact") or {}
        base["primary_contact_name"] = primary.get("name") or ""
        base["primary_contact_title"] = primary.get("title") or ""
        base["primary_contact_email"] = primary.get("email") or ""
        base["primary_contact_phone"] = primary.get("phone") or ""
        return base

    def list_users(self) -> List[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, name, email, role FROM users WHERE is_active = 1 ORDER BY name"
            ).fetchall()
            return [self._row(row) for row in rows]

    def get_bootstrap(self) -> dict:
        return {
            "users": self.list_users(),
            "stages": STAGES,
            "priorities": PRIORITIES,
            "opportunity_statuses": OPPORTUNITY_STATUSES,
            "task_priorities": TASK_PRIORITIES,
            "task_statuses": TASK_STATUSES,
            "note_types": NOTE_TYPES,
            "intake_statuses": INTAKE_STATUSES,
            "intake_outcomes": INTAKE_OUTCOMES,
            "relationship_link_types": RELATIONSHIP_LINK_TYPES,
            "relationship_statuses": RELATIONSHIP_STATUSES,
            "organization_types": ORGANIZATION_TYPES,
            "pipeline_record_stages": PIPELINE_RECORD_STAGES,
            "pipeline_record_statuses": PIPELINE_RECORD_STATUSES,
            "decision_outcomes": DECISION_OUTCOMES,
            "current_user_id": 2,
        }

    def list_organizations(self, filters: Optional[dict] = None) -> List[dict]:
        filters = filters or {}
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    org.*,
                    u.name AS owner_name
                FROM organizations org
                LEFT JOIN users u ON u.id = org.owner_user_id
                ORDER BY org.updated_at DESC, org.name ASC
                """
            ).fetchall()
            items = [self._enrich_organization(conn, row) for row in rows]
            search = (filters.get("q") or "").strip().lower()
            if search:
                items = [
                    item for item in items
                    if search in (item.get("name") or "").lower()
                    or search in (item.get("sector_primary") or "").lower()
                    or search in (item.get("website") or "").lower()
                ]
            for key in ("organization_type", "owner_user_id", "relationship_status"):
                value = filters.get(key)
                if value and value not in {"all", ""}:
                    items = [item for item in items if str(item.get(key)) == str(value)]
            return items

    def get_organization(self, organization_id: int, conn=None) -> dict:
        if conn is not None:
            return self._enrich_organization(conn, self._base_organization(conn, organization_id))
        with self.connect() as own_conn:
            return self._enrich_organization(own_conn, self._base_organization(own_conn, organization_id))

    def create_organization(self, payload: dict) -> dict:
        fields = {}
        name = (payload.get("name") or "").strip()
        if not name:
            fields["name"] = "This field is required"
        organization_type = payload.get("organization_type") or "company"
        relationship_status = payload.get("relationship_status") or "active"
        self._validate_organization_type(organization_type)
        self._validate_relationship_status(relationship_status)
        self._validate_optional_user_id(payload.get("owner_user_id"), "owner_user_id", fields)
        if fields:
            self._error("Invalid organization payload", fields)
        now = iso_now()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO organizations(
                    organization_type, name, website, sector_primary, subsector, geography,
                    hq_city, hq_country, business_model, description, owner_user_id,
                    relationship_status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    organization_type,
                    name,
                    (payload.get("website") or "").strip(),
                    (payload.get("sector_primary") or "").strip(),
                    (payload.get("subsector") or "").strip(),
                    (payload.get("geography") or "").strip(),
                    (payload.get("hq_city") or "").strip(),
                    (payload.get("hq_country") or "").strip(),
                    (payload.get("business_model") or "").strip(),
                    (payload.get("description") or "").strip(),
                    int(payload["owner_user_id"]) if payload.get("owner_user_id") not in ("", None) else None,
                    relationship_status,
                    now,
                    now,
                ),
            )
            organization_id = cursor.lastrowid
            self._upsert_primary_organization_contact(conn, organization_id, payload)
            detail = self.get_organization(organization_id, conn=conn)
            actor_id = int(payload.get("actor_user_id") or payload.get("owner_user_id") or 2)
            self._audit(conn, "organization", organization_id, "create", actor_id, None, detail)
            return detail

    def update_organization(self, organization_id: int, payload: dict) -> dict:
        fields = {}
        if "name" in payload and not (payload.get("name") or "").strip():
            fields["name"] = "This field is required"
        if "organization_type" in payload:
            self._validate_organization_type(payload.get("organization_type") or "company")
        if "relationship_status" in payload:
            self._validate_relationship_status(payload.get("relationship_status") or "active")
        self._validate_optional_user_id(payload.get("owner_user_id"), "owner_user_id", fields)
        if fields:
            self._error("Invalid organization payload", fields)
        with self.connect() as conn:
            before = self.get_organization(organization_id, conn=conn)
            actor_id = int(payload.get("actor_user_id") or before.get("owner_user_id") or 2)
            conn.execute(
                """
                UPDATE organizations
                SET organization_type = ?, name = ?, website = ?, sector_primary = ?, subsector = ?, geography = ?,
                    hq_city = ?, hq_country = ?, business_model = ?, description = ?, owner_user_id = ?,
                    relationship_status = ?, updated_at = ?, archived_at = ?
                WHERE id = ?
                """,
                (
                    payload.get("organization_type", before.get("organization_type")),
                    (payload.get("name", before.get("name")) or "").strip(),
                    (payload.get("website", before.get("website")) or "").strip(),
                    (payload.get("sector_primary", before.get("sector_primary")) or "").strip(),
                    (payload.get("subsector", before.get("subsector")) or "").strip(),
                    (payload.get("geography", before.get("geography")) or "").strip(),
                    (payload.get("hq_city", before.get("hq_city")) or "").strip(),
                    (payload.get("hq_country", before.get("hq_country")) or "").strip(),
                    (payload.get("business_model", before.get("business_model")) or "").strip(),
                    (payload.get("description", before.get("description")) or "").strip(),
                    int(payload["owner_user_id"]) if payload.get("owner_user_id") not in ("", None) else before.get("owner_user_id"),
                    payload.get("relationship_status", before.get("relationship_status")),
                    iso_now(),
                    payload.get("archived_at", before.get("archived_at")),
                    organization_id,
                ),
            )
            self._upsert_primary_organization_contact(conn, organization_id, payload)
            after = self.get_organization(organization_id, conn=conn)
            self._audit(conn, "organization", organization_id, "update", actor_id, before, after)
            return after

    def list_pipeline_records(self, filters: Optional[dict] = None) -> List[dict]:
        filters = filters or {}
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    pr.*,
                    org.name AS organization_name,
                    org.organization_type,
                    org.relationship_status AS organization_relationship_status,
                    org.legacy_company_id,
                    u.name AS owner_name
                FROM pipeline_records pr
                JOIN organizations org ON org.id = pr.organization_id
                LEFT JOIN users u ON u.id = pr.owner_user_id
                ORDER BY pr.updated_at DESC
                """
            ).fetchall()
            items = [self._enrich_pipeline_record(conn, row) for row in rows]
            search = (filters.get("q") or "").strip().lower()
            if search:
                items = [
                    item for item in items
                    if search in (item.get("organization_name") or "").lower()
                    or search in (item.get("record_code") or "").lower()
                    or search in (item.get("source_detail") or "").lower()
                    or search in (item.get("investment_thesis") or "").lower()
                ]
            for key in ("stage", "status", "owner_user_id", "fund_fit", "priority"):
                value = filters.get(key)
                if value and value not in {"all", ""}:
                    items = [item for item in items if str(item.get(key)) == str(value)]
            return items

    def get_pipeline_record(self, pipeline_record_id: int, conn=None) -> dict:
        if conn is not None:
            return self._enrich_pipeline_record(conn, self._base_pipeline_record(conn, pipeline_record_id))
        with self.connect() as own_conn:
            return self._enrich_pipeline_record(own_conn, self._base_pipeline_record(own_conn, pipeline_record_id))

    def _fetch_pipeline_record_task_comments(self, conn, pipeline_record_task_id: int) -> List[dict]:
        return [
            self._row(row)
            for row in conn.execute(
                """
                SELECT prtc.*, u.name AS user_name
                FROM pipeline_record_task_comments prtc
                LEFT JOIN users u ON u.id = prtc.user_id
                WHERE prtc.pipeline_record_task_id = ?
                ORDER BY prtc.created_at ASC
                """,
                (pipeline_record_task_id,),
            ).fetchall()
        ]

    def _pipeline_record_task_detail(self, conn, pipeline_record_task_id: int) -> dict:
        row = conn.execute(
            """
            SELECT
                prt.*,
                assignee.name AS assignee_name,
                creator.name AS created_by_name
            FROM pipeline_record_tasks prt
            LEFT JOIN users assignee ON assignee.id = prt.assigned_user_id
            LEFT JOIN users creator ON creator.id = prt.created_by_user_id
            WHERE prt.id = ?
            """,
            (pipeline_record_task_id,),
        ).fetchone()
        if not row:
            raise NotFoundError("Pipeline record task not found")
        task = self._row(row)
        task["comments"] = self._fetch_pipeline_record_task_comments(conn, pipeline_record_task_id)
        task["latest_comment"] = task["comments"][-1]["body"] if task["comments"] else ""
        task["pipeline_record_id"] = task["pipeline_record_id"]
        task["legacy_opportunity_id"] = None
        task["is_overdue"] = bool(
            task.get("due_at")
            and task["status"] not in {"done", "canceled"}
            and parse_iso(task["due_at"])
            and parse_iso(task["due_at"]).date() < utc_now().date()
        )
        return task

    def _pipeline_record_audit_events(self, conn, pipeline_record_id: int) -> List[dict]:
        return [
            self._row(row)
            for row in conn.execute(
                """
                SELECT ae.*, u.name AS actor_name
                FROM audit_events ae
                LEFT JOIN users u ON u.id = ae.actor_user_id
                WHERE entity_type = 'pipeline_record' AND entity_id = ?
                ORDER BY created_at DESC
                LIMIT 100
                """,
                (pipeline_record_id,),
            ).fetchall()
        ]

    def _pipeline_record_stage_label(self, stage: str) -> str:
        return (stage or "").replace("_", " ").title()

    def _pipeline_record_status_for_stage(self, stage: str, current_status: Optional[str] = None) -> str:
        if stage == "invested":
            return "approved"
        if stage == "passed":
            return "passed"
        if stage == "archived":
            return "archived"
        return current_status if current_status in {"on_hold", "waiting_external"} else "active"

    def _pipeline_record_decision_for_stage(self, stage: str, current_outcome: Optional[str] = None) -> str:
        if stage == "invested":
            return "approved"
        if stage == "passed":
            return "declined"
        return current_outcome or "pending"

    def _log_pipeline_record_activity(
        self,
        conn,
        pipeline_record_id: int,
        task_id: Optional[int],
        user_id: Optional[int],
        activity_type: str,
        entity_type: str,
        entity_id: Optional[int],
        summary: str,
        metadata: Optional[dict] = None,
        created_at: Optional[str] = None,
    ) -> None:
        created_at = created_at or iso_now()
        conn.execute(
            """
            INSERT INTO pipeline_record_activities(
                pipeline_record_id, task_id, user_id, activity_type, entity_type, entity_id,
                summary, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pipeline_record_id,
                task_id,
                user_id,
                activity_type,
                entity_type,
                entity_id,
                summary,
                json.dumps(metadata or {}),
                created_at,
            ),
        )
        conn.execute(
            "UPDATE pipeline_records SET updated_at = ? WHERE id = ?",
            (created_at, pipeline_record_id),
        )

    def _native_pipeline_record_notes(self, conn, pipeline_record_id: int) -> List[dict]:
        return [
            self._row(row)
            for row in conn.execute(
                """
                SELECT prn.*, u.name AS author_name
                FROM pipeline_record_notes prn
                LEFT JOIN users u ON u.id = prn.author_user_id
                WHERE prn.pipeline_record_id = ?
                ORDER BY prn.is_pinned DESC, prn.created_at DESC
                """,
                (pipeline_record_id,),
            ).fetchall()
        ]

    def _native_pipeline_record_tasks(self, conn, pipeline_record_id: int) -> List[dict]:
        task_ids = [
            row["id"]
            for row in conn.execute(
                """
                SELECT prt.id
                FROM pipeline_record_tasks prt
                WHERE prt.pipeline_record_id = ?
                ORDER BY
                    CASE prt.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                    CASE prt.status WHEN 'blocked' THEN 0 WHEN 'in_progress' THEN 1 WHEN 'todo' THEN 2 ELSE 3 END,
                    prt.due_at ASC
                """,
                (pipeline_record_id,),
            ).fetchall()
        ]
        return [self._pipeline_record_task_detail(conn, task_id) for task_id in task_ids]

    def _native_pipeline_record_documents(self, conn, pipeline_record_id: int) -> List[dict]:
        return [
            self._row(row)
            for row in conn.execute(
                """
                SELECT prd.*, u.name AS uploader_name
                FROM pipeline_record_documents prd
                LEFT JOIN users u ON u.id = prd.uploaded_by_user_id
                WHERE prd.pipeline_record_id = ?
                ORDER BY prd.created_at DESC
                """,
                (pipeline_record_id,),
            ).fetchall()
        ]

    def _native_pipeline_record_decisions(self, conn, pipeline_record_id: int) -> List[dict]:
        return [
            self._row(row)
            for row in conn.execute(
                """
                SELECT prd.*, u.name AS decided_by_name
                FROM pipeline_record_decisions prd
                LEFT JOIN users u ON u.id = prd.decided_by_user_id
                WHERE prd.pipeline_record_id = ?
                ORDER BY prd.created_at DESC
                """,
                (pipeline_record_id,),
            ).fetchall()
        ]

    def _native_pipeline_record_stage_history(self, conn, pipeline_record_id: int) -> List[dict]:
        items = []
        for row in conn.execute(
                """
                SELECT prsh.*, u.name AS changed_by_name
                FROM pipeline_record_stage_history prsh
                LEFT JOIN users u ON u.id = prsh.changed_by_user_id
                WHERE prsh.pipeline_record_id = ?
                ORDER BY prsh.created_at DESC
                """,
                (pipeline_record_id,),
            ).fetchall():
            item = self._row(row)
            item["pipeline_record_id"] = pipeline_record_id
            item["legacy_opportunity_id"] = None
            items.append(item)
        return items

    def _native_pipeline_record_activities(self, conn, pipeline_record_id: int) -> List[dict]:
        items = []
        for row in conn.execute(
            """
            SELECT pra.*, u.name AS user_name
            FROM pipeline_record_activities pra
            LEFT JOIN users u ON u.id = pra.user_id
            WHERE pra.pipeline_record_id = ?
            ORDER BY pra.created_at DESC
            """,
            (pipeline_record_id,),
        ).fetchall():
            item = self._row(row)
            item["metadata"] = item.get("metadata_json") or {}
            item["pipeline_record_id"] = pipeline_record_id
            item["legacy_opportunity_id"] = None
            items.append(item)
        return items

    def _native_pipeline_record_workflow(self, conn, pipeline_record_id: int) -> dict:
        pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
        notes = self._native_pipeline_record_notes(conn, pipeline_record_id)
        tasks = self._native_pipeline_record_tasks(conn, pipeline_record_id)
        documents = self._native_pipeline_record_documents(conn, pipeline_record_id)
        decisions = self._native_pipeline_record_decisions(conn, pipeline_record_id)
        for item in notes + documents + decisions:
            item["pipeline_record_id"] = pipeline_record_id
            item["legacy_opportunity_id"] = None
        return {
            "pipeline_record": pipeline_record,
            "legacy_opportunity_id": None,
            "opportunity": None,
            "notes": notes,
            "tasks": tasks,
            "documents": documents,
            "decision_logs": decisions,
            "stage_history": self._native_pipeline_record_stage_history(conn, pipeline_record_id),
            "activities": self._native_pipeline_record_activities(conn, pipeline_record_id),
            "audit_events": self._pipeline_record_audit_events(conn, pipeline_record_id),
        }

    def _resolve_pipeline_record_legacy_opportunity(self, conn, pipeline_record_id: int) -> tuple[dict, int]:
        pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
        legacy_opportunity_id = pipeline_record.get("legacy_opportunity_id")
        if not legacy_opportunity_id:
            self._error(
                "Pipeline record is not linked to a legacy opportunity yet",
                {"pipeline_record_id": "This workflow is only available for bridged pipeline records"},
            )
        self._base_opportunity(conn, int(legacy_opportunity_id))
        return pipeline_record, int(legacy_opportunity_id)

    def _augment_bridge_payload(self, payload: dict, pipeline_record: dict, legacy_opportunity_id: int) -> dict:
        if isinstance(payload, list):
            return [
                self._augment_bridge_payload(item, pipeline_record, legacy_opportunity_id)
                for item in payload
            ]
        if isinstance(payload, dict):
            payload = dict(payload)
            payload["pipeline_record_id"] = pipeline_record["id"]
            payload["legacy_opportunity_id"] = legacy_opportunity_id
        return payload

    def _augment_bridge_list(self, items: List[dict], pipeline_record: dict, legacy_opportunity_id: int) -> List[dict]:
        return [self._augment_bridge_payload(item, pipeline_record, legacy_opportunity_id) for item in items]

    def get_pipeline_record_workflow(self, pipeline_record_id: int) -> dict:
        with self.connect() as conn:
            pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            if not pipeline_record.get("legacy_opportunity_id"):
                return self._native_pipeline_record_workflow(conn, pipeline_record_id)
            _, legacy_opportunity_id = self._resolve_pipeline_record_legacy_opportunity(conn, pipeline_record_id)
            workflow = self.get_opportunity(legacy_opportunity_id, conn=conn)
            return {
                "pipeline_record": pipeline_record,
                "legacy_opportunity_id": legacy_opportunity_id,
                "opportunity": self._augment_bridge_payload(workflow, pipeline_record, legacy_opportunity_id),
                "notes": self._augment_bridge_list(workflow.get("notes", []), pipeline_record, legacy_opportunity_id),
                "tasks": self._augment_bridge_list(workflow.get("tasks", []), pipeline_record, legacy_opportunity_id),
                "documents": self._augment_bridge_list(workflow.get("documents", []), pipeline_record, legacy_opportunity_id),
                "decision_logs": self._augment_bridge_list(workflow.get("decision_logs", []), pipeline_record, legacy_opportunity_id),
                "stage_history": workflow.get("stage_history", []),
                "activities": workflow.get("activities", []),
                "audit_events": workflow.get("audit_events", []),
            }

    def list_pipeline_record_notes(self, pipeline_record_id: int) -> List[dict]:
        workflow = self.get_pipeline_record_workflow(pipeline_record_id)
        return workflow["notes"]

    def list_pipeline_record_tasks(self, pipeline_record_id: int, filters: Optional[dict] = None) -> List[dict]:
        filters = dict(filters or {})
        with self.connect() as conn:
            pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            if not pipeline_record.get("legacy_opportunity_id"):
                tasks = self._native_pipeline_record_tasks(conn, pipeline_record_id)
                if filters.get("status") and filters["status"] not in {"all", ""}:
                    tasks = [task for task in tasks if task.get("status") == filters["status"]]
                if filters.get("priority") and filters["priority"] not in {"all", ""}:
                    tasks = [task for task in tasks if task.get("priority") == filters["priority"]]
                if filters.get("assigned_user_id"):
                    tasks = [task for task in tasks if str(task.get("assigned_user_id")) == str(filters["assigned_user_id"])]
                if filters.get("overdue_only") == "1":
                    tasks = [task for task in tasks if task.get("is_overdue")]
                return tasks
            _, legacy_opportunity_id = self._resolve_pipeline_record_legacy_opportunity(conn, pipeline_record_id)
        filters["opportunity_id"] = legacy_opportunity_id
        tasks = self.list_tasks(filters)
        return [self._augment_bridge_payload(task, pipeline_record, legacy_opportunity_id) for task in tasks]

    def list_pipeline_record_documents(self, pipeline_record_id: int) -> List[dict]:
        workflow = self.get_pipeline_record_workflow(pipeline_record_id)
        return workflow["documents"]

    def list_pipeline_record_decisions(self, pipeline_record_id: int) -> List[dict]:
        workflow = self.get_pipeline_record_workflow(pipeline_record_id)
        return workflow["decision_logs"]

    def add_pipeline_record_note(self, pipeline_record_id: int, payload: dict) -> dict:
        with self.connect() as conn:
            pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            if not pipeline_record.get("legacy_opportunity_id"):
                note_type = payload.get("note_type", "general")
                self._validate_note_type(note_type)
                body = (payload.get("body") or "").strip()
                if not body:
                    self._error("Note body is required", {"body": "This field is required"})
                actor_id = int(payload.get("author_user_id") or 2)
                now = iso_now()
                note_id = conn.execute(
                    """
                    INSERT INTO pipeline_record_notes(
                        pipeline_record_id, author_user_id, note_type, body, is_pinned, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (pipeline_record_id, actor_id, note_type, body, 1 if payload.get("is_pinned") else 0, now, now),
                ).lastrowid
                row = conn.execute(
                    """
                    SELECT prn.*, u.name AS author_name
                    FROM pipeline_record_notes prn
                    LEFT JOIN users u ON u.id = prn.author_user_id
                    WHERE prn.id = ?
                    """,
                    (note_id,),
                ).fetchone()
                self._log_pipeline_record_activity(conn, pipeline_record_id, None, actor_id, "note_added", "pipeline_record_note", note_id, "Pipeline note added.")
                detail = self._row(row)
                detail["pipeline_record_id"] = pipeline_record_id
                detail["legacy_opportunity_id"] = None
                self._audit(conn, "pipeline_record_note", note_id, "create", actor_id, None, detail)
                return detail
            _, legacy_opportunity_id = self._resolve_pipeline_record_legacy_opportunity(conn, pipeline_record_id)
        note = self.add_note(legacy_opportunity_id, payload)
        return self._augment_bridge_payload(note, pipeline_record, legacy_opportunity_id)

    def create_pipeline_record_task(self, pipeline_record_id: int, payload: dict) -> dict:
        with self.connect() as conn:
            pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            if not pipeline_record.get("legacy_opportunity_id"):
                title = (payload.get("title") or "").strip()
                if not title:
                    self._error("Task title is required", {"title": "This field is required"})
                status = payload.get("status", "todo")
                priority = payload.get("priority", "medium")
                self._validate_task_status(status)
                self._validate_task_priority(priority)
                actor_id = int(payload.get("created_by_user_id") or 2)
                now = iso_now()
                task_id = conn.execute(
                    """
                    INSERT INTO pipeline_record_tasks(
                        pipeline_record_id, title, description, status, priority, assigned_user_id,
                        created_by_user_id, due_at, is_blocking, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pipeline_record_id,
                        title,
                        payload.get("description", ""),
                        status,
                        priority,
                        payload.get("assigned_user_id") or None,
                        actor_id,
                        payload.get("due_at") or None,
                        1 if payload.get("is_blocking") else 0,
                        now,
                        now,
                    ),
                ).lastrowid
                self._log_pipeline_record_activity(conn, pipeline_record_id, task_id, actor_id, "task_created", "pipeline_record_task", task_id, f"Task created: {title}")
                detail = self._pipeline_record_task_detail(conn, task_id)
                self._audit(conn, "pipeline_record_task", task_id, "create", actor_id, None, detail)
                return detail
            _, legacy_opportunity_id = self._resolve_pipeline_record_legacy_opportunity(conn, pipeline_record_id)
        task = self.create_task({**payload, "opportunity_id": legacy_opportunity_id})
        return self._augment_bridge_payload(task, pipeline_record, legacy_opportunity_id)

    def update_pipeline_record_task(self, pipeline_record_task_id: int, payload: dict) -> dict:
        with self.connect() as conn:
            before = self._pipeline_record_task_detail(conn, pipeline_record_task_id)
            actor_id = int(payload.get("updated_by_user_id") or before.get("created_by_user_id") or 2)
            status = payload.get("status", before["status"])
            priority = payload.get("priority", before["priority"])
            self._validate_task_status(status)
            self._validate_task_priority(priority)
            now = iso_now()
            completed_at = before.get("completed_at")
            if status == "done" and not completed_at:
                completed_at = now
            elif status != "done":
                completed_at = None
            conn.execute(
                """
                UPDATE pipeline_record_tasks
                SET title = ?, description = ?, status = ?, priority = ?, assigned_user_id = ?, due_at = ?,
                    completed_at = ?, is_blocking = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    payload.get("title", before["title"]),
                    payload.get("description", before.get("description", "")),
                    status,
                    priority,
                    payload.get("assigned_user_id", before.get("assigned_user_id")),
                    payload.get("due_at", before.get("due_at")),
                    completed_at,
                    1 if payload.get("is_blocking", before.get("is_blocking")) else 0,
                    now,
                    pipeline_record_task_id,
                ),
            )
            after = self._pipeline_record_task_detail(conn, pipeline_record_task_id)
            self._log_pipeline_record_activity(
                conn,
                before["pipeline_record_id"],
                pipeline_record_task_id,
                actor_id,
                "task_updated",
                "pipeline_record_task",
                pipeline_record_task_id,
                f"Task updated: {after['title']}",
                metadata={"status": after["status"], "priority": after["priority"]},
                created_at=now,
            )
            self._audit(conn, "pipeline_record_task", pipeline_record_task_id, "update", actor_id, before, after)
            return after

    def delete_pipeline_record_task(self, pipeline_record_task_id: int, actor_user_id: int) -> dict:
        with self.connect() as conn:
            before = self._pipeline_record_task_detail(conn, pipeline_record_task_id)
            self._log_pipeline_record_activity(
                conn,
                before["pipeline_record_id"],
                None,
                actor_user_id,
                "task_deleted",
                "pipeline_record_task",
                pipeline_record_task_id,
                f"Task deleted: {before['title']}",
            )
            conn.execute("DELETE FROM pipeline_record_tasks WHERE id = ?", (pipeline_record_task_id,))
            self._audit(conn, "pipeline_record_task", pipeline_record_task_id, "delete", actor_user_id, before, None)
            return {"success": True}

    def add_pipeline_record_task_comment(self, pipeline_record_task_id: int, payload: dict) -> dict:
        body = (payload.get("body") or "").strip()
        if not body:
            self._error("Comment body is required", {"body": "This field is required"})
        with self.connect() as conn:
            task = self._pipeline_record_task_detail(conn, pipeline_record_task_id)
            actor_id = int(payload.get("user_id") or 2)
            now = iso_now()
            comment_id = conn.execute(
                """
                INSERT INTO pipeline_record_task_comments(pipeline_record_task_id, user_id, body, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (pipeline_record_task_id, actor_id, body, now),
            ).lastrowid
            self._log_pipeline_record_activity(
                conn,
                task["pipeline_record_id"],
                pipeline_record_task_id,
                actor_id,
                "comment_added",
                "pipeline_record_task_comment",
                comment_id,
                "Task comment added.",
                created_at=now,
            )
            row = conn.execute(
                """
                SELECT prtc.*, u.name AS user_name
                FROM pipeline_record_task_comments prtc
                LEFT JOIN users u ON u.id = prtc.user_id
                WHERE prtc.id = ?
                """,
                (comment_id,),
            ).fetchone()
            comment = self._row(row)
            self._audit(conn, "pipeline_record_task_comment", comment_id, "create", actor_id, None, comment)
            return comment

    def add_pipeline_record_document(self, pipeline_record_id: int, payload: dict) -> dict:
        with self.connect() as conn:
            pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            if not pipeline_record.get("legacy_opportunity_id"):
                label = (payload.get("file_name") or payload.get("label") or "").strip()
                path = (payload.get("storage_path") or payload.get("url") or "").strip()
                if not label:
                    self._error("Document name is required", {"file_name": "This field is required"})
                if not path:
                    self._error("Document path or URL is required", {"storage_path": "This field is required"})
                actor_id = int(payload.get("uploaded_by_user_id") or 2)
                now = iso_now()
                document_id = conn.execute(
                    """
                    INSERT INTO pipeline_record_documents(
                        pipeline_record_id, file_name, document_category, storage_path, uploaded_by_user_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pipeline_record_id,
                        label,
                        payload.get("document_category", "other"),
                        path,
                        actor_id,
                        now,
                    ),
                ).lastrowid
                self._log_pipeline_record_activity(conn, pipeline_record_id, None, actor_id, "document_added", "pipeline_record_document", document_id, f"Document added: {label}")
                row = conn.execute("SELECT * FROM pipeline_record_documents WHERE id = ?", (document_id,)).fetchone()
                detail = self._row(row)
                detail["pipeline_record_id"] = pipeline_record_id
                detail["legacy_opportunity_id"] = None
                self._audit(conn, "pipeline_record_document", document_id, "create", actor_id, None, detail)
                return detail
            _, legacy_opportunity_id = self._resolve_pipeline_record_legacy_opportunity(conn, pipeline_record_id)
        document = self.add_document(legacy_opportunity_id, payload)
        return self._augment_bridge_payload(document, pipeline_record, legacy_opportunity_id)

    def add_pipeline_record_decision(self, pipeline_record_id: int, payload: dict) -> dict:
        with self.connect() as conn:
            pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            if not pipeline_record.get("legacy_opportunity_id"):
                decision_type = payload.get("decision_type")
                summary = (payload.get("decision_summary") or "").strip()
                if not decision_type or not summary:
                    self._error("Decision type and summary are required", {"decision_type": "Required", "decision_summary": "Required"})
                actor_id = int(payload.get("decided_by_user_id") or 2)
                now = iso_now()
                decision_id = conn.execute(
                    """
                    INSERT INTO pipeline_record_decisions(
                        pipeline_record_id, decision_type, decision_summary, rationale, decided_by_user_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (pipeline_record_id, decision_type, summary, payload.get("rationale", ""), actor_id, now),
                ).lastrowid
                row = conn.execute(
                    """
                    SELECT prd.*, u.name AS decided_by_name
                    FROM pipeline_record_decisions prd
                    LEFT JOIN users u ON u.id = prd.decided_by_user_id
                    WHERE prd.id = ?
                    """,
                    (decision_id,),
                ).fetchone()
                if payload.get("decision_outcome"):
                    self._validate_decision_outcome(payload["decision_outcome"])
                    conn.execute(
                        "UPDATE pipeline_records SET decision_outcome = ?, updated_at = ? WHERE id = ?",
                        (payload["decision_outcome"], now, pipeline_record_id),
                    )
                self._log_pipeline_record_activity(conn, pipeline_record_id, None, actor_id, "decision_logged", "pipeline_record_decision", decision_id, f"Decision logged: {summary}")
                detail = self._row(row)
                detail["pipeline_record_id"] = pipeline_record_id
                detail["legacy_opportunity_id"] = None
                self._audit(conn, "pipeline_record_decision", decision_id, "create", actor_id, None, detail)
                return detail
            _, legacy_opportunity_id = self._resolve_pipeline_record_legacy_opportunity(conn, pipeline_record_id)
        decision = self.add_decision(legacy_opportunity_id, payload)
        return self._augment_bridge_payload(decision, pipeline_record, legacy_opportunity_id)

    def change_pipeline_record_stage(self, pipeline_record_id: int, payload: dict) -> dict:
        with self.connect() as conn:
            pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            if not pipeline_record.get("legacy_opportunity_id"):
                new_stage = payload.get("to_stage")
                self._validate_pipeline_record_stage(new_stage)
                actor_id = int(payload.get("actor_user_id") or pipeline_record.get("owner_user_id") or 2)
                now = iso_now()
                before = dict(pipeline_record)
                updated = self.update_pipeline_record(
                    pipeline_record_id,
                    {
                        "stage": new_stage,
                        "status": self._pipeline_record_status_for_stage(new_stage, pipeline_record.get("status")),
                        "decision_outcome": self._pipeline_record_decision_for_stage(new_stage, pipeline_record.get("decision_outcome")),
                        "actor_user_id": actor_id,
                    },
                )
                conn.execute(
                    """
                    INSERT INTO pipeline_record_stage_history(
                        pipeline_record_id, from_stage, to_stage, changed_by_user_id, reason, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (pipeline_record_id, before["stage"], new_stage, actor_id, payload.get("reason", ""), now),
                )
                self._log_pipeline_record_activity(
                    conn,
                    pipeline_record_id,
                    None,
                    actor_id,
                    "stage_changed",
                    "pipeline_record",
                    pipeline_record_id,
                    f"Stage changed from {self._pipeline_record_stage_label(before['stage'])} to {self._pipeline_record_stage_label(new_stage)}.",
                    metadata={"from_stage": before["stage"], "to_stage": new_stage},
                    created_at=now,
                )
                return {
                    "pipeline_record": updated,
                    "legacy_opportunity_id": None,
                    "opportunity": None,
                }
            _, legacy_opportunity_id = self._resolve_pipeline_record_legacy_opportunity(conn, pipeline_record_id)
        opportunity = self.change_stage(legacy_opportunity_id, payload)
        with self.connect() as conn:
            updated_pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
        return {
            "pipeline_record": updated_pipeline_record,
            "legacy_opportunity_id": legacy_opportunity_id,
            "opportunity": self._augment_bridge_payload(opportunity, pipeline_record, legacy_opportunity_id),
        }

    def export_pipeline_record_report_markdown(self, pipeline_record_id: int) -> str:
        with self.connect() as conn:
            pipeline_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            if not pipeline_record.get("legacy_opportunity_id"):
                workflow = self._native_pipeline_record_workflow(conn, pipeline_record_id)
                lines = [
                    f"# {pipeline_record['organization_name']} ({pipeline_record['record_code']})",
                    "",
                    f"- Stage: {pipeline_record['stage']}",
                    f"- Status: {pipeline_record['status']}",
                    f"- Decision outcome: {pipeline_record['decision_outcome']}",
                    f"- Owner: {pipeline_record.get('owner_name') or 'Unassigned'}",
                    f"- Next step: {pipeline_record.get('next_step') or 'Not set'}",
                    "",
                    "## Investment Thesis",
                    pipeline_record.get("investment_thesis") or "No thesis captured yet.",
                    "",
                    "## Key Concerns",
                    pipeline_record.get("key_concerns") or "No concerns captured yet.",
                    "",
                    "## Notes",
                ]
                if workflow["notes"]:
                    for note in workflow["notes"]:
                        lines.append(f"- {note['created_at']}: {note['body']}")
                else:
                    lines.append("- No notes logged yet.")
                lines.extend(["", "## Tasks"])
                if workflow["tasks"]:
                    for task in workflow["tasks"]:
                        lines.append(f"- {task['title']} [{task['status']}] due {task.get('due_at') or 'TBD'}")
                else:
                    lines.append("- No tasks logged yet.")
                lines.extend(["", "## Decisions"])
                if workflow["decision_logs"]:
                    for decision in workflow["decision_logs"]:
                        lines.append(f"- {decision['created_at']}: {decision['decision_summary']} ({decision['decision_type']})")
                else:
                    lines.append("- No decisions logged yet.")
                lines.extend(["", "## Documents"])
                if workflow["documents"]:
                    for document in workflow["documents"]:
                        lines.append(f"- {document['file_name']} ({document['document_category']})")
                else:
                    lines.append("- No documents attached yet.")
                return "\n".join(lines)
            _, legacy_opportunity_id = self._resolve_pipeline_record_legacy_opportunity(conn, pipeline_record_id)
        return self.export_opportunity_report_markdown(legacy_opportunity_id)

    @staticmethod
    def _is_blank_value(value) -> bool:
        return value is None or value == "" or value == []

    def _apply_autofill_organization_updates(self, conn, organization_id: int, suggestions: dict, overwrite: bool = False) -> List[str]:
        before = self.get_organization(organization_id, conn=conn)
        fields = []
        assignments = []
        values = []
        mapping = [
            ("name", "name"),
            ("website", "website"),
            ("description", "description"),
            ("sector_primary", "sector_primary"),
            ("subsector", "subsector"),
            ("business_model", "business_model"),
            ("geography", "geography"),
            ("hq_city", "hq_city"),
            ("hq_country", "hq_country"),
        ]
        for db_field, source_field in mapping:
            suggested = suggestions.get(source_field)
            if self._is_blank_value(suggested):
                continue
            current = before.get(db_field)
            if not overwrite and not self._is_blank_value(current):
                continue
            assignments.append(f"{db_field} = ?")
            values.append(str(suggested).strip())
            fields.append(db_field)
        if assignments:
            assignments.append("updated_at = ?")
            values.append(iso_now())
            values.append(organization_id)
            conn.execute(
                f"UPDATE organizations SET {', '.join(assignments)} WHERE id = ?",
                tuple(values),
            )
        return fields

    def _apply_autofill_pipeline_record_updates(self, conn, pipeline_record_id: int, suggestions: dict, overwrite: bool = False) -> List[str]:
        before = self.get_pipeline_record(pipeline_record_id, conn=conn)
        fields = []
        assignments = []
        values = []
        mapping = [
            "round_name",
            "ticket_size_target",
            "ownership_target_pct",
            "valuation_min",
            "valuation_max",
            "annual_recurring_revenue",
            "revenue_growth_pct",
            "gross_margin_pct",
            "ebitda_margin_pct",
            "rule_of_40_pct",
            "monthly_burn",
            "cash_runway_months",
            "financials_updated_at",
            "investment_thesis",
            "key_concerns",
        ]
        for field_name in mapping:
            suggested = suggestions.get(field_name)
            if self._is_blank_value(suggested):
                continue
            current = before.get(field_name)
            if not overwrite and not self._is_blank_value(current):
                continue
            assignments.append(f"{field_name} = ?")
            values.append(str(suggested).strip())
            fields.append(field_name)
        suggested_risk_flags = suggestions.get("risk_flags") or []
        if suggested_risk_flags:
            merged_flags = sorted(set((before.get("risk_flags") or []) + list(suggested_risk_flags)))
            if overwrite or merged_flags != (before.get("risk_flags") or []):
                assignments.append("risk_flags = ?")
                values.append(to_json(merged_flags))
                fields.append("risk_flags")
        if assignments:
            assignments.append("updated_at = ?")
            values.append(iso_now())
            values.append(pipeline_record_id)
            conn.execute(
                f"UPDATE pipeline_records SET {', '.join(assignments)} WHERE id = ?",
                tuple(values),
            )
        return fields

    def autofill_pipeline_record_from_sources(self, pipeline_record_id: int, payload: Optional[dict] = None) -> dict:
        payload = payload or {}
        overwrite = bool(payload.get("overwrite"))
        preview_only = bool(payload.get("preview_only"))
        with self.connect() as conn:
            record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            organization = self.get_organization(record["organization_id"], conn=conn)
            documents = self.list_pipeline_record_documents(pipeline_record_id)
            suggestions = run_pipeline_record_autofill(
                record=record,
                organization=organization,
                documents=documents,
                shared_documents_dir=self.db_path.parent / "shared_documents",
            )
            applied = {
                "organization": [],
                "contact": [],
                "pipeline_record": [],
            }
            if not preview_only:
                applied["organization"] = self._apply_autofill_organization_updates(
                    conn,
                    record["organization_id"],
                    suggestions.get("organization", {}),
                    overwrite=overwrite,
                )
                contact_before = self._fetch_primary_organization_contact(conn, record["organization_id"])
                contact_id = self._upsert_primary_organization_contact(conn, record["organization_id"], suggestions.get("contact", {}))
                contact_after = self._fetch_primary_organization_contact(conn, record["organization_id"])
                if contact_id and contact_after != contact_before:
                    applied["contact"] = [
                        field_name
                        for field_name in ("name", "title", "email", "phone")
                        if (contact_after or {}).get(field_name) != (contact_before or {}).get(field_name)
                    ]
                applied["pipeline_record"] = self._apply_autofill_pipeline_record_updates(
                    conn,
                    pipeline_record_id,
                    suggestions.get("pipeline_record", {}),
                    overwrite=overwrite,
                )
                if any(applied.values()):
                    actor_id = int(payload.get("actor_user_id") or record.get("owner_user_id") or 2)
                    summary = "Autofill applied from website and linked materials."
                    self._log_pipeline_record_activity(
                        conn,
                        pipeline_record_id,
                        None,
                        actor_id,
                        "autofill_applied",
                        "pipeline_record",
                        pipeline_record_id,
                        summary,
                        metadata={
                            "applied_fields": applied,
                            "source_summary": suggestions.get("source_summary", {}),
                        },
                    )
            refreshed_record = self.get_pipeline_record(pipeline_record_id, conn=conn)
            refreshed_organization = self.get_organization(record["organization_id"], conn=conn)
            return {
                "pipeline_record": refreshed_record,
                "organization": refreshed_organization,
                "applied_fields": applied,
                "suggested_fields": suggestions,
                "source_summary": suggestions.get("source_summary", {}),
            }

    def preview_intake_autofill(self, payload: Optional[dict] = None) -> dict:
        payload = payload or {}
        website = (payload.get("website") or "").strip()
        company_name = (payload.get("company_name") or "").strip()
        documents = []
        for item in payload.get("documents") or []:
            if not isinstance(item, dict):
                continue
            documents.append(
                {
                    "file_name": (item.get("file_name") or "").strip(),
                    "document_category": (item.get("document_category") or "other").strip(),
                    "storage_path": (item.get("storage_path") or "").strip(),
                }
            )
        suggestions = run_pipeline_record_autofill(
            record={"organization_name": company_name},
            organization={"name": company_name, "website": website},
            documents=documents,
            shared_documents_dir=self.db_path.parent / "shared_documents",
        )
        prefill = {
            "website": suggestions.get("organization", {}).get("website") or website,
            "company_description": suggestions.get("organization", {}).get("description") or "",
            "sector": suggestions.get("organization", {}).get("sector_primary") or "",
            "subsector": suggestions.get("organization", {}).get("subsector") or "",
            "geography": suggestions.get("organization", {}).get("geography") or "",
            "hq_city": suggestions.get("organization", {}).get("hq_city") or "",
            "hq_country": suggestions.get("organization", {}).get("hq_country") or "",
            "business_model": suggestions.get("organization", {}).get("business_model") or "",
            "primary_contact_name": suggestions.get("contact", {}).get("primary_contact_name") or "",
            "primary_contact_title": suggestions.get("contact", {}).get("primary_contact_title") or "",
            "primary_contact_email": suggestions.get("contact", {}).get("primary_contact_email") or "",
            "primary_contact_phone": suggestions.get("contact", {}).get("primary_contact_phone") or "",
            "round_name": suggestions.get("pipeline_record", {}).get("round_name") or "",
            "ticket_size_target": suggestions.get("pipeline_record", {}).get("ticket_size_target") or "",
            "ownership_target_pct": suggestions.get("pipeline_record", {}).get("ownership_target_pct") or "",
            "valuation_min": suggestions.get("pipeline_record", {}).get("valuation_min") or "",
            "valuation_max": suggestions.get("pipeline_record", {}).get("valuation_max") or "",
            "annual_recurring_revenue": suggestions.get("pipeline_record", {}).get("annual_recurring_revenue") or "",
            "revenue_growth_pct": suggestions.get("pipeline_record", {}).get("revenue_growth_pct") or "",
            "gross_margin_pct": suggestions.get("pipeline_record", {}).get("gross_margin_pct") or "",
            "ebitda_margin_pct": suggestions.get("pipeline_record", {}).get("ebitda_margin_pct") or "",
            "rule_of_40_pct": suggestions.get("pipeline_record", {}).get("rule_of_40_pct") or "",
            "monthly_burn": suggestions.get("pipeline_record", {}).get("monthly_burn") or "",
            "cash_runway_months": suggestions.get("pipeline_record", {}).get("cash_runway_months") or "",
            "financials_updated_at": suggestions.get("pipeline_record", {}).get("financials_updated_at") or "",
            "investment_thesis": suggestions.get("pipeline_record", {}).get("investment_thesis") or "",
            "key_concerns": suggestions.get("pipeline_record", {}).get("key_concerns") or "",
        }
        return {
            "prefill": prefill,
            "suggested_fields": suggestions,
            "source_summary": suggestions.get("source_summary", {}),
        }

    def create_pipeline_record(self, payload: dict) -> dict:
        fields = {}
        if payload.get("organization_id") in ("", None):
            fields["organization_id"] = "This field is required"
        record_code = (payload.get("record_code") or "").strip()
        if not record_code:
            fields["record_code"] = "This field is required"
        for field_name in ("organization_id", "owner_user_id"):
            value = payload.get(field_name)
            if value in ("", None):
                continue
            try:
                int(value)
            except (TypeError, ValueError):
                fields[field_name] = "Must be numeric"
        stage = payload.get("stage") or "intake"
        status = payload.get("status") or "active"
        decision_outcome = payload.get("decision_outcome") or "pending"
        priority = payload.get("priority") or "medium"
        self._validate_pipeline_record_stage(stage)
        self._validate_pipeline_record_status(status)
        self._validate_decision_outcome(decision_outcome)
        self._validate_priority(priority)
        if fields:
            self._error("Invalid pipeline record payload", fields)
        now = iso_now()
        with self.connect() as conn:
            org = conn.execute("SELECT id FROM organizations WHERE id = ?", (int(payload["organization_id"]),)).fetchone()
            if not org:
                raise NotFoundError("Organization not found")
            self._upsert_primary_organization_contact(conn, int(payload["organization_id"]), payload)
            cursor = conn.execute(
                """
                INSERT INTO pipeline_records(
                    organization_id, record_code, stage, status, fund_fit, source_type, source_detail,
                    priority, owner_user_id, next_step, next_step_due_at, round_name, ticket_size_target,
                    ownership_target_pct, valuation_min, valuation_max, annual_recurring_revenue,
                    revenue_growth_pct, gross_margin_pct, ebitda_margin_pct, rule_of_40_pct,
                    monthly_burn, cash_runway_months, financials_updated_at, investment_thesis,
                    key_concerns, relationship_notes, nda_required, nda_status, risk_flags, tags,
                    decision_due_at, decision_outcome, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(payload["organization_id"]),
                    record_code,
                    stage,
                    status,
                    (payload.get("fund_fit") or "").strip(),
                    (payload.get("source_type") or "").strip(),
                    (payload.get("source_detail") or "").strip(),
                    priority,
                    int(payload["owner_user_id"]) if payload.get("owner_user_id") not in ("", None) else None,
                    (payload.get("next_step") or "").strip(),
                    payload.get("next_step_due_at") or None,
                    (payload.get("round_name") or "").strip(),
                    payload.get("ticket_size_target") or None,
                    payload.get("ownership_target_pct") or None,
                    payload.get("valuation_min") or None,
                    payload.get("valuation_max") or None,
                    payload.get("annual_recurring_revenue") or None,
                    payload.get("revenue_growth_pct") or None,
                    payload.get("gross_margin_pct") or None,
                    payload.get("ebitda_margin_pct") or None,
                    payload.get("rule_of_40_pct") or None,
                    payload.get("monthly_burn") or None,
                    payload.get("cash_runway_months") or None,
                    payload.get("financials_updated_at") or None,
                    (payload.get("investment_thesis") or "").strip(),
                    (payload.get("key_concerns") or "").strip(),
                    (payload.get("relationship_notes") or "").strip(),
                    1 if str(payload.get("nda_required", "0")) in {"1", "true", "True"} else 0,
                    payload.get("nda_status") or "not_required",
                    to_json(payload.get("risk_flags", [])),
                    to_json(payload.get("tags", [])),
                    payload.get("decision_due_at") or None,
                    decision_outcome,
                    now,
                    now,
                ),
            )
            pipeline_record_id = cursor.lastrowid
            detail = self.get_pipeline_record(pipeline_record_id, conn=conn)
            actor_id = int(payload.get("actor_user_id") or payload.get("owner_user_id") or 2)
            self._audit(conn, "pipeline_record", pipeline_record_id, "create", actor_id, None, detail)
            return detail

    def update_pipeline_record(self, pipeline_record_id: int, payload: dict) -> dict:
        fields = {}
        if "record_code" in payload and not (payload.get("record_code") or "").strip():
            fields["record_code"] = "This field is required"
        for field_name in ("organization_id", "owner_user_id"):
            value = payload.get(field_name)
            if value in ("", None):
                continue
            try:
                int(value)
            except (TypeError, ValueError):
                fields[field_name] = "Must be numeric"
        if "stage" in payload:
            self._validate_pipeline_record_stage(payload.get("stage") or "intake")
        if "status" in payload:
            self._validate_pipeline_record_status(payload.get("status") or "active")
        if "decision_outcome" in payload:
            self._validate_decision_outcome(payload.get("decision_outcome") or "pending")
        if "priority" in payload:
            self._validate_priority(payload.get("priority") or "medium")
        if fields:
            self._error("Invalid pipeline record payload", fields)
        with self.connect() as conn:
            before = self.get_pipeline_record(pipeline_record_id, conn=conn)
            organization_id = int(payload["organization_id"]) if payload.get("organization_id") not in ("", None) else before["organization_id"]
            org = conn.execute("SELECT id FROM organizations WHERE id = ?", (organization_id,)).fetchone()
            if not org:
                raise NotFoundError("Organization not found")
            actor_id = int(payload.get("actor_user_id") or before.get("owner_user_id") or 2)
            self._upsert_primary_organization_contact(conn, organization_id, payload)
            conn.execute(
                """
                UPDATE pipeline_records
                SET organization_id = ?, record_code = ?, stage = ?, status = ?, fund_fit = ?, source_type = ?,
                    source_detail = ?, priority = ?, owner_user_id = ?, next_step = ?, next_step_due_at = ?,
                    round_name = ?, ticket_size_target = ?, ownership_target_pct = ?, valuation_min = ?, valuation_max = ?,
                    annual_recurring_revenue = ?, revenue_growth_pct = ?, gross_margin_pct = ?, ebitda_margin_pct = ?,
                    rule_of_40_pct = ?, monthly_burn = ?, cash_runway_months = ?, financials_updated_at = ?,
                    investment_thesis = ?, key_concerns = ?, relationship_notes = ?, nda_required = ?, nda_status = ?,
                    risk_flags = ?, tags = ?, decision_due_at = ?, decision_outcome = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    organization_id,
                    (payload.get("record_code", before.get("record_code")) or "").strip(),
                    payload.get("stage", before.get("stage")),
                    payload.get("status", before.get("status")),
                    (payload.get("fund_fit", before.get("fund_fit")) or "").strip(),
                    (payload.get("source_type", before.get("source_type")) or "").strip(),
                    (payload.get("source_detail", before.get("source_detail")) or "").strip(),
                    payload.get("priority", before.get("priority")),
                    int(payload["owner_user_id"]) if payload.get("owner_user_id") not in ("", None) else before.get("owner_user_id"),
                    (payload.get("next_step", before.get("next_step")) or "").strip(),
                    payload.get("next_step_due_at", before.get("next_step_due_at")),
                    (payload.get("round_name", before.get("round_name")) or "").strip(),
                    payload.get("ticket_size_target", before.get("ticket_size_target")),
                    payload.get("ownership_target_pct", before.get("ownership_target_pct")),
                    payload.get("valuation_min", before.get("valuation_min")),
                    payload.get("valuation_max", before.get("valuation_max")),
                    payload.get("annual_recurring_revenue", before.get("annual_recurring_revenue")),
                    payload.get("revenue_growth_pct", before.get("revenue_growth_pct")),
                    payload.get("gross_margin_pct", before.get("gross_margin_pct")),
                    payload.get("ebitda_margin_pct", before.get("ebitda_margin_pct")),
                    payload.get("rule_of_40_pct", before.get("rule_of_40_pct")),
                    payload.get("monthly_burn", before.get("monthly_burn")),
                    payload.get("cash_runway_months", before.get("cash_runway_months")),
                    payload.get("financials_updated_at", before.get("financials_updated_at")),
                    (payload.get("investment_thesis", before.get("investment_thesis")) or "").strip(),
                    (payload.get("key_concerns", before.get("key_concerns")) or "").strip(),
                    (payload.get("relationship_notes", before.get("relationship_notes")) or "").strip(),
                    1 if str(payload.get("nda_required", before.get("nda_required", 0))) in {"1", "true", "True"} else 0,
                    payload.get("nda_status", before.get("nda_status")),
                    to_json(payload.get("risk_flags", before.get("risk_flags", []))),
                    to_json(payload.get("tags", before.get("tags", []))),
                    payload.get("decision_due_at", before.get("decision_due_at")),
                    payload.get("decision_outcome", before.get("decision_outcome")),
                    iso_now(),
                    pipeline_record_id,
                ),
            )
            after = self.get_pipeline_record(pipeline_record_id, conn=conn)
            self._audit(conn, "pipeline_record", pipeline_record_id, "update", actor_id, before, after)
            return after

    def list_intake_submissions(self, filters: Optional[dict] = None) -> List[dict]:
        filters = filters or {}
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    i.*,
                    u.name AS owner_name,
                    c.name AS converted_company_name,
                    o.deal_code AS converted_deal_code
                FROM intake_submissions i
                LEFT JOIN users u ON u.id = i.owner_user_id
                LEFT JOIN companies c ON c.id = i.converted_company_id
                LEFT JOIN opportunities o ON o.id = i.converted_opportunity_id
                ORDER BY i.created_at DESC
                """
            ).fetchall()
            items = [self._row(row) for row in rows]
            search = (filters.get("q") or "").strip().lower()
            if search:
                items = [
                    item for item in items
                    if search in (item.get("company_name") or "").lower()
                    or search in (item.get("submitted_by_name") or "").lower()
                    or search in (item.get("submitted_by_email") or "").lower()
                    or search in (item.get("summary") or "").lower()
                ]
            for key in ("status", "outcome", "intake_kind", "owner_user_id"):
                value = filters.get(key)
                if value and value not in {"all", ""}:
                    items = [item for item in items if str(item.get(key)) == str(value)]
            return items

    def create_intake_submission(self, payload: dict) -> dict:
        fields = {}
        company_name = (payload.get("company_name") or "").strip()
        if not company_name:
            fields["company_name"] = "This field is required"
        intake_kind = payload.get("intake_kind") or "direct_deal"
        outcome = payload.get("outcome") or "pending"
        status = payload.get("status") or "new"
        self._validate_optional_user_id(payload.get("owner_user_id"), "owner_user_id", fields)
        if fields:
            self._error("Invalid intake submission payload", fields)
        self._validate_intake_status(status)
        self._validate_intake_outcome(outcome)
        now = iso_now()
        raw_payload = payload.get("raw_payload", {})
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO intake_submissions(
                    submitted_by_name, submitted_by_email, company_name, source_type, intake_kind,
                    status, outcome, owner_user_id, summary, raw_payload, notes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (payload.get("submitted_by_name") or "").strip(),
                    (payload.get("submitted_by_email") or "").strip(),
                    company_name,
                    payload.get("source_type") or "inbound",
                    intake_kind,
                    status,
                    outcome,
                    int(payload["owner_user_id"]) if payload.get("owner_user_id") not in ("", None) else None,
                    (payload.get("summary") or "").strip(),
                    json.dumps(raw_payload if isinstance(raw_payload, (dict, list)) else {"value": raw_payload}),
                    (payload.get("notes") or "").strip(),
                    now,
                    now,
                ),
            )
            submission_id = cursor.lastrowid
            row = conn.execute(
                """
                SELECT i.*, u.name AS owner_name
                FROM intake_submissions i
                LEFT JOIN users u ON u.id = i.owner_user_id
                WHERE i.id = ?
                """,
                (submission_id,),
            ).fetchone()
            actor_id = int(payload.get("actor_user_id") or payload.get("owner_user_id") or 2)
            self._audit(conn, "intake_submission", submission_id, "create", actor_id, None, self._row(row))
            return self._row(row)

    def list_relationship_links(self, filters: Optional[dict] = None) -> List[dict]:
        filters = filters or {}
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    rl.*,
                    u.name AS owner_name,
                    c.name AS company_name,
                    ct.name AS contact_name,
                    rc.name AS related_company_name,
                    rct.name AS related_contact_name,
                    o.deal_code AS opportunity_deal_code
                FROM relationship_links rl
                LEFT JOIN users u ON u.id = rl.owner_user_id
                LEFT JOIN companies c ON c.id = rl.company_id
                LEFT JOIN contacts ct ON ct.id = rl.contact_id
                LEFT JOIN companies rc ON rc.id = rl.related_company_id
                LEFT JOIN contacts rct ON rct.id = rl.related_contact_id
                LEFT JOIN opportunities o ON o.id = rl.opportunity_id
                ORDER BY COALESCE(rl.next_touch_at, '9999-12-31') ASC, rl.updated_at DESC
                """
            ).fetchall()
            items = [self._row(row) for row in rows]
            search = (filters.get("q") or "").strip().lower()
            if search:
                items = [
                    item for item in items
                    if search in (item.get("company_name") or "").lower()
                    or search in (item.get("contact_name") or "").lower()
                    or search in (item.get("related_company_name") or "").lower()
                    or search in (item.get("related_contact_name") or "").lower()
                    or search in (item.get("notes") or "").lower()
                ]
            for key in ("link_type", "relationship_status", "owner_user_id", "opportunity_id", "company_id"):
                value = filters.get(key)
                if value and value not in {"all", ""}:
                    items = [item for item in items if str(item.get(key)) == str(value)]
            return items

    def create_relationship_link(self, payload: dict) -> dict:
        fields = {}
        link_type = payload.get("link_type") or ""
        relationship_status = payload.get("relationship_status") or "active"
        self._validate_relationship_link_type(link_type)
        self._validate_relationship_status(relationship_status)
        self._validate_optional_user_id(payload.get("owner_user_id"), "owner_user_id", fields)
        for field_name in ("company_id", "contact_id", "opportunity_id", "related_company_id", "related_contact_id"):
            value = payload.get(field_name)
            if value in ("", None):
                continue
            try:
                int(value)
            except (TypeError, ValueError):
                fields[field_name] = "Must be numeric"
        if not any(payload.get(name) not in ("", None) for name in ("company_id", "contact_id", "opportunity_id")):
            fields["company_id"] = "One anchor entity is required"
        if not any(payload.get(name) not in ("", None) for name in ("related_company_id", "related_contact_id")):
            fields["related_company_id"] = "A related company or contact is required"
        if fields:
            self._error("Invalid relationship link payload", fields)
        now = iso_now()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO relationship_links(
                    company_id, contact_id, opportunity_id, related_company_id, related_contact_id,
                    link_type, relationship_status, owner_user_id, warmth, notes,
                    last_touch_at, next_touch_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(payload["company_id"]) if payload.get("company_id") not in ("", None) else None,
                    int(payload["contact_id"]) if payload.get("contact_id") not in ("", None) else None,
                    int(payload["opportunity_id"]) if payload.get("opportunity_id") not in ("", None) else None,
                    int(payload["related_company_id"]) if payload.get("related_company_id") not in ("", None) else None,
                    int(payload["related_contact_id"]) if payload.get("related_contact_id") not in ("", None) else None,
                    link_type,
                    relationship_status,
                    int(payload["owner_user_id"]) if payload.get("owner_user_id") not in ("", None) else None,
                    (payload.get("warmth") or "").strip(),
                    (payload.get("notes") or "").strip(),
                    payload.get("last_touch_at") or None,
                    payload.get("next_touch_at") or None,
                    now,
                    now,
                ),
            )
            link_id = cursor.lastrowid
            row = conn.execute(
                """
                SELECT
                    rl.*,
                    u.name AS owner_name,
                    c.name AS company_name,
                    ct.name AS contact_name,
                    rc.name AS related_company_name,
                    rct.name AS related_contact_name,
                    o.deal_code AS opportunity_deal_code
                FROM relationship_links rl
                LEFT JOIN users u ON u.id = rl.owner_user_id
                LEFT JOIN companies c ON c.id = rl.company_id
                LEFT JOIN contacts ct ON ct.id = rl.contact_id
                LEFT JOIN companies rc ON rc.id = rl.related_company_id
                LEFT JOIN contacts rct ON rct.id = rl.related_contact_id
                LEFT JOIN opportunities o ON o.id = rl.opportunity_id
                WHERE rl.id = ?
                """,
                (link_id,),
            ).fetchone()
            actor_id = int(payload.get("actor_user_id") or payload.get("owner_user_id") or 2)
            self._audit(conn, "relationship_link", link_id, "create", actor_id, None, self._row(row))
            return self._row(row)

    def list_opportunities(self, filters: Optional[dict] = None) -> List[dict]:
        filters = filters or {}
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    o.*,
                    c.name AS company_name,
                    c.website,
                    c.sector,
                    c.subsector,
                    c.geography,
                    c.hq_city,
                    c.hq_country,
                    c.business_model,
                    c.description AS company_description,
                    u.name AS owner_name,
                    u.role AS owner_role
                FROM opportunities o
                JOIN companies c ON c.id = o.company_id
                LEFT JOIN users u ON u.id = o.owner_user_id
                WHERE o.deleted_at IS NULL
                ORDER BY o.updated_at DESC
                """
            ).fetchall()
            opportunities = [self._enrich_opportunity(conn, row) for row in rows]
            search = (filters.get("q") or "").strip().lower()
            if search:
                opportunities = [
                    item
                    for item in opportunities
                    if search in (item["company_name"] or "").lower()
                    or search in (item.get("sector") or "").lower()
                    or search in (item.get("source_detail") or "").lower()
                    or search in (item.get("investment_thesis") or "").lower()
                    or search in (item.get("key_concerns") or "").lower()
                    or search in (item.get("primary_contact_name") or "").lower()
                ]
            for key in ("stage", "priority", "owner_user_id", "sector", "geography", "status"):
                value = filters.get(key)
                if value and value not in {"all", ""}:
                    opportunities = [item for item in opportunities if str(item.get(key)) == str(value)]
            if filters.get("missing_info") == "1":
                opportunities = [item for item in opportunities if item["missing_fields"]]
            if filters.get("overdue_only") == "1":
                opportunities = [item for item in opportunities if item["overdue_tasks"] or "overdue_next_step" in item["workflow_flags"]]
            sort_key = filters.get("sort") or "updated_at"
            reverse = filters.get("order", "desc") != "asc"

            def sort_value(item):
                if sort_key in {"ticket_size_target", "valuation_max", "priority_score", "open_tasks", "overdue_tasks"}:
                    return item.get(sort_key) or 0
                if sort_key == "company_name":
                    return (item.get("company_name") or "").lower()
                if sort_key == "stage":
                    return STAGE_RANKS.get(item.get("stage"), 999)
                return item.get(sort_key) or ""

            opportunities.sort(key=sort_value, reverse=reverse)
            return opportunities

    def list_pipeline_kanban(self, filters: Optional[dict] = None) -> dict:
        items = self.list_opportunities(filters)
        board = {stage["id"]: [] for stage in STAGES if stage["id"] in ACTIVE_STAGES or stage["id"] in {"closed_won", "closed_lost"}}
        for item in items:
            board.setdefault(item["stage"], []).append(item)
        return board

    def get_opportunity(self, opportunity_id: int, conn: Optional[sqlite3.Connection] = None) -> dict:
        own_conn = False
        if conn is None:
            own_conn = True
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
        try:
            detail = self._enrich_opportunity(conn, self._base_opportunity(conn, opportunity_id))
            detail["notes"] = [
                self._row(row)
                for row in conn.execute(
                    """
                    SELECT n.*, u.name AS author_name
                    FROM notes n
                    LEFT JOIN users u ON u.id = n.author_user_id
                    WHERE opportunity_id = ?
                    ORDER BY is_pinned DESC, created_at DESC
                    """,
                    (opportunity_id,),
                ).fetchall()
            ]
            detail["tasks"] = [
                self._row(row)
                for row in conn.execute(
                    """
                    SELECT t.*, u.name AS assignee_name
                    FROM tasks t
                    LEFT JOIN users u ON u.id = t.assigned_user_id
                    WHERE opportunity_id = ?
                    ORDER BY
                        CASE t.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                        CASE t.status WHEN 'blocked' THEN 0 WHEN 'in_progress' THEN 1 WHEN 'todo' THEN 2 ELSE 3 END,
                        t.due_at ASC
                    """,
                    (opportunity_id,),
                ).fetchall()
            ]
            for task in detail["tasks"]:
                task["comments"] = [
                    self._row(row)
                    for row in conn.execute(
                        """
                        SELECT tc.*, u.name AS user_name
                        FROM task_comments tc
                        LEFT JOIN users u ON u.id = tc.user_id
                        WHERE task_id = ?
                        ORDER BY created_at ASC
                        """,
                        (task["id"],),
                    ).fetchall()
                ]
                task["is_overdue"] = bool(
                    task.get("due_at")
                    and task["status"] not in {"done", "canceled"}
                    and parse_iso(task["due_at"])
                    and parse_iso(task["due_at"]).date() < utc_now().date()
                )
            detail["documents"] = [
                self._row(row)
                for row in conn.execute(
                    """
                    SELECT d.*, u.name AS uploader_name
                    FROM documents d
                    LEFT JOIN users u ON u.id = d.uploaded_by_user_id
                    WHERE opportunity_id = ?
                    ORDER BY created_at DESC
                    """,
                    (opportunity_id,),
                ).fetchall()
            ]
            detail["decision_logs"] = [
                self._row(row)
                for row in conn.execute(
                    """
                    SELECT dl.*, u.name AS decided_by_name
                    FROM decision_logs dl
                    LEFT JOIN users u ON u.id = dl.decided_by_user_id
                    WHERE opportunity_id = ?
                    ORDER BY created_at DESC
                    """,
                    (opportunity_id,),
                ).fetchall()
            ]
            detail["stage_history"] = [
                self._row(row)
                for row in conn.execute(
                    """
                    SELECT sh.*, u.name AS changed_by_name
                    FROM stage_history sh
                    LEFT JOIN users u ON u.id = sh.changed_by_user_id
                    WHERE opportunity_id = ?
                    ORDER BY created_at DESC
                    """,
                    (opportunity_id,),
                ).fetchall()
            ]
            detail["activities"] = [
                self._row(row)
                for row in conn.execute(
                    """
                    SELECT a.*, u.name AS user_name
                    FROM activities a
                    LEFT JOIN users u ON u.id = a.user_id
                    WHERE opportunity_id = ?
                    ORDER BY created_at DESC
                    LIMIT 100
                    """,
                    (opportunity_id,),
                ).fetchall()
            ]
            detail["audit_events"] = [
                self._row(row)
                for row in conn.execute(
                    """
                    SELECT ae.*, u.name AS actor_name
                    FROM audit_events ae
                    LEFT JOIN users u ON u.id = ae.actor_user_id
                    WHERE entity_type = 'opportunity' AND entity_id = ?
                    ORDER BY created_at DESC
                    LIMIT 100
                    """,
                    (opportunity_id,),
                ).fetchall()
            ]
            return detail
        finally:
            if own_conn:
                conn.close()

    def _upsert_company(self, conn, payload: dict, existing_company_id: Optional[int] = None) -> int:
        name = (payload.get("company_name") or "").strip()
        if not name:
            self._error("Company name is required", {"company_name": "This field is required"})
        now = iso_now()
        if existing_company_id:
            conn.execute(
                """
                UPDATE companies
                SET name = ?, website = ?, sector = ?, subsector = ?, geography = ?, hq_city = ?, hq_country = ?,
                    business_model = ?, description = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name,
                    payload.get("website", ""),
                    payload.get("sector", ""),
                    payload.get("subsector", ""),
                    payload.get("geography", ""),
                    payload.get("hq_city", ""),
                    payload.get("hq_country", ""),
                    payload.get("business_model", ""),
                    payload.get("company_description", ""),
                    now,
                    existing_company_id,
                ),
            )
            company_id = existing_company_id
        else:
            existing = conn.execute("SELECT id FROM companies WHERE lower(name) = lower(?)", (name,)).fetchone()
            if existing:
                company_id = existing["id"]
                conn.execute(
                    """
                    UPDATE companies
                    SET website = COALESCE(NULLIF(?, ''), website),
                        sector = COALESCE(NULLIF(?, ''), sector),
                        subsector = COALESCE(NULLIF(?, ''), subsector),
                        geography = COALESCE(NULLIF(?, ''), geography),
                        hq_city = COALESCE(NULLIF(?, ''), hq_city),
                        hq_country = COALESCE(NULLIF(?, ''), hq_country),
                        business_model = COALESCE(NULLIF(?, ''), business_model),
                        description = COALESCE(NULLIF(?, ''), description),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        payload.get("website", ""),
                        payload.get("sector", ""),
                        payload.get("subsector", ""),
                        payload.get("geography", ""),
                        payload.get("hq_city", ""),
                        payload.get("hq_country", ""),
                        payload.get("business_model", ""),
                        payload.get("company_description", ""),
                        now,
                        company_id,
                    ),
                )
            else:
                company_id = conn.execute(
                    """
                    INSERT INTO companies(
                        name, website, sector, subsector, geography, hq_city, hq_country,
                        business_model, description, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        payload.get("website", ""),
                        payload.get("sector", ""),
                        payload.get("subsector", ""),
                        payload.get("geography", ""),
                        payload.get("hq_city", ""),
                        payload.get("hq_country", ""),
                        payload.get("business_model", ""),
                        payload.get("company_description", ""),
                        now,
                        now,
                    ),
                ).lastrowid

        primary = self._fetch_primary_contact(conn, company_id)
        contact_name = payload.get("primary_contact_name", "")
        contact_email = payload.get("primary_contact_email", "")
        contact_phone = payload.get("primary_contact_phone", "")
        contact_title = payload.get("primary_contact_title", "Founder")
        if primary:
            conn.execute(
                """
                UPDATE contacts
                SET name = ?, title = ?, email = ?, phone = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    contact_name or primary.get("name") or "",
                    contact_title or primary.get("title") or "",
                    contact_email or primary.get("email") or "",
                    contact_phone or primary.get("phone") or "",
                    now,
                    primary["id"],
                ),
            )
        elif contact_name:
            conn.execute(
                """
                INSERT INTO contacts(company_id, name, title, email, phone, is_primary, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (company_id, contact_name, contact_title, contact_email, contact_phone, now, now),
            )
        self._sync_company_to_organization(
            conn,
            company_id,
            owner_user_id=payload.get("owner_user_id"),
        )
        return company_id

    def _validate_opportunity_payload(self, payload: dict, partial: bool = False) -> None:
        fields = {}
        required = ["company_name", "source_detail", "stage", "priority"]
        if not partial:
            for field in required:
                if not payload.get(field):
                    fields[field] = "This field is required"
        if payload.get("stage"):
            self._validate_stage(payload["stage"])
        if payload.get("status"):
            self._validate_opportunity_status(payload["status"])
        if payload.get("priority"):
            self._validate_priority(payload["priority"])
        owner = payload.get("owner_user_id")
        if owner is not None and str(owner) != "":
            try:
                int(owner)
            except ValueError:
                fields["owner_user_id"] = "Owner must be a number"
        for field in ("ticket_size_target", "ticket_size_min", "ticket_size_max", "valuation_min", "valuation_max", "ownership_target_pct"):
            value = payload.get(field)
            if value in ("", None):
                continue
            try:
                float(value)
            except (TypeError, ValueError):
                fields[field] = "Must be numeric"
        if fields:
            self._error("Invalid opportunity payload", fields)

    def _snapshot(self, conn, opportunity_id: int) -> dict:
        return self.get_opportunity(opportunity_id, conn=conn)

    def _log_activity(
        self,
        conn,
        opportunity_id: Optional[int],
        task_id: Optional[int],
        user_id: Optional[int],
        activity_type: str,
        entity_type: str,
        entity_id: Optional[int],
        summary: str,
        metadata: Optional[dict] = None,
        created_at: Optional[str] = None,
    ) -> None:
        created_at = created_at or iso_now()
        conn.execute(
            """
            INSERT INTO activities(
                opportunity_id, task_id, user_id, activity_type, entity_type, entity_id,
                summary, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                opportunity_id,
                task_id,
                user_id,
                activity_type,
                entity_type,
                entity_id,
                summary,
                json.dumps(metadata or {}),
                created_at,
            ),
        )
        if opportunity_id:
            conn.execute(
                "UPDATE opportunities SET last_activity_at = ?, updated_at = ? WHERE id = ?",
                (created_at, created_at, opportunity_id),
            )

    def _audit(self, conn, entity_type: str, entity_id: int, action: str, actor_user_id: Optional[int], before, after) -> None:
        conn.execute(
            """
            INSERT INTO audit_events(entity_type, entity_id, action, actor_user_id, before_json, after_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_type,
                entity_id,
                action,
                actor_user_id,
                json.dumps(before) if before is not None else None,
                json.dumps(after) if after is not None else None,
                iso_now(),
            ),
        )

    def create_opportunity(self, payload: dict) -> dict:
        self._validate_opportunity_payload(payload)
        with self.connect() as conn:
            actor_id = int(payload.get("actor_user_id") or payload.get("owner_user_id") or 2)
            company_id = self._upsert_company(conn, payload)
            now = iso_now()
            stage = payload.get("stage", "new")
            cursor = conn.execute(
                """
                INSERT INTO opportunities(
                    deal_code, company_id, source_type, source_detail, owner_user_id, stage, status, priority,
                    priority_score, fund_fit, fund_fit_score, market_score, team_score, traction_score,
                    round_name, ticket_size_target, ticket_size_min, ticket_size_max, valuation_min, valuation_max,
                    ownership_target_pct, next_step, next_step_due_at, last_contacted_at, decision_due_at,
                    investment_thesis, key_concerns, relationship_notes, nda_required, nda_status, workflow_flags,
                    risk_flags, missing_fields, tags, last_activity_at, stage_entered_at, created_by_user_id,
                    updated_by_user_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("deal_code") or f"VCC-{int(datetime.now().timestamp())}",
                    company_id,
                    payload.get("source_type") or "inbound",
                    payload.get("source_detail"),
                    payload.get("owner_user_id") or None,
                    stage,
                    payload.get("status") or ("on_hold" if stage in {"closed_lost"} else "active"),
                    payload.get("priority") or "medium",
                    self._compute_priority_score(payload),
                    payload.get("fund_fit", ""),
                    int(payload.get("fund_fit_score") or 0),
                    int(payload.get("market_score") or 0),
                    int(payload.get("team_score") or 0),
                    int(payload.get("traction_score") or 0),
                    payload.get("round_name", ""),
                    payload.get("ticket_size_target") or None,
                    payload.get("ticket_size_min") or None,
                    payload.get("ticket_size_max") or None,
                    payload.get("valuation_min") or None,
                    payload.get("valuation_max") or None,
                    payload.get("ownership_target_pct") or None,
                    payload.get("next_step", ""),
                    payload.get("next_step_due_at") or None,
                    payload.get("last_contacted_at") or None,
                    payload.get("decision_due_at") or None,
                    payload.get("investment_thesis", ""),
                    payload.get("key_concerns", ""),
                    payload.get("relationship_notes", ""),
                    1 if str(payload.get("nda_required", "0")) in {"1", "true", "True"} else 0,
                    payload.get("nda_status", "not_required"),
                    to_json(payload.get("workflow_flags", [])),
                    to_json(payload.get("risk_flags", [])),
                    to_json([]),
                    to_json(payload.get("tags", [])),
                    now,
                    now,
                    actor_id,
                    actor_id,
                    now,
                    now,
                ),
            )
            opportunity_id = cursor.lastrowid
            self._snapshot(conn, opportunity_id)
            detail = self.get_opportunity(opportunity_id, conn=conn)
            conn.execute(
                "UPDATE opportunities SET missing_fields = ?, workflow_flags = ? WHERE id = ?",
                (to_json(detail["missing_fields"]), to_json(detail["workflow_flags"]), opportunity_id),
            )
            detail = self.get_opportunity(opportunity_id, conn=conn)
            conn.execute(
                """
                INSERT INTO stage_history(opportunity_id, from_stage, to_stage, changed_by_user_id, reason, created_at)
                VALUES (?, NULL, ?, ?, ?, ?)
                """,
                (opportunity_id, stage, actor_id, "Opportunity created", now),
            )
            self._log_activity(conn, opportunity_id, None, actor_id, "opportunity_created", "opportunity", opportunity_id, "Opportunity created.")
            self._sync_opportunity_to_pipeline_record(conn, opportunity_id)
            self._audit(conn, "opportunity", opportunity_id, "create", actor_id, None, detail)
            return self.get_opportunity(opportunity_id, conn=conn)

    def update_opportunity(self, opportunity_id: int, payload: dict) -> dict:
        self._validate_opportunity_payload(payload, partial=True)
        with self.connect() as conn:
            before = self.get_opportunity(opportunity_id, conn=conn)
            actor_id = int(payload.get("actor_user_id") or before.get("owner_user_id") or 2)
            company_id = self._upsert_company(conn, {**before, **payload}, existing_company_id=before["company_id"])
            updates = {
                "source_type": payload.get("source_type", before["source_type"]),
                "source_detail": payload.get("source_detail", before["source_detail"]),
                "owner_user_id": payload.get("owner_user_id", before["owner_user_id"]),
                "status": payload.get("status", before["status"]),
                "priority": payload.get("priority", before["priority"]),
                "priority_score": self._compute_priority_score({**before, **payload}),
                "fund_fit": payload.get("fund_fit", before.get("fund_fit")),
                "fund_fit_score": int(payload.get("fund_fit_score", before.get("fund_fit_score") or 0)),
                "market_score": int(payload.get("market_score", before.get("market_score") or 0)),
                "team_score": int(payload.get("team_score", before.get("team_score") or 0)),
                "traction_score": int(payload.get("traction_score", before.get("traction_score") or 0)),
                "round_name": payload.get("round_name", before.get("round_name")),
                "ticket_size_target": payload.get("ticket_size_target", before.get("ticket_size_target")),
                "ticket_size_min": payload.get("ticket_size_min", before.get("ticket_size_min")),
                "ticket_size_max": payload.get("ticket_size_max", before.get("ticket_size_max")),
                "valuation_min": payload.get("valuation_min", before.get("valuation_min")),
                "valuation_max": payload.get("valuation_max", before.get("valuation_max")),
                "ownership_target_pct": payload.get("ownership_target_pct", before.get("ownership_target_pct")),
                "next_step": payload.get("next_step", before.get("next_step")),
                "next_step_due_at": payload.get("next_step_due_at", before.get("next_step_due_at")),
                "last_contacted_at": payload.get("last_contacted_at", before.get("last_contacted_at")),
                "decision_due_at": payload.get("decision_due_at", before.get("decision_due_at")),
                "investment_thesis": payload.get("investment_thesis", before.get("investment_thesis")),
                "key_concerns": payload.get("key_concerns", before.get("key_concerns")),
                "relationship_notes": payload.get("relationship_notes", before.get("relationship_notes")),
                "nda_required": 1 if str(payload.get("nda_required", before.get("nda_required", 0))) in {"1", "true", "True"} else 0,
                "nda_status": payload.get("nda_status", before.get("nda_status")),
                "workflow_flags": to_json(payload.get("workflow_flags", before.get("workflow_flags", []))),
                "risk_flags": to_json(payload.get("risk_flags", before.get("risk_flags", []))),
                "tags": to_json(payload.get("tags", before.get("tags", []))),
                "updated_by_user_id": actor_id,
                "updated_at": iso_now(),
                "company_id": company_id,
            }
            if updates["status"] == "closed_won":
                updates["stage"] = "closed_won"
            elif updates["status"] == "closed_lost":
                updates["stage"] = "closed_lost"
            else:
                updates["stage"] = before["stage"]
            conn.execute(
                """
                UPDATE opportunities
                SET company_id = ?, source_type = ?, source_detail = ?, owner_user_id = ?, status = ?, stage = ?, priority = ?, priority_score = ?,
                    fund_fit = ?, fund_fit_score = ?, market_score = ?, team_score = ?, traction_score = ?, round_name = ?,
                    ticket_size_target = ?, ticket_size_min = ?, ticket_size_max = ?, valuation_min = ?, valuation_max = ?,
                    ownership_target_pct = ?, next_step = ?, next_step_due_at = ?, last_contacted_at = ?, decision_due_at = ?,
                    investment_thesis = ?, key_concerns = ?, relationship_notes = ?, nda_required = ?, nda_status = ?,
                    workflow_flags = ?, risk_flags = ?, tags = ?, updated_by_user_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    updates["company_id"],
                    updates["source_type"],
                    updates["source_detail"],
                    updates["owner_user_id"],
                    updates["status"],
                    updates["stage"],
                    updates["priority"],
                    updates["priority_score"],
                    updates["fund_fit"],
                    updates["fund_fit_score"],
                    updates["market_score"],
                    updates["team_score"],
                    updates["traction_score"],
                    updates["round_name"],
                    updates["ticket_size_target"],
                    updates["ticket_size_min"],
                    updates["ticket_size_max"],
                    updates["valuation_min"],
                    updates["valuation_max"],
                    updates["ownership_target_pct"],
                    updates["next_step"],
                    updates["next_step_due_at"],
                    updates["last_contacted_at"],
                    updates["decision_due_at"],
                    updates["investment_thesis"],
                    updates["key_concerns"],
                    updates["relationship_notes"],
                    updates["nda_required"],
                    updates["nda_status"],
                    updates["workflow_flags"],
                    updates["risk_flags"],
                    updates["tags"],
                    updates["updated_by_user_id"],
                    updates["updated_at"],
                    opportunity_id,
                ),
            )
            after = self.get_opportunity(opportunity_id, conn=conn)
            if before["status"] != after["status"]:
                self._log_activity(
                    conn,
                    opportunity_id,
                    None,
                    actor_id,
                    "status_changed",
                    "opportunity",
                    opportunity_id,
                    f"Status changed from {before['status'].replace('_', ' ')} to {after['status'].replace('_', ' ')}.",
                    metadata={"from_status": before["status"], "to_status": after["status"]},
                )
                after = self.get_opportunity(opportunity_id, conn=conn)
            conn.execute(
                "UPDATE opportunities SET missing_fields = ?, workflow_flags = ? WHERE id = ?",
                (to_json(after["missing_fields"]), to_json(after["workflow_flags"]), opportunity_id),
            )
            after = self.get_opportunity(opportunity_id, conn=conn)
            self._log_activity(conn, opportunity_id, None, actor_id, "field_updated", "opportunity", opportunity_id, "Opportunity updated.")
            self._sync_opportunity_to_pipeline_record(conn, opportunity_id)
            self._audit(conn, "opportunity", opportunity_id, "update", actor_id, before, after)
            return after

    def delete_opportunity(self, opportunity_id: int, actor_user_id: int) -> dict:
        with self.connect() as conn:
            before = self.get_opportunity(opportunity_id, conn=conn)
            now = iso_now()
            conn.execute(
                "UPDATE opportunities SET deleted_at = ?, deleted_by_user_id = ?, updated_at = ?, status = 'closed_lost', stage = 'closed_lost' WHERE id = ?",
                (now, actor_user_id, now, opportunity_id),
            )
            self._log_activity(conn, opportunity_id, None, actor_user_id, "opportunity_deleted", "opportunity", opportunity_id, "Opportunity marked as lost/removed.")
            self._sync_opportunity_to_pipeline_record(conn, opportunity_id)
            self._audit(conn, "opportunity", opportunity_id, "delete", actor_user_id, before, {"deleted_at": now})
            return {"success": True}

    def change_stage(self, opportunity_id: int, payload: dict) -> dict:
        new_stage = payload.get("to_stage")
        self._validate_stage(new_stage)
        with self.connect() as conn:
            before = self.get_opportunity(opportunity_id, conn=conn)
            actor_id = int(payload.get("actor_user_id") or before.get("owner_user_id") or 2)
            if STAGE_RANKS[new_stage] > STAGE_RANKS[before["stage"]] and before["missing_fields"]:
                self._error("Cannot advance stage while required fields are missing", {"missing_fields": ", ".join(before["missing_fields"])})
            now = iso_now()
            status = before["status"]
            if new_stage == "closed_won":
                status = "closed_won"
            elif new_stage == "closed_lost":
                status = "closed_lost"
            else:
                status = "active"
            conn.execute(
                "UPDATE opportunities SET stage = ?, status = ?, stage_entered_at = ?, updated_at = ?, updated_by_user_id = ? WHERE id = ?",
                (new_stage, status, now, now, actor_id, opportunity_id),
            )
            conn.execute(
                """
                INSERT INTO stage_history(opportunity_id, from_stage, to_stage, changed_by_user_id, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (opportunity_id, before["stage"], new_stage, actor_id, payload.get("reason", ""), now),
            )
            self._log_activity(
                conn,
                opportunity_id,
                None,
                actor_id,
                "stage_changed",
                "opportunity",
                opportunity_id,
                f"Stage changed from {STAGE_LABELS[before['stage']]} to {STAGE_LABELS[new_stage]}.",
                metadata={"from_stage": before["stage"], "to_stage": new_stage},
            )
            after = self.get_opportunity(opportunity_id, conn=conn)
            conn.execute(
                "UPDATE opportunities SET workflow_flags = ?, missing_fields = ? WHERE id = ?",
                (to_json(after["workflow_flags"]), to_json(after["missing_fields"]), opportunity_id),
            )
            after = self.get_opportunity(opportunity_id, conn=conn)
            self._sync_opportunity_to_pipeline_record(conn, opportunity_id)
            self._audit(conn, "opportunity", opportunity_id, "update", actor_id, before, after)
            return after

    def add_note(self, opportunity_id: int, payload: dict) -> dict:
        note_type = payload.get("note_type", "general")
        self._validate_note_type(note_type)
        body = (payload.get("body") or "").strip()
        if not body:
            self._error("Note body is required", {"body": "This field is required"})
        with self.connect() as conn:
            self._base_opportunity(conn, opportunity_id)
            actor_id = int(payload.get("author_user_id") or 2)
            now = iso_now()
            note_id = conn.execute(
                """
                INSERT INTO notes(opportunity_id, author_user_id, note_type, body, is_pinned, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (opportunity_id, actor_id, note_type, body, 1 if payload.get("is_pinned") else 0, now, now),
            ).lastrowid
            self._log_activity(conn, opportunity_id, None, actor_id, "note_added", "note", note_id, "Analyst note added.")
            row = conn.execute(
                """
                SELECT n.*, u.name AS author_name
                FROM notes n LEFT JOIN users u ON u.id = n.author_user_id
                WHERE n.id = ?
                """,
                (note_id,),
            ).fetchone()
            self._audit(conn, "note", note_id, "create", actor_id, None, self._row(row))
            return self._row(row)

    def update_note(self, note_id: int, payload: dict) -> dict:
        body = (payload.get("body") or "").strip()
        if not body:
            self._error("Note body is required", {"body": "This field is required"})
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM notes WHERE id = ?", (note_id,)).fetchone()
            if not row:
                raise NotFoundError("Note not found")
            before = self._row(row)
            actor_id = int(payload.get("author_user_id") or before.get("author_user_id") or 2)
            now = iso_now()
            conn.execute(
                "UPDATE notes SET body = ?, note_type = ?, is_pinned = ?, updated_at = ? WHERE id = ?",
                (body, payload.get("note_type", before["note_type"]), 1 if payload.get("is_pinned") else 0, now, note_id),
            )
            after = conn.execute(
                "SELECT * FROM notes WHERE id = ?",
                (note_id,),
            ).fetchone()
            self._audit(conn, "note", note_id, "update", actor_id, before, self._row(after))
            return self._row(after)

    def delete_note(self, note_id: int, actor_user_id: int) -> dict:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM notes WHERE id = ?", (note_id,)).fetchone()
            if not row:
                raise NotFoundError("Note not found")
            before = self._row(row)
            conn.execute("DELETE FROM notes WHERE id = ?", (note_id,))
            self._audit(conn, "note", note_id, "delete", actor_user_id, before, None)
            return {"success": True}

    def list_tasks(self, filters: Optional[dict] = None) -> List[dict]:
        filters = filters or {}
        with self.connect() as conn:
            legacy_rows = conn.execute(
                """
                SELECT
                    t.*,
                    u.name AS assignee_name,
                    c.name AS company_name,
                    o.stage,
                    o.priority AS opportunity_priority,
                    pr.id AS pipeline_record_id
                FROM tasks t
                JOIN opportunities o ON o.id = t.opportunity_id AND o.deleted_at IS NULL
                JOIN companies c ON c.id = o.company_id
                LEFT JOIN pipeline_records pr ON pr.legacy_opportunity_id = o.id
                LEFT JOIN users u ON u.id = t.assigned_user_id
                ORDER BY
                    CASE t.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                    COALESCE(t.due_at, '9999-12-31') ASC
                """
            ).fetchall()
            tasks = []
            for row in legacy_rows:
                item = self._row(row)
                item["task_source"] = "legacy"
                if filters.get("assigned_user_id") and str(item.get("assigned_user_id")) != str(filters["assigned_user_id"]):
                    continue
                if filters.get("status") and filters["status"] not in {"all", ""} and item.get("status") != filters["status"]:
                    continue
                if filters.get("priority") and filters["priority"] not in {"all", ""} and item.get("priority") != filters["priority"]:
                    continue
                if filters.get("opportunity_id") and str(item.get("opportunity_id")) != str(filters["opportunity_id"]):
                    continue
                if filters.get("pipeline_record_id") and str(item.get("pipeline_record_id")) != str(filters["pipeline_record_id"]):
                    continue
                item["comments"] = [
                    self._row(comment)
                    for comment in conn.execute(
                        """
                        SELECT tc.*, u.name AS user_name
                        FROM task_comments tc
                        LEFT JOIN users u ON u.id = tc.user_id
                        WHERE tc.task_id = ?
                        ORDER BY tc.created_at DESC
                        """,
                        (item["id"],),
                    ).fetchall()
                ]
                item["latest_comment"] = item["comments"][0]["body"] if item["comments"] else ""
                item["is_overdue"] = bool(
                    item.get("due_at")
                    and item["status"] not in {"done", "canceled"}
                    and parse_iso(item["due_at"])
                    and parse_iso(item["due_at"]).date() < utc_now().date()
                )
                if filters.get("overdue_only") == "1" and not item["is_overdue"]:
                    continue
                tasks.append(item)
            native_rows = conn.execute(
                """
                SELECT
                    prt.*,
                    pr.organization_id,
                    pr.record_code,
                    pr.stage,
                    pr.priority AS opportunity_priority,
                    pr.id AS pipeline_record_id,
                    org.name AS company_name,
                    u.name AS assignee_name
                FROM pipeline_record_tasks prt
                JOIN pipeline_records pr ON pr.id = prt.pipeline_record_id
                JOIN organizations org ON org.id = pr.organization_id
                LEFT JOIN users u ON u.id = prt.assigned_user_id
                WHERE pr.legacy_opportunity_id IS NULL
                ORDER BY
                    CASE prt.priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                    COALESCE(prt.due_at, '9999-12-31') ASC
                """
            ).fetchall()
            for row in native_rows:
                item = self._row(row)
                item["task_source"] = "pipeline_record"
                item["opportunity_id"] = None
                if filters.get("assigned_user_id") and str(item.get("assigned_user_id")) != str(filters["assigned_user_id"]):
                    continue
                if filters.get("status") and filters["status"] not in {"all", ""} and item.get("status") != filters["status"]:
                    continue
                if filters.get("priority") and filters["priority"] not in {"all", ""} and item.get("priority") != filters["priority"]:
                    continue
                if filters.get("opportunity_id"):
                    continue
                if filters.get("pipeline_record_id") and str(item.get("pipeline_record_id")) != str(filters["pipeline_record_id"]):
                    continue
                item["comments"] = self._fetch_pipeline_record_task_comments(conn, item["id"])
                item["latest_comment"] = item["comments"][-1]["body"] if item["comments"] else ""
                item["is_overdue"] = bool(
                    item.get("due_at")
                    and item["status"] not in {"done", "canceled"}
                    and parse_iso(item["due_at"])
                    and parse_iso(item["due_at"]).date() < utc_now().date()
                )
                if filters.get("overdue_only") == "1" and not item["is_overdue"]:
                    continue
                tasks.append(item)
            tasks.sort(
                key=lambda item: (
                    {"urgent": 0, "high": 1, "medium": 2}.get(item.get("priority"), 3),
                    item.get("due_at") or "9999-12-31",
                )
            )
            return tasks

    def create_task(self, payload: dict) -> dict:
        title = (payload.get("title") or "").strip()
        if not title:
            self._error("Task title is required", {"title": "This field is required"})
        self._validate_task_priority(payload.get("priority", "medium"))
        self._validate_task_status(payload.get("status", "todo"))
        with self.connect() as conn:
            opportunity_id = int(payload["opportunity_id"])
            self._base_opportunity(conn, opportunity_id)
            actor_id = int(payload.get("created_by_user_id") or 2)
            now = iso_now()
            task_id = conn.execute(
                """
                INSERT INTO tasks(
                    opportunity_id, title, description, status, priority, assigned_user_id,
                    created_by_user_id, due_at, is_blocking, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    opportunity_id,
                    title,
                    payload.get("description", ""),
                    payload.get("status", "todo"),
                    payload.get("priority", "medium"),
                    payload.get("assigned_user_id") or None,
                    actor_id,
                    payload.get("due_at") or None,
                    1 if payload.get("is_blocking") else 0,
                    now,
                    now,
                ),
            ).lastrowid
            self._log_activity(conn, opportunity_id, task_id, actor_id, "task_created", "task", task_id, f"Task created: {title}")
            row = conn.execute(
                """
                SELECT t.*, u.name AS assignee_name
                FROM tasks t LEFT JOIN users u ON u.id = t.assigned_user_id
                WHERE t.id = ?
                """,
                (task_id,),
            ).fetchone()
            task = self._row(row)
            task["comments"] = []
            self._audit(conn, "task", task_id, "create", actor_id, None, task)
            return task

    def update_task(self, task_id: int, payload: dict) -> dict:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if not row:
                native_row = conn.execute("SELECT * FROM pipeline_record_tasks WHERE id = ?", (task_id,)).fetchone()
                if not native_row:
                    raise NotFoundError("Task not found")
                before = self._row(native_row)
                actor_id = int(payload.get("updated_by_user_id") or before.get("created_by_user_id") or 2)
                status = payload.get("status", before["status"])
                priority = payload.get("priority", before["priority"])
                self._validate_task_status(status)
                self._validate_task_priority(priority)
                now = iso_now()
                completed_at = before.get("completed_at")
                if status == "done" and not completed_at:
                    completed_at = now
                elif status != "done":
                    completed_at = None
                conn.execute(
                    """
                    UPDATE pipeline_record_tasks
                    SET title = ?, description = ?, status = ?, priority = ?, assigned_user_id = ?, due_at = ?,
                        completed_at = ?, is_blocking = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        payload.get("title", before["title"]),
                        payload.get("description", before.get("description", "")),
                        status,
                        priority,
                        payload.get("assigned_user_id", before.get("assigned_user_id")),
                        payload.get("due_at", before.get("due_at")),
                        completed_at,
                        1 if payload.get("is_blocking", before.get("is_blocking")) else 0,
                        now,
                        task_id,
                    ),
                )
                after_row = conn.execute(
                    """
                    SELECT prt.*, u.name AS assignee_name
                    FROM pipeline_record_tasks prt
                    LEFT JOIN users u ON u.id = prt.assigned_user_id
                    WHERE prt.id = ?
                    """,
                    (task_id,),
                ).fetchone()
                after = self._row(after_row)
                after["comments"] = self._fetch_pipeline_record_task_comments(conn, task_id)
                after["latest_comment"] = after["comments"][-1]["body"] if after["comments"] else ""
                after["pipeline_record_id"] = before["pipeline_record_id"]
                after["legacy_opportunity_id"] = None
                after["opportunity_id"] = None
                after["task_source"] = "pipeline_record"
                after["is_overdue"] = bool(
                    after.get("due_at")
                    and after["status"] not in {"done", "canceled"}
                    and parse_iso(after["due_at"])
                    and parse_iso(after["due_at"]).date() < utc_now().date()
                )
                self._pipeline_record_activity(
                    conn,
                    before["pipeline_record_id"],
                    task_id,
                    actor_id,
                    "task_updated",
                    "pipeline_record_task",
                    task_id,
                    f"Task updated: {after['title']}",
                )
                self._audit(conn, "pipeline_record_task", task_id, "update", actor_id, before, after)
                return after
            before = self._row(row)
            actor_id = int(payload.get("updated_by_user_id") or before.get("created_by_user_id") or 2)
            status = payload.get("status", before["status"])
            priority = payload.get("priority", before["priority"])
            self._validate_task_status(status)
            self._validate_task_priority(priority)
            now = iso_now()
            completed_at = before.get("completed_at")
            if status == "done" and not completed_at:
                completed_at = now
            elif status != "done":
                completed_at = None
            conn.execute(
                """
                UPDATE tasks
                SET title = ?, description = ?, status = ?, priority = ?, assigned_user_id = ?, due_at = ?,
                    completed_at = ?, is_blocking = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    payload.get("title", before["title"]),
                    payload.get("description", before.get("description", "")),
                    status,
                    priority,
                    payload.get("assigned_user_id", before.get("assigned_user_id")),
                    payload.get("due_at", before.get("due_at")),
                    completed_at,
                    1 if payload.get("is_blocking", before.get("is_blocking")) else 0,
                    now,
                    task_id,
                ),
            )
            after_row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            after = self._row(after_row)
            self._log_activity(
                conn,
                before["opportunity_id"],
                task_id,
                actor_id,
                "task_updated",
                "task",
                task_id,
                f"Task updated: {after['title']}",
            )
            self._audit(conn, "task", task_id, "update", actor_id, before, after)
            return after

    def delete_task(self, task_id: int, actor_user_id: int) -> dict:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if not row:
                native_row = conn.execute("SELECT * FROM pipeline_record_tasks WHERE id = ?", (task_id,)).fetchone()
                if not native_row:
                    raise NotFoundError("Task not found")
                before = self._row(native_row)
                conn.execute("DELETE FROM pipeline_record_tasks WHERE id = ?", (task_id,))
                self._pipeline_record_activity(
                    conn,
                    before["pipeline_record_id"],
                    task_id,
                    actor_user_id,
                    "task_deleted",
                    "pipeline_record_task",
                    task_id,
                    f"Task deleted: {before['title']}",
                )
                self._audit(conn, "pipeline_record_task", task_id, "delete", actor_user_id, before, None)
                return {"success": True}
            before = self._row(row)
            conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            self._log_activity(conn, before["opportunity_id"], task_id, actor_user_id, "task_deleted", "task", task_id, f"Task deleted: {before['title']}")
            self._audit(conn, "task", task_id, "delete", actor_user_id, before, None)
            return {"success": True}

    def add_task_comment(self, task_id: int, payload: dict) -> dict:
        body = (payload.get("body") or "").strip()
        if not body:
            self._error("Comment body is required", {"body": "This field is required"})
        with self.connect() as conn:
            task = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if not task:
                native_task = conn.execute("SELECT * FROM pipeline_record_tasks WHERE id = ?", (task_id,)).fetchone()
                if not native_task:
                    raise NotFoundError("Task not found")
                actor_id = int(payload.get("user_id") or 2)
                now = iso_now()
                comment_id = conn.execute(
                    """
                    INSERT INTO pipeline_record_task_comments(pipeline_record_task_id, user_id, body, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (task_id, actor_id, body, now),
                ).lastrowid
                self._pipeline_record_activity(
                    conn,
                    native_task["pipeline_record_id"],
                    task_id,
                    actor_id,
                    "comment_added",
                    "pipeline_record_task_comment",
                    comment_id,
                    "Task comment added.",
                )
                row = conn.execute(
                    """
                    SELECT prtc.*, u.name AS user_name
                    FROM pipeline_record_task_comments prtc
                    LEFT JOIN users u ON u.id = prtc.user_id
                    WHERE prtc.id = ?
                    """,
                    (comment_id,),
                ).fetchone()
                comment = self._row(row)
                comment["pipeline_record_id"] = native_task["pipeline_record_id"]
                comment["legacy_opportunity_id"] = None
                return comment
            actor_id = int(payload.get("user_id") or 2)
            now = iso_now()
            comment_id = conn.execute(
                "INSERT INTO task_comments(task_id, user_id, body, created_at) VALUES (?, ?, ?, ?)",
                (task_id, actor_id, body, now),
            ).lastrowid
            self._log_activity(conn, task["opportunity_id"], task_id, actor_id, "comment_added", "task_comment", comment_id, "Task comment added.")
            row = conn.execute(
                """
                SELECT tc.*, u.name AS user_name
                FROM task_comments tc LEFT JOIN users u ON u.id = tc.user_id
                WHERE tc.id = ?
                """,
                (comment_id,),
            ).fetchone()
            return self._row(row)

    def add_document(self, opportunity_id: int, payload: dict) -> dict:
        label = (payload.get("file_name") or payload.get("label") or "").strip()
        path = (payload.get("storage_path") or payload.get("url") or "").strip()
        if not label:
            self._error("Document name is required", {"file_name": "This field is required"})
        if not path:
            self._error("Document path or URL is required", {"storage_path": "This field is required"})
        with self.connect() as conn:
            self._base_opportunity(conn, opportunity_id)
            actor_id = int(payload.get("uploaded_by_user_id") or 2)
            now = iso_now()
            document_id = conn.execute(
                """
                INSERT INTO documents(opportunity_id, file_name, document_category, storage_path, uploaded_by_user_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    opportunity_id,
                    label,
                    payload.get("document_category", "other"),
                    path,
                    actor_id,
                    now,
                ),
            ).lastrowid
            self._log_activity(conn, opportunity_id, None, actor_id, "document_added", "document", document_id, f"Document added: {label}")
            row = conn.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()
            self._audit(conn, "document", document_id, "create", actor_id, None, self._row(row))
            return self._row(row)

    def add_decision(self, opportunity_id: int, payload: dict) -> dict:
        decision_type = payload.get("decision_type")
        summary = (payload.get("decision_summary") or "").strip()
        if not decision_type or not summary:
            self._error("Decision type and summary are required", {"decision_type": "Required", "decision_summary": "Required"})
        with self.connect() as conn:
            self._base_opportunity(conn, opportunity_id)
            actor_id = int(payload.get("decided_by_user_id") or 1)
            now = iso_now()
            decision_id = conn.execute(
                """
                INSERT INTO decision_logs(opportunity_id, decision_type, decision_summary, rationale, decided_by_user_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (opportunity_id, decision_type, summary, payload.get("rationale", ""), actor_id, now),
            ).lastrowid
            self._log_activity(conn, opportunity_id, None, actor_id, "decision_logged", "decision", decision_id, f"Decision logged: {summary}")
            row = conn.execute(
                """
                SELECT dl.*, u.name AS decided_by_name
                FROM decision_logs dl LEFT JOIN users u ON u.id = dl.decided_by_user_id
                WHERE dl.id = ?
                """,
                (decision_id,),
            ).fetchone()
            self._audit(conn, "decision", decision_id, "create", actor_id, None, self._row(row))
            return self._row(row)

    def list_companies(self) -> List[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    c.*,
                    COUNT(o.id) AS total_opportunities,
                    MAX(o.updated_at) AS last_activity_at
                FROM companies c
                LEFT JOIN opportunities o ON o.company_id = c.id AND o.deleted_at IS NULL
                GROUP BY c.id
                ORDER BY last_activity_at DESC, c.name ASC
                """
            ).fetchall()
            result = []
            for row in rows:
                item = self._row(row)
                latest = conn.execute(
                    """
                    SELECT o.stage, u.name AS owner_name
                    FROM opportunities o
                    LEFT JOIN users u ON u.id = o.owner_user_id
                    WHERE o.company_id = ? AND o.deleted_at IS NULL
                    ORDER BY o.updated_at DESC
                    LIMIT 1
                    """,
                    (item["id"],),
                ).fetchone()
                item["current_stage"] = latest["stage"] if latest else ""
                item["owner_name"] = latest["owner_name"] if latest else ""
                result.append(item)
            return result

    def _pipeline_record_stage_label(self, stage: Optional[str]) -> str:
        return (stage or "").replace("_", " ").title()

    def _pipeline_record_missing_fields(self, record: dict) -> List[str]:
        missing = []
        if not record.get("owner_user_id"):
            missing.append("owner_user_id")
        if not record.get("primary_contact_name"):
            missing.append("primary_contact_name")
        if not record.get("source_detail"):
            missing.append("source_detail")
        if not record.get("next_step"):
            missing.append("next_step")
        if not record.get("investment_thesis"):
            missing.append("investment_thesis")
        if record.get("stage") in {"screening", "qualified", "diligence", "ic_preparation", "ic_decision", "closing"}:
            if not record.get("round_name"):
                missing.append("round_name")
            if not record.get("ticket_size_target"):
                missing.append("ticket_size_target")
            if not record.get("valuation_max"):
                missing.append("valuation_max")
        if record.get("stage") in {"qualified", "diligence", "ic_preparation", "ic_decision", "closing"} and not record.get("key_concerns"):
            missing.append("key_concerns")
        if record.get("stage") in {"diligence", "ic_preparation", "ic_decision", "closing"} and not record.get("financials_updated_at"):
            missing.append("financials_updated_at")
        if record.get("nda_required") and record.get("nda_status") != "signed":
            missing.append("nda_status")
        return missing

    def _active_canonical_pipeline_records(self) -> List[dict]:
        inactive_stages = {"invested", "passed", "archived"}
        return [
            item
            for item in self.list_pipeline_records()
            if not item.get("legacy_opportunity_id") and item.get("stage") not in inactive_stages
        ]

    def _canonical_reporting_item(self, record: dict) -> dict:
        return {
            "id": record["id"],
            "pipeline_record_id": record["id"],
            "company_name": record.get("organization_name") or "",
            "stage": record.get("stage"),
            "stage_label": self._pipeline_record_stage_label(record.get("stage")),
            "owner_name": record.get("owner_name"),
            "owner_user_id": record.get("owner_user_id"),
            "next_step": record.get("next_step"),
            "missing_fields": self._pipeline_record_missing_fields(record),
            "last_activity_at": record.get("updated_at"),
            "updated_at": record.get("updated_at"),
            "priority": record.get("priority"),
            "record_code": record.get("record_code"),
        }

    def get_dashboard(self) -> dict:
        opportunities = self.list_opportunities()
        tasks = self.list_tasks()
        canonical_active = self._active_canonical_pipeline_records()
        stage_counts = []
        for stage in STAGES:
            count = len([item for item in opportunities if item["stage"] == stage["id"] and item["status"] in {"active", "on_hold", "closed_won", "closed_lost"}])
            if stage["id"] in ACTIVE_STAGES or count:
                stage_counts.append({"stage": stage["id"], "label": stage["label"], "count": count})
        active = [item for item in opportunities if item["stage"] in ACTIVE_STAGES]
        overdue_tasks = [task for task in tasks if task["is_overdue"]]
        missing = [item for item in opportunities if item["missing_fields"]]
        missing += [self._canonical_reporting_item(item) for item in canonical_active if self._pipeline_record_missing_fields(item)]
        decision_queue = [
            item for item in opportunities
            if item["stage"] in DECISION_STAGES and ("decision_needed_this_week" in item["workflow_flags"] or item["priority"] in {"high", "critical"})
        ]
        decision_queue += [
            self._canonical_reporting_item(item)
            for item in canonical_active
            if item.get("stage") in {"qualified", "diligence", "ic_preparation", "ic_decision", "closing"}
            and (item.get("decision_outcome") in {None, "", "pending"} or item.get("priority") in {"high", "critical"})
        ]
        stale = [
            item for item in opportunities
            if item.get("last_activity_at") and parse_iso(item["last_activity_at"]) and (utc_now() - parse_iso(item["last_activity_at"])).days >= 7
        ]
        stale += [
            self._canonical_reporting_item(item)
            for item in canonical_active
            if item.get("updated_at") and parse_iso(item["updated_at"]) and (utc_now() - parse_iso(item["updated_at"])).days >= 7
        ]
        with self.connect() as conn:
            activity_rows = conn.execute(
                """
                SELECT a.*, u.name AS user_name, c.name AS company_name
                FROM activities a
                LEFT JOIN users u ON u.id = a.user_id
                LEFT JOIN opportunities o ON o.id = a.opportunity_id
                LEFT JOIN companies c ON c.id = o.company_id
                ORDER BY a.created_at DESC
                LIMIT 10
                """
            ).fetchall()
            recent_activity = [self._row(row) for row in activity_rows]

        workload = self._workload_snapshot(opportunities, tasks)
        return {
            "summary": {
                "active_deals": len(active),
                "qualified_plus": len([item for item in active if STAGE_RANKS[item["stage"]] >= STAGE_RANKS["qualified"]]),
                "ic_review_this_week": len([item for item in active if item["stage"] == "ic_review"]),
                "overdue_tasks": len(overdue_tasks),
                "missing_data": len(missing),
                "deals_without_owner": len([item for item in active if not item.get("owner_user_id")]),
            },
            "stage_counts": stage_counts,
            "decision_queue": decision_queue[:6],
            "stale_deals": stale[:6],
            "overdue_tasks": overdue_tasks[:8],
            "missing_information": missing[:6],
            "recent_activity": recent_activity,
            "workload": workload,
        }

    def _workload_snapshot(self, opportunities: List[dict], tasks: List[dict]) -> List[dict]:
        users = {user["id"]: {**user, "active_deals": 0, "open_tasks": 0, "overdue_tasks": 0, "critical_deals": 0} for user in self.list_users()}
        for item in opportunities:
            owner_id = item.get("owner_user_id")
            if owner_id in users and item["stage"] in ACTIVE_STAGES:
                users[owner_id]["active_deals"] += 1
                if item["priority"] in {"high", "critical"}:
                    users[owner_id]["critical_deals"] += 1
        for task in tasks:
            assignee = task.get("assigned_user_id")
            if assignee in users and task["status"] not in {"done", "canceled"}:
                users[assignee]["open_tasks"] += 1
                if task["is_overdue"]:
                    users[assignee]["overdue_tasks"] += 1
        result = list(users.values())
        for row in result:
            score = row["active_deals"] + row["open_tasks"] + (row["critical_deals"] * 2) + (row["overdue_tasks"] * 2)
            row["load_score"] = score
            if score >= 10:
                row["load_flag"] = "overloaded"
            elif score <= 2:
                row["load_flag"] = "underutilized"
            else:
                row["load_flag"] = "balanced"
        result.sort(key=lambda item: item["load_score"], reverse=True)
        return result

    def get_reflection_report(self) -> dict:
        opportunities = self.list_opportunities()
        tasks = self.list_tasks()
        canonical_active = self._active_canonical_pipeline_records()
        active = [item for item in opportunities if item["stage"] in ACTIVE_STAGES]
        new_this_week = [item for item in opportunities if parse_iso(item["created_at"]) and (utc_now() - parse_iso(item["created_at"])).days <= 7]
        new_this_week += [
            item for item in canonical_active
            if item.get("created_at") and parse_iso(item["created_at"]) and (utc_now() - parse_iso(item["created_at"])).days <= 7
        ]
        advanced_this_week = 0
        dropped_this_week = len([item for item in opportunities if item["stage"] == "closed_lost" and parse_iso(item["updated_at"]) and (utc_now() - parse_iso(item["updated_at"])).days <= 7])
        with self.connect() as conn:
            advanced_this_week = conn.execute(
                """
                SELECT COUNT(*) FROM stage_history
                WHERE from_stage IS NOT NULL AND created_at >= date('now', '-7 day')
                """
            ).fetchone()[0]
        overdue_tasks = [task for task in tasks if task["is_overdue"]]
        stale_deals = [
            item for item in active
            if item.get("last_activity_at") and parse_iso(item["last_activity_at"]) and (utc_now() - parse_iso(item["last_activity_at"])).days >= 7
        ]
        stale_deals += [
            self._canonical_reporting_item(item)
            for item in canonical_active
            if item.get("updated_at") and parse_iso(item["updated_at"]) and (utc_now() - parse_iso(item["updated_at"])).days >= 7
        ]
        deals_requiring_decision = [
            item for item in active if item["stage"] in DECISION_STAGES and ("decision_needed_this_week" in item["workflow_flags"] or item["missing_fields"])
        ]
        deals_requiring_decision += [
            self._canonical_reporting_item(item)
            for item in canonical_active
            if item.get("stage") in {"qualified", "diligence", "ic_preparation", "ic_decision", "closing"}
            and (item.get("decision_outcome") in {None, "", "pending"} or self._pipeline_record_missing_fields(item))
        ]
        missing_information = [item for item in active if item["missing_fields"]]
        missing_information += [self._canonical_reporting_item(item) for item in canonical_active if self._pipeline_record_missing_fields(item)]
        workload = self._workload_snapshot(opportunities, tasks)
        avg_stage_days = round(
            sum((utc_now() - parse_iso(item["stage_entered_at"])).days for item in active if parse_iso(item["stage_entered_at"])) / max(1, len(active)),
            1,
        )
        stage_distribution = [
            {
                "stage": stage["id"],
                "label": stage["label"],
                "count": len([item for item in active if item["stage"] == stage["id"]]),
                "target_ticket_total": sum(item.get("ticket_size_target") or 0 for item in active if item["stage"] == stage["id"]),
            }
            for stage in STAGES
            if stage["id"] in ACTIVE_STAGES
        ]
        biggest_bottleneck = max(stage_distribution, key=lambda item: item["count"]) if stage_distribution else None
        return {
            "executive_summary": {
                "active_opportunities": len(active),
                "new_this_week": len(new_this_week),
                "advanced_this_week": advanced_this_week,
                "dropped_this_week": dropped_this_week,
                "deals_awaiting_decision": len(deals_requiring_decision),
                "overdue_tasks": len(overdue_tasks),
                "stale_opportunities": len(stale_deals),
                "avg_days_in_stage": avg_stage_days,
                "biggest_bottleneck": biggest_bottleneck["label"] if biggest_bottleneck else "No bottleneck",
                "execution_risk": "High" if len(overdue_tasks) >= 3 else "Moderate" if overdue_tasks else "Low",
                "recommended_action": "Use Friday review to clear decision-ready deals and close missing-input gaps.",
            },
            "pipeline_health": stage_distribution,
            "bottlenecks": stale_deals[:10],
            "missing_information": missing_information[:10],
            "analyst_workload": workload,
            "overdue_tasks": overdue_tasks[:12],
            "deals_requiring_decision": deals_requiring_decision[:12],
        }

    def export_reflection_report_markdown(self) -> str:
        report = self.get_reflection_report()
        lines = [
            "# VCC Pipeline Reflection Report",
            "",
            "## Executive Summary",
        ]
        for key, value in report["executive_summary"].items():
            label = key.replace("_", " ").title()
            lines.append(f"- {label}: {value}")
        lines.extend(["", "## Deals Requiring Decision"])
        for deal in report["deals_requiring_decision"]:
            lines.append(
                f"- {deal['company_name']} | {deal['stage_label']} | {deal['owner_name'] or 'Unassigned'} | Missing: {', '.join(deal['missing_fields']) or 'none'}"
            )
        lines.extend(["", "## Overdue Tasks"])
        for task in report["overdue_tasks"]:
            lines.append(
                f"- {task['title']} | {task['company_name']} | {task['assignee_name'] or 'Unassigned'} | due {task['due_at'] or 'TBD'}"
            )
        return "\n".join(lines)

    def export_pipeline_csv(self) -> str:
        opportunities = self.list_opportunities()
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow([
            "Company",
            "Stage",
            "Priority",
            "Owner",
            "Sector",
            "Geography",
            "Round",
            "Ticket Target",
            "Valuation Max",
            "Ownership Target",
            "Next Step",
            "Next Step Due",
            "Open Tasks",
            "Missing Fields",
            "Last Activity",
        ])
        for item in opportunities:
            writer.writerow([
                item["company_name"],
                item["stage_label"],
                item["priority_label"],
                item.get("owner_name") or "",
                item.get("sector") or "",
                item.get("geography") or "",
                item.get("round_name") or "",
                item.get("ticket_size_target") or "",
                item.get("valuation_max") or "",
                item.get("ownership_target_pct") or "",
                item.get("next_step") or "",
                item.get("next_step_due_at") or "",
                item.get("open_tasks") or 0,
                ", ".join(item.get("missing_fields") or []),
                item.get("last_activity_at") or "",
            ])
        return buffer.getvalue()

    def export_opportunity_report_markdown(self, opportunity_id: int) -> str:
        item = self.get_opportunity(opportunity_id)
        lines = [
            f"# {item['company_name']} Investment Note",
            "",
            f"- Stage: {item['stage_label']}",
            f"- Priority: {item['priority_label']} ({item['priority_score']})",
            f"- Owner: {item.get('owner_name') or 'Unassigned'}",
            f"- Round: {item.get('round_name') or 'Unknown'}",
            f"- Ticket Target: {item.get('ticket_size_target') or 'TBD'}",
            f"- Ownership Target: {item.get('ownership_target_pct') or 'TBD'}",
            "",
            "## Thesis",
            item.get("investment_thesis") or "Not yet written.",
            "",
            "## Key Concerns",
            item.get("key_concerns") or "Not yet written.",
            "",
            "## Missing Fields",
        ]
        if item["missing_fields"]:
            lines.extend([f"- {field}" for field in item["missing_fields"]])
        else:
            lines.append("- None")
        lines.extend(["", "## Decision History"])
        if item["decision_logs"]:
            for decision in item["decision_logs"]:
                lines.append(f"- {decision['created_at']}: {decision['decision_summary']} ({decision['decision_type']})")
        else:
            lines.append("- No decisions logged yet.")
        return "\n".join(lines)
