const state = {
  bootstrap: null,
  currentUserId: 2,
  route: "dashboard",
  opportunities: [],
  kanban: {},
  dashboard: null,
  tasks: [],
  companies: [],
  organizations: [],
  pipelineRecords: [],
  intakeSubmissions: [],
  relationshipLinks: [],
  report: null,
  selectedOpportunity: null,
  selectedOrganizationId: null,
  selectedPipelineRecordId: null,
  loading: false,
  error: "",
  banner: null,
  viewMode: "table",
  editingOpportunity: false,
  pipelineFilters: {
    q: "",
    stage: "all",
    status: "all",
    owner_user_id: "all",
    overdue_only: "0",
    missing_info: "0",
    sort: "updated_at",
    order: "desc",
  },
  taskFilters: {
    assigned_user_id: "all",
    status: "all",
    priority: "all",
    overdue_only: "0",
  },
  opportunityForm: null,
  opportunityFormErrors: {},
  opportunitySourceFiles: {
    deck: [],
    financials: [],
    other: [],
  },
  opportunityPrefillPending: false,
  intakeSection: "materials",
  toasts: [],
  modal: null,
};

const pageTitle = document.querySelector("#pageTitle");
const pageEyebrow = document.querySelector("#pageEyebrow");
const app = document.querySelector("#app");
const bannerHost = document.querySelector("#bannerHost");
const toastHost = document.querySelector("#toastHost");
const modalHost = document.querySelector("#modalHost");
const currentUserSelect = document.querySelector("#currentUserSelect");

const opportunitySections = [
  { id: "materials", label: "Materials" },
  { id: "company", label: "Company" },
  { id: "contacts", label: "Contacts" },
  { id: "deal", label: "Deal" },
  { id: "fit", label: "Investment Fit" },
  { id: "risks", label: "Risks / Missing Info" },
  { id: "review", label: "Review" },
];

function setBanner(type, message) {
  state.banner = { type, message };
  renderBanner();
}

function clearBanner() {
  state.banner = null;
  renderBanner();
}

function toast(message, type = "success") {
  const id = `${Date.now()}-${Math.random()}`;
  state.toasts.push({ id, message, type });
  renderToasts();
  setTimeout(() => {
    state.toasts = state.toasts.filter((item) => item.id !== id);
    renderToasts();
  }, 3000);
}

function openModal(modal) {
  state.modal = modal;
  renderModal();
}

function closeModal() {
  state.modal = null;
  renderModal();
}

function renderToasts() {
  toastHost.innerHTML = state.toasts
    .map((item) => `<div class="toast ${item.type}">${item.message}</div>`)
    .join("");
}

function renderBanner() {
  if (!state.banner) {
    bannerHost.innerHTML = "";
    return;
  }
  bannerHost.innerHTML = `<div class="banner ${state.banner.type}">${state.banner.message}</div>`;
}

function titleCaseStatus(status) {
  return String(status || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function normalizeText(value) {
  return String(value || "").trim().toLowerCase();
}

function compareValues(left, right, direction = "asc") {
  const multiplier = direction === "desc" ? -1 : 1;
  if (left === right) return 0;
  if (left === null || left === undefined || left === "") return 1 * multiplier;
  if (right === null || right === undefined || right === "") return -1 * multiplier;
  return left > right ? 1 * multiplier : -1 * multiplier;
}

function isClosedPipelineRecord(record) {
  const value = normalizeText(record.status || record.stage);
  return ["closed", "closed_won", "closed_lost", "rejected", "passed", "archived"].includes(value);
}

function pipelineRecordMissingCount(record) {
  return [
    record.source_type,
    record.next_step,
    record.investment_thesis,
    record.key_concerns,
    record.decision_outcome,
  ].filter((value) => !String(value || "").trim()).length;
}

function relatedPipelineRecords(organizationId) {
  return state.pipelineRecords.filter((item) => String(item.organization_id) === String(organizationId));
}

function linkedOrganizationForCompany(company) {
  return state.organizations.find((item) => String(item.legacy_company_id) === String(company?.id)) || null;
}

function sortRecordsByRecentActivity(records) {
  return [...records].sort((left, right) => {
    const leftValue = String(left.last_activity_at || left.updated_at || left.created_at || "");
    const rightValue = String(right.last_activity_at || right.updated_at || right.created_at || "");
    return compareValues(leftValue, rightValue, "desc");
  });
}

function companyDetailContext(companyId) {
  const company = state.companies.find((item) => String(item.id) === String(companyId)) || null;
  if (!company) return null;
  const organization = linkedOrganizationForCompany(company);
  const linkedRecords = organization ? sortRecordsByRecentActivity(relatedPipelineRecords(organization.id)) : [];
  const linkedDocuments = linkedRecords.flatMap((item) => pipelineRecordDocuments(item));
  const activeRecord = linkedRecords[0] || null;
  return { company, organization, linkedRecords, linkedDocuments, activeRecord };
}

function companyDisplayName(context) {
  return context?.organization?.name || context?.activeRecord?.organization_name || context?.company?.name || "Company";
}

function selectedOrganization() {
  return state.organizations.find((item) => String(item.id) === String(state.selectedOrganizationId)) || null;
}

function selectedPipelineRecord(records = state.pipelineRecords) {
  return records.find((item) => String(item.id) === String(state.selectedPipelineRecordId)) || null;
}

function upsertPipelineRecord(record) {
  const index = state.pipelineRecords.findIndex((item) => String(item.id) === String(record.id));
  if (index >= 0) {
    state.pipelineRecords[index] = { ...state.pipelineRecords[index], ...record };
  } else {
    state.pipelineRecords.unshift(record);
  }
}

function pipelineRecordLegacyWorkflowLabel(record) {
  if (!record?.legacy_opportunity_id) {
    return "Canonical record only. Notes, tasks, documents, and decisions are stored directly on this pipeline record.";
  }
  return `Legacy workflow available on deal ${record.legacy_opportunity_id}. Notes, tasks, documents, and decisions still land there for now.`;
}

function openEntityActionAttributes(item) {
  if (item?.pipeline_record_id) return `data-open-pipeline-record="${item.pipeline_record_id}"`;
  if (item?.opportunity_id) return `data-open-opportunity="${item.opportunity_id}"`;
  if (item?.id && item?.stage) return `data-open-opportunity="${item.id}"`;
  return "";
}

function openEntityActionLabel(item) {
  return item?.pipeline_record_id ? "Open record" : "Open deal";
}

function userOptionsHtml(selectedValue, includeEmpty = true) {
  const options = state.bootstrap.users.map((user) => `<option value="${user.id}" ${String(selectedValue || "") === String(user.id) ? "selected" : ""}>${user.name}</option>`);
  if (includeEmpty) {
    options.unshift(`<option value="" ${selectedValue ? "" : "selected"}>Unassigned</option>`);
  }
  return options.join("");
}

function renderCompanyEditForm(context) {
  const { company, organization, activeRecord, linkedDocuments } = context;
  const companyName = companyDisplayName(context);
  const deckCount = filterDocumentsByCategory(linkedDocuments, ["deck"]).length;
  const financialCount = filterDocumentsByCategory(linkedDocuments, ["financials"]).length;
  const supportCount = Math.max(linkedDocuments.length - deckCount - financialCount, 0);
  return `
    <div class="company-edit-layout">
      <div class="detail-card">
        <div class="panel-header">
          <div>
            <h4>Canonical organization</h4>
            <p class="muted">These fields belong to the shared company identity and refresh every linked workflow view.</p>
          </div>
          <span class="pill">${organization ? `Org ${organization.id}` : "No org linked"}</span>
        </div>
        ${
          organization
            ? `
              <div class="form-grid">
                <label>
                  Company name
                  <input id="companyEditOrganizationName" value="${organization.name || company.name || ""}" />
                </label>
                <label>
                  Owner
                  <select id="companyEditOrganizationOwner">
                    ${userOptionsHtml(organization.owner_user_id)}
                  </select>
                </label>
                <label>
                  Website
                  <input id="companyEditOrganizationWebsite" value="${organization.website || activeRecord?.website || ""}" placeholder="https://company.com" />
                </label>
                <label>
                  Sector
                  <input id="companyEditOrganizationSector" value="${organization.sector_primary || company.sector || ""}" placeholder="Climate software" />
                </label>
                <label>
                  Subsector
                  <input id="companyEditOrganizationSubsector" value="${organization.subsector || ""}" placeholder="Grid analytics" />
                </label>
                <label>
                  Geography
                  <input id="companyEditOrganizationGeography" value="${organization.geography || company.geography || ""}" placeholder="Benelux" />
                </label>
                <label>
                  HQ city
                  <input id="companyEditOrganizationHqCity" value="${organization.hq_city || activeRecord?.hq_city || ""}" placeholder="Brussels" />
                </label>
                <label>
                  HQ country
                  <input id="companyEditOrganizationHqCountry" value="${organization.hq_country || activeRecord?.hq_country || ""}" placeholder="Belgium" />
                </label>
                <label>
                  Business model
                  <input id="companyEditOrganizationBusinessModel" value="${organization.business_model || ""}" placeholder="B2B SaaS" />
                </label>
                <label class="full">
                  Description
                  <textarea id="companyEditOrganizationDescription" rows="5" placeholder="Short company overview for the canonical organization.">${organization.description || activeRecord?.company_description || ""}</textarea>
                </label>
              </div>
            `
            : `
              <p class="muted">This company does not have a linked canonical organization yet, so overview edits cannot be saved from the Companies tab until that link exists.</p>
            `
        }
      </div>
      <div class="detail-card">
        <div class="panel-header">
          <div>
            <h4>Active pipeline record</h4>
            <p class="muted">These fields are the canonical evaluation snapshot sourced from the latest shared materials and the current deal team view.</p>
          </div>
          <span class="pill">${activeRecord ? activeRecord.record_code || `Record ${activeRecord.id}` : "No active record"}</span>
        </div>
        ${
          activeRecord
            ? `
              <div class="form-grid">
                <label>
                  Record owner
                  <select id="companyEditRecordOwner">
                    ${userOptionsHtml(activeRecord.owner_user_id)}
                  </select>
                </label>
                <label>
                  Fund fit
                  <input id="companyEditRecordFundFit" value="${activeRecord.fund_fit || ""}" placeholder="Growth / Precise / Both" />
                </label>
                <label>
                  Round
                  <input id="companyEditRecordRoundName" value="${activeRecord.round_name || ""}" placeholder="Series A" />
                </label>
                <label>
                  Source detail
                  <input id="companyEditRecordSourceDetail" value="${activeRecord.source_detail || ""}" placeholder="Why is this in the pipeline?" />
                </label>
                <label>
                  Ticket target
                  <input id="companyEditRecordTicketTarget" value="${activeRecord.ticket_size_target || ""}" placeholder="2500000" />
                </label>
                <label>
                  Ownership target %
                  <input id="companyEditRecordOwnershipTarget" value="${activeRecord.ownership_target_pct || ""}" placeholder="10" />
                </label>
                <label>
                  Valuation min
                  <input id="companyEditRecordValuationMin" value="${activeRecord.valuation_min || ""}" placeholder="15000000" />
                </label>
                <label>
                  Valuation max
                  <input id="companyEditRecordValuationMax" value="${activeRecord.valuation_max || ""}" placeholder="25000000" />
                </label>
                <label>
                  ARR
                  <input id="companyEditRecordRevenueArr" type="number" step="0.01" value="${inputValue(firstPresentValue(activeRecord.annual_recurring_revenue, activeRecord.arr, activeRecord.revenue, activeRecord.revenue_run_rate))}" placeholder="2500000" />
                </label>
                <label>
                  Growth %
                  <input id="companyEditRecordRevenueGrowth" type="number" step="0.1" value="${inputValue(firstPresentValue(activeRecord.revenue_growth_pct, activeRecord.growth_pct))}" placeholder="35" />
                </label>
                <label>
                  Gross margin %
                  <input id="companyEditRecordGrossMargin" type="number" step="0.1" value="${inputValue(activeRecord.gross_margin_pct)}" placeholder="72" />
                </label>
                <label>
                  EBITDA margin %
                  <input id="companyEditRecordEbitdaMargin" type="number" step="0.1" value="${inputValue(firstPresentValue(activeRecord.ebitda_margin_pct, activeRecord.ebit_margin_pct))}" placeholder="18" />
                </label>
                <label>
                  Rule of 40 %
                  <input id="companyEditRecordRuleOf40" type="number" step="0.1" value="${inputValue(firstPresentValue(activeRecord.rule_of_40_pct, activeRecord.rule_of_40))}" placeholder="53" />
                </label>
                <label>
                  Monthly burn
                  <input id="companyEditRecordMonthlyBurn" type="number" step="0.01" value="${inputValue(firstPresentValue(activeRecord.monthly_burn, activeRecord.burn_rate))}" placeholder="150000" />
                </label>
                <label>
                  Cash runway (months)
                  <input id="companyEditRecordCashRunway" type="number" step="0.1" value="${inputValue(activeRecord.cash_runway_months)}" placeholder="14" />
                </label>
                <label>
                  Financials updated
                  <input id="companyEditRecordFinancialsUpdatedAt" type="date" value="${activeRecord.financials_updated_at || activeRecord.financials_last_updated_at || ""}" />
                </label>
                <label>
                  Primary contact
                  <input id="companyEditRecordPrimaryContactName" value="${activeRecord.primary_contact_name || activeRecord.primary_contact?.name || ""}" placeholder="Founder / CFO" />
                </label>
                <label>
                  Contact title
                  <input id="companyEditRecordPrimaryContactTitle" value="${activeRecord.primary_contact_title || activeRecord.primary_contact?.title || ""}" placeholder="CEO" />
                </label>
                <label>
                  Contact email
                  <input id="companyEditRecordPrimaryContactEmail" value="${activeRecord.primary_contact_email || activeRecord.primary_contact?.email || ""}" placeholder="name@company.com" />
                </label>
                <label>
                  Contact phone
                  <input id="companyEditRecordPrimaryContactPhone" value="${activeRecord.primary_contact_phone || activeRecord.primary_contact?.phone || ""}" placeholder="+32 ..." />
                </label>
                <label class="full">
                  Investment thesis
                  <textarea id="companyEditRecordInvestmentThesis" rows="4" placeholder="Why should this company stay active in the funnel?">${activeRecord.investment_thesis || ""}</textarea>
                </label>
                <label class="full">
                  Key concerns
                  <textarea id="companyEditRecordKeyConcerns" rows="4" placeholder="What is still unproven in the current materials?">${activeRecord.key_concerns || ""}</textarea>
                </label>
              </div>
            `
            : `
              <p class="muted">No linked pipeline record is active yet, so material-derived metrics and investment fields cannot be updated here.</p>
            `
        }
      </div>
      <div class="detail-card">
        <h4>Materials context</h4>
        <p class="muted">${companyName} currently has ${linkedDocuments.length} shared material${linkedDocuments.length === 1 ? "" : "s"} linked across canonical records. Use the pipeline record page to upload or replace files; this editor is for the canonical fields those materials inform.</p>
        <div class="list-stack">
          <div class="summary-block">
            <strong>Decks</strong>
            <p class="muted">${deckCount} linked</p>
          </div>
          <div class="summary-block">
            <strong>Financials</strong>
            <p class="muted">${financialCount} linked</p>
          </div>
          <div class="summary-block">
            <strong>Supporting files</strong>
            <p class="muted">${supportCount} linked</p>
          </div>
        </div>
      </div>
    </div>
  `;
}

async function saveCompanyEdit(companyId) {
  const context = companyDetailContext(companyId);
  if (!context) throw new Error("Company not found");
  const updates = [];
  if (context.organization) {
    const organizationPayload = compactPayload({
      actor_user_id: state.currentUserId,
      name: document.querySelector("#companyEditOrganizationName")?.value.trim(),
      owner_user_id: document.querySelector("#companyEditOrganizationOwner")?.value || "",
      website: document.querySelector("#companyEditOrganizationWebsite")?.value.trim(),
      sector_primary: document.querySelector("#companyEditOrganizationSector")?.value.trim(),
      subsector: document.querySelector("#companyEditOrganizationSubsector")?.value.trim(),
      geography: document.querySelector("#companyEditOrganizationGeography")?.value.trim(),
      hq_city: document.querySelector("#companyEditOrganizationHqCity")?.value.trim(),
      hq_country: document.querySelector("#companyEditOrganizationHqCountry")?.value.trim(),
      business_model: document.querySelector("#companyEditOrganizationBusinessModel")?.value.trim(),
      description: document.querySelector("#companyEditOrganizationDescription")?.value.trim(),
    });
    if (Object.keys(organizationPayload).length > 1) {
      updates.push(request(`/api/v1/organizations/${context.organization.id}`, {
        method: "PATCH",
        body: JSON.stringify(organizationPayload),
      }));
    }
  }
  if (context.activeRecord) {
    const recordPayload = compactPayload({
      actor_user_id: state.currentUserId,
      owner_user_id: document.querySelector("#companyEditRecordOwner")?.value || "",
      fund_fit: document.querySelector("#companyEditRecordFundFit")?.value.trim(),
      source_detail: document.querySelector("#companyEditRecordSourceDetail")?.value.trim(),
      round_name: document.querySelector("#companyEditRecordRoundName")?.value.trim(),
      ticket_size_target: document.querySelector("#companyEditRecordTicketTarget")?.value.trim(),
      ownership_target_pct: document.querySelector("#companyEditRecordOwnershipTarget")?.value.trim(),
      valuation_min: document.querySelector("#companyEditRecordValuationMin")?.value.trim(),
      valuation_max: document.querySelector("#companyEditRecordValuationMax")?.value.trim(),
      annual_recurring_revenue: readNumberInputValue("#companyEditRecordRevenueArr"),
      revenue_growth_pct: readNumberInputValue("#companyEditRecordRevenueGrowth"),
      gross_margin_pct: readNumberInputValue("#companyEditRecordGrossMargin"),
      ebitda_margin_pct: readNumberInputValue("#companyEditRecordEbitdaMargin"),
      rule_of_40_pct: readNumberInputValue("#companyEditRecordRuleOf40"),
      monthly_burn: readNumberInputValue("#companyEditRecordMonthlyBurn"),
      cash_runway_months: readNumberInputValue("#companyEditRecordCashRunway"),
      financials_updated_at: document.querySelector("#companyEditRecordFinancialsUpdatedAt")?.value || null,
      primary_contact_name: document.querySelector("#companyEditRecordPrimaryContactName")?.value.trim(),
      primary_contact_title: document.querySelector("#companyEditRecordPrimaryContactTitle")?.value.trim(),
      primary_contact_email: document.querySelector("#companyEditRecordPrimaryContactEmail")?.value.trim(),
      primary_contact_phone: document.querySelector("#companyEditRecordPrimaryContactPhone")?.value.trim(),
      investment_thesis: document.querySelector("#companyEditRecordInvestmentThesis")?.value.trim(),
      key_concerns: document.querySelector("#companyEditRecordKeyConcerns")?.value.trim(),
    });
    if (Object.keys(recordPayload).length > 1) {
      updates.push(request(`/api/v1/pipeline-records/${context.activeRecord.id}`, {
        method: "PATCH",
        body: JSON.stringify(recordPayload),
      }));
    }
  }
  if (!updates.length) {
    toast("No canonical edits to save");
    state.modal = { type: "company-detail", companyId };
    renderModal();
    return;
  }
  await Promise.all(updates);
  await refreshCurrentData();
  state.modal = { type: "company-detail", companyId };
  renderModal();
  toast("Company detail saved");
}

function documentCategoryValue(document) {
  return normalizeText(document?.document_category || document?.category || "other");
}

function filterDocumentsByCategory(documents, categories) {
  const allowed = new Set(categories.map((item) => normalizeText(item)));
  return documents.filter((document) => allowed.has(documentCategoryValue(document)));
}

function sharedWorkspacePath(fileName) {
  return `workspace://shared/${encodeURIComponent(fileName || "document")}`;
}

function decodeSharedWorkspaceFileName(path) {
  if (!String(path || "").startsWith("workspace://shared/")) return "";
  return decodeURIComponent(String(path).replace("workspace://shared/", ""));
}

function renderDocumentReference(path) {
  if (!path) return "Shared with everyone on this platform";
  if (/^https?:\/\//i.test(path)) {
    return `<a href="${path}" target="_blank">${path}</a>`;
  }
  if (String(path).startsWith("workspace://shared/")) {
    const label = decodeSharedWorkspaceFileName(path) || "Shared workspace file";
    return `${label} · shared with everyone on this platform`;
  }
  return path;
}

function inferDocumentCategoryFromFile(file) {
  const fileName = normalizeText(file?.name || "");
  if (!fileName) return "other";
  if (fileName.includes("deck") || fileName.includes("pitch")) return "deck";
  if (
    fileName.includes("financial") ||
    fileName.includes("forecast") ||
    fileName.includes("budget") ||
    fileName.includes("model") ||
    fileName.endsWith(".xls") ||
    fileName.endsWith(".xlsx") ||
    fileName.endsWith(".csv")
  ) {
    return "financials";
  }
  if (fileName.includes("memo")) return "memo";
  if (fileName.includes("nda")) return "nda";
  if (fileName.includes("data room")) return "data_room";
  return "other";
}

function emptyOpportunitySourceFiles() {
  return {
    deck: [],
    financials: [],
    other: [],
  };
}

function createOpportunitySourceFileEntry(file, category) {
  return {
    file,
    name: file?.name || "document",
    document_category: category || inferDocumentCategoryFromFile(file),
    storage_path: "",
  };
}

function inferDocumentCategoryFromReference(reference, fallback = "other") {
  const normalized = normalizeText(reference);
  if (!normalized) return fallback;
  if (normalized.includes("deck") || normalized.includes("pitch")) return "deck";
  if (
    normalized.includes("financial") ||
    normalized.includes("forecast") ||
    normalized.includes("budget") ||
    normalized.includes("model") ||
    normalized.endsWith(".xls") ||
    normalized.endsWith(".xlsx") ||
    normalized.endsWith(".csv")
  ) {
    return "financials";
  }
  if (normalized.includes("memo")) return "memo";
  if (normalized.includes("nda")) return "nda";
  if (normalized.includes("data room")) return "data_room";
  return fallback;
}

function documentNameFromReference(reference, fallback = "Shared document") {
  const value = String(reference || "").trim();
  if (!value) return fallback;
  const withoutQuery = value.split("?")[0].split("#")[0];
  const name = withoutQuery.split("/").filter(Boolean).pop();
  return name || fallback;
}

function renderOpportunityFileSummary(files, emptyLabel) {
  if (!files.length) return `<p class="muted">${emptyLabel}</p>`;
  return `
    <div class="selected-file-list">
      ${files.map((file) => `<span class="tag">${file.name}</span>`).join("")}
    </div>
  `;
}

function parseMaterialLinks(value, fallbackCategory = "other") {
  return String(value || "")
    .split("\n")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((reference) => ({
      storage_path: reference,
      file_name: documentNameFromReference(reference),
      document_category: inferDocumentCategoryFromReference(reference, fallbackCategory),
    }));
}

function hasValue(value) {
  return value !== null && value !== undefined && String(value).trim() !== "";
}

function firstPresentValue(...values) {
  return values.find((value) => hasValue(value));
}

function inputValue(value) {
  return hasValue(value) ? value : "";
}

function formatPercent(value, maximumFractionDigits = 1) {
  if (!hasValue(value)) return "TBD";
  const numericValue = Number(value);
  if (Number.isNaN(numericValue)) return `${value}%`;
  return `${new Intl.NumberFormat("en-BE", { maximumFractionDigits }).format(numericValue)}%`;
}

function formatDecimal(value, maximumFractionDigits = 1) {
  if (!hasValue(value)) return "TBD";
  const numericValue = Number(value);
  if (Number.isNaN(numericValue)) return String(value);
  return new Intl.NumberFormat("en-BE", { maximumFractionDigits }).format(numericValue);
}

function renderFinancialOverviewCard(subject, options = {}) {
  const name = options.name || subject?.name || subject?.organization_name || subject?.company_name || "this company";
  const revenue = firstPresentValue(subject?.annual_recurring_revenue, subject?.arr, subject?.revenue, subject?.revenue_run_rate);
  const growth = firstPresentValue(subject?.revenue_growth_pct, subject?.growth_pct);
  const grossMargin = firstPresentValue(subject?.gross_margin_pct);
  const ebitdaMargin = firstPresentValue(subject?.ebitda_margin_pct, subject?.ebit_margin_pct);
  const ruleOf40 = firstPresentValue(subject?.rule_of_40_pct, subject?.rule_of_40);
  const burn = firstPresentValue(subject?.monthly_burn, subject?.burn_rate);
  const runway = firstPresentValue(subject?.cash_runway_months);
  const lastUpdated = firstPresentValue(subject?.financials_updated_at, subject?.last_financials_at);
  return `
    <div class="detail-card">
      <h4>Financial overview</h4>
      <p class="muted">This becomes the shared operating snapshot for ${name}: the numbers everyone should review before diligence, partner discussion, and IC.</p>
      <div class="financial-metrics-grid">
        <div class="summary-block financial-metric-card">
          <strong>Revenue / ARR</strong>
          <p class="muted">${hasValue(revenue) ? formatCurrency(revenue) : "Add the latest reported revenue or ARR."}</p>
        </div>
        <div class="summary-block financial-metric-card">
          <strong>Revenue growth</strong>
          <p class="muted">${hasValue(growth) ? formatPercent(growth) : "Add the latest growth percentage."}</p>
        </div>
        <div class="summary-block financial-metric-card">
          <strong>Gross margin</strong>
          <p class="muted">${hasValue(grossMargin) ? formatPercent(grossMargin) : "Add gross margin once finance files are loaded."}</p>
        </div>
        <div class="summary-block financial-metric-card">
          <strong>EBITDA margin</strong>
          <p class="muted">${hasValue(ebitdaMargin) ? formatPercent(ebitdaMargin) : "Add EBITDA margin when available."}</p>
        </div>
        <div class="summary-block financial-metric-card">
          <strong>Rule of 40</strong>
          <p class="muted">${hasValue(ruleOf40) ? formatPercent(ruleOf40) : "Add the current Rule of 40 score."}</p>
        </div>
        <div class="summary-block financial-metric-card">
          <strong>Monthly burn</strong>
          <p class="muted">${hasValue(burn) ? formatCurrency(burn) : "Add the current monthly burn."}</p>
        </div>
        <div class="summary-block financial-metric-card">
          <strong>Cash runway</strong>
          <p class="muted">${hasValue(runway) ? `${formatDecimal(runway)} month${Number(runway) === 1 ? "" : "s"}` : "Add the current cash runway."}</p>
        </div>
        <div class="summary-block financial-metric-card">
          <strong>Financials last updated</strong>
          <p class="muted">${hasValue(lastUpdated) ? formatDate(lastUpdated) : "No financial package linked yet."}</p>
        </div>
      </div>
    </div>
  `;
}

function renderMaterialsHub(title, documents, options = {}) {
  const deckDocuments = filterDocumentsByCategory(documents, ["deck"]);
  const financialDocuments = filterDocumentsByCategory(documents, ["financials"]);
  const otherDocuments = documents.filter(
    (document) => !deckDocuments.includes(document) && !financialDocuments.includes(document)
  );
  const intro =
    options.intro ||
    "Keep the latest deck, financial package, and supporting files together so everyone is reviewing the same source material.";
  return `
    <div class="detail-card">
      <h4>${title}</h4>
      <p class="muted">${intro}</p>
      <div class="list-stack">
        <div class="summary-block">
          <strong>Pitch deck</strong>
          <p class="muted">${deckDocuments.length ? `${deckDocuments[0].file_name || deckDocuments[0].title || "Latest deck"} is already in the shared room.` : "Reserved for the current pitch deck or company presentation."}</p>
        </div>
        <div class="summary-block">
          <strong>Financials</strong>
          <p class="muted">${financialDocuments.length ? `${financialDocuments.length} shared financial file${financialDocuments.length === 1 ? "" : "s"} linked.` : "Reserved for P&L, forecast, KPI pack, model, and board financials."}</p>
        </div>
        <div class="summary-block">
          <strong>Everything else</strong>
          <p class="muted">${otherDocuments.length ? `${otherDocuments.length} supporting file${otherDocuments.length === 1 ? "" : "s"} available for the team.` : "Use this area for memos, data-room material, NDAs, and analysis."}</p>
        </div>
      </div>
      ${documents.length
        ? `
          <div class="timeline" style="margin-top:12px">
            ${documents
              .slice(0, 4)
              .map(
                (document) => `
                  <div class="timeline-item">
                    <strong>${document.file_name || document.title || "Document"}</strong>
                    <p class="muted">${titleCaseStatus(documentCategoryValue(document))} · ${renderDocumentReference(document.storage_path)}</p>
                  </div>
                `
              )
              .join("")}
          </div>
        `
        : ""}
      ${options.uploadHtml || ""}
    </div>
  `;
}

function renderPipelineRecordDocumentUpload(record) {
  return `
    <div class="quick-form" style="margin-top:12px">
      <div class="form-grid">
        <label>
          Document name
          <input id="pipelineRecordDocumentName-${record.id}" placeholder="Series A pitch deck" />
        </label>
        <label>
          Category
          <select id="pipelineRecordDocumentCategory-${record.id}">
            <option value="other">Auto detect / other</option>
            <option value="deck">Deck</option>
            <option value="financials">Financials</option>
            <option value="memo">Memo</option>
            <option value="nda">NDA</option>
            <option value="data_room">Data room</option>
          </select>
        </label>
        <label class="full">
          Browse from your computer
          <input id="pipelineRecordDocumentFile-${record.id}" type="file" />
        </label>
        <label>
          Shared access
          <input value="Everyone on this platform" disabled />
        </label>
        <label class="full">
          Shared link or stored file reference
          <input id="pipelineRecordDocumentPath-${record.id}" placeholder="Auto-filled from a selected file or paste a shared URL" />
        </label>
      </div>
      <p id="pipelineRecordDocumentShareLabel-${record.id}" class="muted">Choose any file from your computer or paste a shared link. New documents added here are meant to be visible to everyone using this platform.</p>
      <div class="footer-actions">
        <button class="button button-secondary" data-add-pipeline-record-document="${record.id}">Add shared document</button>
      </div>
    </div>
  `;
}

function renderPipelineRecordWorkflowEditor(record, buttonLabel = "Save canonical workflow") {
  const linkedOrganization = state.organizations.find((item) => String(item.id) === String(record.organization_id)) || null;
  const websiteReady = Boolean(linkedOrganization?.website);
  const documents = pipelineRecordDocuments(record);
  const deckCount = filterDocumentsByCategory(documents, ["deck"]).length;
  const financialCount = filterDocumentsByCategory(documents, ["financials"]).length;
  const financialsLastUpdated = String(firstPresentValue(record.financials_updated_at, record.last_financials_at) || "").slice(0, 10);
  return `
    <div class="quick-form">
      <div class="form-grid">
        <label>
          Stage
          <select id="pipelineRecordStage-${record.id}">
            ${state.bootstrap.pipeline_record_stages
              .map((stage) => `<option value="${stage}" ${record.stage === stage ? "selected" : ""}>${titleCaseStatus(stage)}</option>`)
              .join("")}
          </select>
        </label>
        <label>
          Status
          <select id="pipelineRecordStatus-${record.id}">
            ${state.bootstrap.pipeline_record_statuses
              .map((status) => `<option value="${status}" ${record.status === status ? "selected" : ""}>${titleCaseStatus(status)}</option>`)
              .join("")}
          </select>
        </label>
        <label>
          Priority
          <select id="pipelineRecordPriority-${record.id}">
            ${state.bootstrap.priorities
              .map((priority) => `<option value="${priority}" ${record.priority === priority ? "selected" : ""}>${titleCaseStatus(priority)}</option>`)
              .join("")}
          </select>
        </label>
        <label>
          Decision outcome
          <select id="pipelineRecordDecision-${record.id}">
            ${state.bootstrap.decision_outcomes
              .map((outcome) => `<option value="${outcome}" ${record.decision_outcome === outcome ? "selected" : ""}>${titleCaseStatus(outcome)}</option>`)
              .join("")}
          </select>
        </label>
        <label>
          Source type
          <input id="pipelineRecordSourceType-${record.id}" value="${record.source_type || ""}" placeholder="inbound / network / referral" />
        </label>
        <label>
          Primary contact
          <input id="pipelineRecordPrimaryContactName-${record.id}" value="${record.primary_contact_name || record.primary_contact?.name || ""}" placeholder="Founder / CFO / main contact" />
        </label>
        <label>
          Contact title
          <input id="pipelineRecordPrimaryContactTitle-${record.id}" value="${record.primary_contact_title || record.primary_contact?.title || ""}" placeholder="CEO" />
        </label>
        <label>
          Contact email
          <input id="pipelineRecordPrimaryContactEmail-${record.id}" value="${record.primary_contact_email || record.primary_contact?.email || ""}" placeholder="name@company.com" />
        </label>
        <label>
          Contact phone
          <input id="pipelineRecordPrimaryContactPhone-${record.id}" value="${record.primary_contact_phone || record.primary_contact?.phone || ""}" placeholder="+32 ..." />
        </label>
        <label>
          Fund fit
          <input id="pipelineRecordFundFit-${record.id}" value="${record.fund_fit || ""}" placeholder="Growth / Precise / Both" />
        </label>
        <label class="full">
          Source detail
          <input id="pipelineRecordSourceDetail-${record.id}" value="${record.source_detail || ""}" placeholder="Where did this originate?" />
        </label>
        <label>
          Round
          <input id="pipelineRecordRoundName-${record.id}" value="${record.round_name || ""}" placeholder="Series A" />
        </label>
        <label>
          Ticket target
          <input id="pipelineRecordTicketTarget-${record.id}" value="${record.ticket_size_target || ""}" placeholder="2500000" />
        </label>
        <label>
          Ownership target %
          <input id="pipelineRecordOwnershipTarget-${record.id}" value="${record.ownership_target_pct || ""}" placeholder="10" />
        </label>
        <label>
          Valuation min
          <input id="pipelineRecordValuationMin-${record.id}" value="${record.valuation_min || ""}" placeholder="15000000" />
        </label>
        <label>
          Valuation max
          <input id="pipelineRecordValuationMax-${record.id}" value="${record.valuation_max || ""}" placeholder="25000000" />
        </label>
        <label>
          Next step due
          <input id="pipelineRecordDueDate-${record.id}" type="date" value="${record.next_step_due_at || ""}" />
        </label>
        <label>
          Decision due
          <input id="pipelineRecordDecisionDueDate-${record.id}" type="date" value="${record.decision_due_at || ""}" />
        </label>
        <div class="financial-metrics-section full">
          <div class="financial-metrics-section-header">
            <strong>Canonical financial metrics</strong>
            <p class="muted">Keep the shared snapshot current for revenue quality, profitability, burn, runway, and the recency of the finance pack.</p>
          </div>
          <div class="financial-metrics-editor-grid">
            <label>
              Revenue / ARR
              <input id="pipelineRecordRevenueArr-${record.id}" type="number" step="0.01" value="${inputValue(firstPresentValue(record.annual_recurring_revenue, record.arr, record.revenue, record.revenue_run_rate))}" placeholder="2500000" />
            </label>
            <label>
              Revenue growth %
              <input id="pipelineRecordRevenueGrowth-${record.id}" type="number" step="0.1" value="${inputValue(firstPresentValue(record.revenue_growth_pct, record.growth_pct))}" placeholder="35" />
            </label>
            <label>
              Gross margin %
              <input id="pipelineRecordGrossMargin-${record.id}" type="number" step="0.1" value="${inputValue(record.gross_margin_pct)}" placeholder="72" />
            </label>
            <label>
              EBITDA margin %
              <input id="pipelineRecordEbitdaMargin-${record.id}" type="number" step="0.1" value="${inputValue(firstPresentValue(record.ebitda_margin_pct, record.ebit_margin_pct))}" placeholder="18" />
            </label>
            <label>
              Rule of 40 %
              <input id="pipelineRecordRuleOf40-${record.id}" type="number" step="0.1" value="${inputValue(firstPresentValue(record.rule_of_40_pct, record.rule_of_40))}" placeholder="53" />
            </label>
            <label>
              Monthly burn
              <input id="pipelineRecordMonthlyBurn-${record.id}" type="number" step="0.01" value="${inputValue(firstPresentValue(record.monthly_burn, record.burn_rate))}" placeholder="150000" />
            </label>
            <label>
              Cash runway months
              <input id="pipelineRecordCashRunway-${record.id}" type="number" step="0.1" value="${inputValue(record.cash_runway_months)}" placeholder="14" />
            </label>
            <label>
              Financials last updated
              <input id="pipelineRecordFinancialsUpdatedAt-${record.id}" type="date" value="${financialsLastUpdated}" />
            </label>
          </div>
        </div>
        <label>
          NDA required
          <select id="pipelineRecordNdaRequired-${record.id}">
            <option value="0" ${String(record.nda_required) === "0" ? "selected" : ""}>No</option>
            <option value="1" ${String(record.nda_required) === "1" ? "selected" : ""}>Yes</option>
          </select>
        </label>
        <label>
          NDA status
          <select id="pipelineRecordNdaStatus-${record.id}">
            ${["not_required", "awaiting_signature", "signed"].map((status) => `<option value="${status}" ${record.nda_status === status ? "selected" : ""}>${titleCaseStatus(status)}</option>`).join("")}
          </select>
        </label>
        <label class="full">
          Next step
          <input id="pipelineRecordNextStep-${record.id}" value="${record.next_step || ""}" placeholder="What needs to happen next?" />
        </label>
        <label class="full">
          Investment thesis
          <textarea id="pipelineRecordInvestmentThesis-${record.id}" rows="3" placeholder="Why should we spend time on this?">${record.investment_thesis || ""}</textarea>
        </label>
        <label class="full">
          Key concerns
          <textarea id="pipelineRecordKeyConcerns-${record.id}" rows="3" placeholder="What still worries us?">${record.key_concerns || ""}</textarea>
        </label>
        <label class="full">
          Relationship / conflict notes
          <textarea id="pipelineRecordRelationshipNotes-${record.id}" rows="3" placeholder="Capture relationship context or conflicts.">${record.relationship_notes || ""}</textarea>
        </label>
        <label class="full">
          Risk flags (comma separated)
          <input id="pipelineRecordRiskFlags-${record.id}" value="${(record.risk_flags || []).join(", ")}" />
        </label>
        <label class="full">
          Tags (comma separated)
          <input id="pipelineRecordTags-${record.id}" value="${(record.tags || []).join(", ")}" />
        </label>
      </div>
      <p class="muted">Autofill sources: ${websiteReady ? "website ready" : "no website yet"} · ${deckCount} deck${deckCount === 1 ? "" : "s"} · ${financialCount} financial file${financialCount === 1 ? "" : "s"}.</p>
      <div class="footer-actions">
        <button class="button button-secondary" data-refresh-pipeline-record="${record.id}">Refresh canonical record</button>
        <button class="button button-secondary" data-autofill-pipeline-record="${record.id}">Autofill From Sources</button>
        <button class="button button-primary" data-save-pipeline-record-workflow="${record.id}">${buttonLabel}</button>
      </div>
    </div>
  `;
}

function renderPipelineRecordWorkflowBridge(record) {
  const hasCanonicalWorkflow =
    Boolean(record?.workflow_available) ||
    Array.isArray(record?.notes) ||
    Array.isArray(record?.tasks) ||
    Array.isArray(record?.documents) ||
    Array.isArray(record?.decision_logs) ||
    Array.isArray(record?.decisions);
  const legacyAction = record.legacy_opportunity_id
    ? `<button class="button button-primary" data-open-full-opportunity="${record.legacy_opportunity_id}">Open legacy workflow</button>`
    : "";
  const workflowMessage = hasCanonicalWorkflow
    ? "Workflow actions below save against the pipeline-record id, so this record can be managed without opening a legacy deal first."
    : pipelineRecordLegacyWorkflowLabel(record);
  return `
    <div class="detail-card">
      <h4>Deeper workflow actions</h4>
      <p class="muted">${workflowMessage}</p>
      <div class="footer-actions">
        ${legacyAction}
      </div>
    </div>
  `;
}

function pipelineRecordNotes(record) {
  return Array.isArray(record?.notes) ? record.notes : [];
}

function pipelineRecordTasks(record) {
  return Array.isArray(record?.tasks) ? record.tasks : [];
}

function pipelineRecordDocuments(record) {
  return Array.isArray(record?.documents) ? record.documents : [];
}

function pipelineRecordDecisionLogs(record) {
  if (Array.isArray(record?.decision_logs)) return record.decision_logs;
  if (Array.isArray(record?.decisions)) return record.decisions;
  return [];
}

function pipelineRecordActivities(record) {
  return Array.isArray(record?.activities) ? record.activities : [];
}

function pipelineRecordTaskComments(task) {
  return Array.isArray(task?.comments) ? task.comments : [];
}

function pipelineRecordTaskActionMode(task) {
  if (task?.workflow_scope === "canonical" || task?.storage_scope === "pipeline_record") return "canonical";
  if (task?.opportunity_id || task?.legacy_task_id) return "legacy";
  if (task?.pipeline_record_id && !task?.opportunity_id) return "canonical";
  return "legacy";
}

function pipelineRecordTaskSupportsDelete(task) {
  return Boolean(task?.can_delete || task?.delete_supported);
}

function pipelineRecordHasWorkflow(record) {
  return Boolean(record?.workflow_available)
    || Boolean(record?.workflow_supported)
    || pipelineRecordNotes(record).length > 0
    || pipelineRecordTasks(record).length > 0
    || pipelineRecordDocuments(record).length > 0
    || pipelineRecordDecisionLogs(record).length > 0
    || pipelineRecordActivities(record).length > 0;
}

function renderPipelineRecordWorkflowWorkspace(record) {
  const notes = pipelineRecordNotes(record);
  const tasks = pipelineRecordTasks(record);
  const documents = pipelineRecordDocuments(record);
  const decisions = pipelineRecordDecisionLogs(record);
  const activities = pipelineRecordActivities(record);
  if (!pipelineRecordHasWorkflow(record)) {
    return `
      <div class="detail-card">
        <h4>Workflow workspace</h4>
        <p class="muted">This canonical record does not expose notes, tasks, documents, or decisions yet. Refresh the record after backend workflow support is available.</p>
      </div>
    `;
  }
  return `
    <div class="detail-card">
      <h4>Notes / comments</h4>
      ${notes.length
        ? notes
            .map(
              (note) => `
                <div class="timeline-item">
                  <strong>${titleCaseStatus(note.note_type || "general")}</strong>
                  <p>${note.body || note.content || "No note body."}</p>
                  <span class="muted">${note.author_name || "Unknown"} · ${formatDate(note.created_at)}</span>
                </div>
              `
            )
            .join("")
        : `<p class="muted">No notes yet.</p>`}
      <div class="quick-form">
        <div class="form-grid">
          <label>
            Note type
            <select id="pipelineRecordNoteType-${record.id}">
              ${state.bootstrap.note_types
                .map((type) => `<option value="${type}">${type}</option>`)
                .join("")}
            </select>
          </label>
          <label class="full">
            Note
            <textarea id="pipelineRecordNoteBody-${record.id}" rows="4" placeholder="Write the note you want attached to this pipeline record."></textarea>
          </label>
        </div>
        <div class="footer-actions">
          <button class="button button-secondary" data-add-pipeline-record-note="${record.id}">Add Note</button>
        </div>
      </div>
    </div>
    <div class="detail-card">
      <h4>Tasks</h4>
      <div class="task-stack">
        ${tasks.length
          ? tasks
              .map(
                (task) => `
                  <div class="task-item ${task.is_overdue ? "overdue" : ""}">
                    <div class="panel-header">
                      <div>
                        <strong>${task.title}</strong>
                        <p class="muted">${task.assignee_name || "Unassigned"} · due ${formatDate(task.due_at)}</p>
                      </div>
                      <div class="split-actions">
                        <span class="pill ${task.is_overdue ? "overdue" : ""}">${task.priority || "medium"}</span>
                        <select data-pipeline-record-task-status-select="${task.id}">
                          ${state.bootstrap.task_statuses
                            .map(
                              (status) => `<option value="${status}" ${task.status === status ? "selected" : ""}>${titleCaseStatus(status)}</option>`
                            )
                            .join("")}
                        </select>
                      </div>
                    </div>
                    <p>${task.description || "No description."}</p>
                    ${pipelineRecordTaskComments(task).length
                      ? `
                        <div class="timeline" style="margin-top:8px">
                          ${pipelineRecordTaskComments(task)
                            .slice(-3)
                            .map(
                              (comment) => `
                                <div class="timeline-item">
                                  <strong>${comment.user_name || comment.author_name || "Unknown"}</strong>
                                  <p>${comment.body}</p>
                                  <span class="muted">${formatDate(comment.created_at)}</span>
                                </div>
                              `
                            )
                            .join("")}
                        </div>
                      `
                      : `<p class="muted">No task comments yet.</p>`}
                    <p class="muted">${pipelineRecordTaskActionMode(task) === "canonical" ? "This task is stored on the pipeline record." : "This task still writes through the linked legacy workflow."}</p>
                    <div class="quick-form">
                      <div class="form-grid">
                        <label class="full">
                          Comment
                          <input id="pipelineRecordTaskComment-${task.id}" placeholder="Add a task comment" />
                        </label>
                      </div>
                      <div class="split-actions">
                        <button class="button button-secondary" data-update-pipeline-record-task="${task.id}" data-pipeline-record-id="${record.id}">Save task</button>
                        <button class="button button-secondary" data-add-pipeline-record-task-comment="${task.id}" data-pipeline-record-id="${record.id}">Add comment</button>
                        ${pipelineRecordTaskSupportsDelete(task)
                          ? `<button class="button button-danger" data-delete-pipeline-record-task="${task.id}" data-pipeline-record-id="${record.id}">Delete task</button>`
                          : ""}
                      </div>
                    </div>
                  </div>
                `
              )
              .join("")
          : `<p class="muted">No tasks yet.</p>`}
      </div>
      <div class="quick-form">
        <div class="form-grid">
          <label>
            Title
            <input id="pipelineRecordTaskTitle-${record.id}" placeholder="Create execution task" />
          </label>
          <label>
            Assignee
            <select id="pipelineRecordTaskAssignee-${record.id}">
              <option value="">Unassigned</option>
              ${state.bootstrap.users.map((user) => `<option value="${user.id}">${user.name}</option>`).join("")}
            </select>
          </label>
          <label>
            Priority
            <select id="pipelineRecordTaskPriority-${record.id}">
              ${state.bootstrap.task_priorities.map((priority) => `<option value="${priority}">${priority}</option>`).join("")}
            </select>
          </label>
          <label>
            Due date
            <input id="pipelineRecordTaskDueDate-${record.id}" type="date" />
          </label>
          <label class="full">
            Description
            <textarea id="pipelineRecordTaskDescription-${record.id}" rows="3" placeholder="What should happen next?"></textarea>
          </label>
        </div>
        <div class="footer-actions">
          <button class="button button-secondary" data-add-pipeline-record-task="${record.id}">Add Task</button>
        </div>
      </div>
    </div>
    ${renderMaterialsHub("Shared materials", documents, {
      intro: "Use this record-level room for the pitch deck, financial package, and the files every teammate should be able to open from the pipeline.",
      uploadHtml: renderPipelineRecordDocumentUpload(record),
    })}
    <div class="detail-card">
      <h4>Decision history</h4>
      ${decisions.length
        ? decisions
            .map(
              (decision) => `
                <div class="timeline-item">
                  <strong>${decision.decision_summary || titleCaseStatus(decision.decision_type || "decision")}</strong>
                  <p class="muted">${decision.rationale || "No rationale recorded."}</p>
                  <span class="muted">${decision.decided_by_name || "Unknown"} · ${formatDate(decision.created_at)}</span>
                </div>
              `
            )
            .join("")
        : `<p class="muted">No decisions logged yet.</p>`}
      <div class="quick-form">
        <div class="form-grid">
          <label>
            Decision type
            <select id="pipelineRecordDecisionType-${record.id}">
              <option value="advance">Advance</option>
              <option value="hold">Hold</option>
              <option value="pass">Pass</option>
              <option value="term_sheet">Term Sheet</option>
              <option value="close">Close</option>
            </select>
          </label>
          <label class="full">
            Summary
            <input id="pipelineRecordDecisionSummary-${record.id}" placeholder="Record the actual decision" />
          </label>
          <label class="full">
            Rationale
            <textarea id="pipelineRecordDecisionRationale-${record.id}" rows="3" placeholder="Why was this decision made?"></textarea>
          </label>
        </div>
        <div class="footer-actions">
          <button class="button button-secondary" data-add-pipeline-record-decision="${record.id}">Log Decision</button>
        </div>
      </div>
    </div>
    <div class="detail-card">
      <h4>Activity log</h4>
      <div class="timeline">
        ${activities.length
          ? activities
              .map(
                (activity) => `
                  <div class="timeline-item">
                    <strong>${activity.summary || activity.activity_type || "Activity"}</strong>
                    <span class="muted">${activity.user_name || "Unknown"} · ${formatDate(activity.created_at)}</span>
                  </div>
                `
              )
              .join("")
          : `<p class="muted">No activity logged yet.</p>`}
      </div>
    </div>
  `;
}

function ensureCanonicalSelections() {
  if (state.selectedOrganizationId && !selectedOrganization()) {
    state.selectedOrganizationId = null;
  }
  if (state.selectedPipelineRecordId && !selectedPipelineRecord()) {
    state.selectedPipelineRecordId = null;
  }
  if (!state.selectedOrganizationId && state.organizations.length) {
    state.selectedOrganizationId = state.organizations[0].id;
  }
  if (!state.selectedPipelineRecordId && state.pipelineRecords.length) {
    state.selectedPipelineRecordId = state.pipelineRecords[0].id;
  }
  const activeRecord = selectedPipelineRecord();
  if (!state.selectedOrganizationId && activeRecord?.organization_id) {
    state.selectedOrganizationId = activeRecord.organization_id;
  } else if (state.selectedOrganizationId) {
    const related = relatedPipelineRecords(state.selectedOrganizationId);
    if (related.length) {
      if (!activeRecord || String(activeRecord.organization_id) !== String(state.selectedOrganizationId)) {
        state.selectedPipelineRecordId = related[0].id;
      }
    } else if (activeRecord && String(activeRecord.organization_id) !== String(state.selectedOrganizationId)) {
      state.selectedPipelineRecordId = null;
    }
  }
}

function legacyOrganizationOptions() {
  const canonical = state.organizations
    .filter((item) => item.legacy_company_id)
    .map((item) => ({ value: item.legacy_company_id, label: item.name }));
  if (canonical.length) return canonical;
  return state.companies.map((company) => ({ value: company.id, label: company.name }));
}

function legacyPipelineRecordOptions() {
  const canonical = state.pipelineRecords
    .filter((item) => item.legacy_opportunity_id)
    .map((item) => ({ value: item.legacy_opportunity_id, label: `${item.organization_name} · ${item.record_code}` }));
  if (canonical.length) return canonical;
  return state.opportunities.map((item) => ({ value: item.id, label: `${item.company_name} · ${item.deal_code || item.id}` }));
}

function renderOrganizationForm(organization = null) {
  const isEditing = Boolean(organization?.id);
  return `
    <form id="organizationForm" class="quick-form" ${isEditing ? `data-organization-id="${organization.id}"` : ""}>
      <div class="form-grid">
        <label>
          Name
          <input name="name" value="${organization?.name || ""}" required />
        </label>
        <label>
          Type
          <select name="organization_type">
            ${state.bootstrap.organization_types.map((type) => `<option value="${type}" ${organization?.organization_type === type ? "selected" : ""}>${type.replaceAll("_", " ")}</option>`).join("")}
          </select>
        </label>
        <label>
          Owner
          <select name="owner_user_id">
            <option value="">Unassigned</option>
            ${state.bootstrap.users.map((user) => `<option value="${user.id}" ${String(organization?.owner_user_id || state.currentUserId) === String(user.id) ? "selected" : ""}>${user.name}</option>`).join("")}
          </select>
        </label>
        <label>
          Relationship status
          <select name="relationship_status">
            ${state.bootstrap.relationship_statuses.map((status) => `<option value="${status}" ${organization?.relationship_status === status ? "selected" : ""}>${status.replaceAll("_", " ")}</option>`).join("")}
          </select>
        </label>
        <label>
          Website
          <input name="website" value="${organization?.website || ""}" />
        </label>
        <label>
          Sector
          <input name="sector_primary" value="${organization?.sector_primary || ""}" />
        </label>
        <label>
          Subsector
          <input name="subsector" value="${organization?.subsector || ""}" />
        </label>
        <label>
          Geography
          <input name="geography" value="${organization?.geography || ""}" />
        </label>
        <label>
          HQ city
          <input name="hq_city" value="${organization?.hq_city || ""}" />
        </label>
        <label>
          HQ country
          <input name="hq_country" value="${organization?.hq_country || ""}" />
        </label>
        <label>
          Business model
          <input name="business_model" value="${organization?.business_model || ""}" />
        </label>
        <label>
          Primary contact
          <input name="primary_contact_name" value="${organization?.primary_contact?.name || ""}" />
        </label>
        <label>
          Contact title
          <input name="primary_contact_title" value="${organization?.primary_contact?.title || ""}" />
        </label>
        <label>
          Contact email
          <input name="primary_contact_email" value="${organization?.primary_contact?.email || ""}" />
        </label>
        <label>
          Contact phone
          <input name="primary_contact_phone" value="${organization?.primary_contact?.phone || ""}" />
        </label>
        <label class="full">
          Description
          <textarea name="description" rows="3">${organization?.description || ""}</textarea>
        </label>
      </div>
      <div class="footer-actions">
        <button type="submit" class="button button-primary">${isEditing ? "Save company profile" : "Create Organization"}</button>
      </div>
    </form>
  `;
}

function renderPipelineRecordForm() {
  const organizationOptions = state.organizations.map((org) => {
    const selected = String(org.id) === String(state.selectedOrganizationId) ? "selected" : "";
    return `<option value="${org.id}" ${selected}>${org.name}</option>`;
  }).join("");

  if (!state.organizations.length) {
    return emptyState(
      "Create an organization first",
      "Pipeline records attach to canonical organizations, so the front door needs at least one organization before a record can be created.",
      `<button class="button button-primary" data-route="organizations">Open organizations</button>`
    );
  }

  return `
    <form id="pipelineRecordForm" class="quick-form">
      <div class="form-grid">
        <label>
          Organization
          <select name="organization_id" required>
            <option value="">Select organization</option>
            ${organizationOptions}
          </select>
        </label>
        <label>
          Record code
          <input name="record_code" placeholder="PR-001" required />
        </label>
        <label>
          Stage
          <select name="stage">
            ${state.bootstrap.pipeline_record_stages.map((stage) => `<option value="${stage}">${stage.replaceAll("_", " ")}</option>`).join("")}
          </select>
        </label>
        <label>
          Status
          <select name="status">
            ${state.bootstrap.pipeline_record_statuses.map((status) => `<option value="${status}">${status.replaceAll("_", " ")}</option>`).join("")}
          </select>
        </label>
        <label>
          Decision outcome
          <select name="decision_outcome">
            ${state.bootstrap.decision_outcomes.map((outcome) => `<option value="${outcome}">${outcome.replaceAll("_", " ")}</option>`).join("")}
          </select>
        </label>
        <label>
          Owner
          <select name="owner_user_id">
            <option value="">Unassigned</option>
            ${state.bootstrap.users.map((user) => `<option value="${user.id}" ${user.id === Number(state.currentUserId) ? "selected" : ""}>${user.name}</option>`).join("")}
          </select>
        </label>
        <label>
          Fund fit
          <input name="fund_fit" placeholder="Growth / Precise / Both" />
        </label>
        <label>
          Source type
          <input name="source_type" placeholder="network / inbound / co-investment" />
        </label>
        <label class="full">
          Source detail
          <input name="source_detail" />
        </label>
        <label class="full">
          Next step
          <input name="next_step" />
        </label>
        <label>
          Next step due
          <input type="date" name="next_step_due_at" />
        </label>
        <label class="full">
          Investment thesis
          <textarea name="investment_thesis" rows="3"></textarea>
        </label>
        <label class="full">
          Key concerns
          <textarea name="key_concerns" rows="3"></textarea>
        </label>
      </div>
      <div class="footer-actions">
        <button type="submit" class="button button-primary">Create Pipeline Record</button>
      </div>
    </form>
  `;
}

function filteredPipelineRecords() {
  const query = normalizeText(state.pipelineFilters.q);
  const now = new Date();
  const items = state.pipelineRecords.filter((item) => {
    if (state.pipelineFilters.stage !== "all" && item.stage !== state.pipelineFilters.stage) return false;
    if (state.pipelineFilters.status !== "all" && item.status !== state.pipelineFilters.status) return false;
    if (
      state.pipelineFilters.owner_user_id !== "all"
      && String(item.owner_user_id || "") !== String(state.pipelineFilters.owner_user_id)
    ) {
      return false;
    }
    if (state.pipelineFilters.overdue_only === "1") {
      const due = item.next_step_due_at ? new Date(item.next_step_due_at) : null;
      if (!due || Number.isNaN(due.getTime()) || due >= now || isClosedPipelineRecord(item)) return false;
    }
    if (state.pipelineFilters.missing_info === "1" && pipelineRecordMissingCount(item) === 0) return false;
    if (query) {
      const haystack = normalizeText([
        item.organization_name,
        item.record_code,
        item.stage,
        item.status,
        item.owner_name,
        item.source_type,
        item.source_detail,
        item.fund_fit,
        item.next_step,
      ].join(" "));
      if (!haystack.includes(query)) return false;
    }
    return true;
  });

  const direction = state.pipelineFilters.order;
  const sorters = {
    updated_at: (item) => item.updated_at || "",
    next_step_due_at: (item) => item.next_step_due_at || "",
    organization_name: (item) => normalizeText(item.organization_name),
    stage: (item) => normalizeText(item.stage),
    status: (item) => normalizeText(item.status),
    record_code: (item) => normalizeText(item.record_code),
  };
  const sorter = sorters[state.pipelineFilters.sort] || sorters.updated_at;

  return [...items].sort((left, right) => compareValues(sorter(left), sorter(right), direction));
}

function formatCurrency(value) {
  if (value === null || value === undefined || value === "") return "TBD";
  return new Intl.NumberFormat("en-BE", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(Number(value));
}

function formatDate(value) {
  if (!value) return "No date";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleDateString("en-GB", { day: "numeric", month: "short" });
}

function readNumberInputValue(selector) {
  const value = document.querySelector(selector)?.value ?? "";
  if (!String(value).trim()) return null;
  const parsed = Number(value);
  return Number.isNaN(parsed) ? value : parsed;
}

function request(path, options = {}) {
  const isFormData = options.body instanceof FormData;
  return fetch(path, {
    headers: isFormData ? {} : { "Content-Type": "application/json" },
    ...options,
  }).then(async (response) => {
    const contentType = response.headers.get("content-type") || "";
    const payload = contentType.includes("application/json") ? await response.json() : await response.text();
    if (!response.ok) {
      const error = payload.error?.message || payload || "Request failed";
      const fields = payload.error?.fields || {};
      const err = new Error(error);
      err.fields = fields;
      throw err;
    }
    return payload;
  });
}

async function requestOptional(path, options = {}) {
  try {
    return await request(path, options);
  } catch (error) {
    return null;
  }
}

async function runPipelineRecordAutofill(recordId, options = {}) {
  const response = await request(`/api/v1/pipeline-records/${recordId}/autofill`, {
    method: "POST",
    body: JSON.stringify({
      actor_user_id: state.currentUserId,
      overwrite: Boolean(options.overwrite),
      preview_only: Boolean(options.preview_only),
    }),
  });
  const appliedCount = Object.values(response.applied_fields || {}).reduce((total, fields) => total + fields.length, 0);
  const warningCount = (response.source_summary?.warnings || []).length;
  return {
    ...response,
    summary: appliedCount
      ? `Autofill updated ${appliedCount} field${appliedCount === 1 ? "" : "s"}${warningCount ? ` with ${warningCount} warning${warningCount === 1 ? "" : "s"}` : ""}.`
      : `Autofill found no blank canonical fields to update${warningCount ? ` and returned ${warningCount} warning${warningCount === 1 ? "" : "s"}` : ""}.`,
  };
}

async function submitDocument(endpoint, config) {
  const file = config.fileInput?.files?.[0] || null;
  const fileName = config.nameInput?.value.trim() || "";
  const documentCategory = config.categoryInput?.value || "other";
  const storagePath = config.pathInput?.value.trim() || "";
  if (file) {
    const payload = new FormData();
    payload.append("file", file);
    payload.append("document_category", documentCategory);
    payload.append("uploaded_by_user_id", String(state.currentUserId));
    if (fileName) payload.append("file_name", fileName);
    return request(endpoint, { method: "POST", body: payload });
  }
  if (!storagePath) {
    throw new Error("Choose a file to upload or enter a URL/path.");
  }
  return request(endpoint, {
    method: "POST",
    body: JSON.stringify({
      file_name: fileName,
      document_category: documentCategory,
      storage_path: storagePath,
      uploaded_by_user_id: state.currentUserId,
    }),
  });
}

async function createSharedPipelineRecordDocument(recordId, document) {
  if (document.file) {
    const payload = new FormData();
    payload.append("file", document.file);
    payload.append("document_category", document.document_category || inferDocumentCategoryFromFile(document.file));
    payload.append("uploaded_by_user_id", String(state.currentUserId));
    payload.append("file_name", document.file_name || document.file.name);
    return request(`/api/v1/pipeline-records/${recordId}/documents`, {
      method: "POST",
      body: payload,
    });
  }
  return request(`/api/v1/pipeline-records/${recordId}/documents`, {
    method: "POST",
    body: JSON.stringify({
      file_name: document.file_name || documentNameFromReference(document.storage_path),
      document_category: document.document_category || inferDocumentCategoryFromReference(document.storage_path),
      storage_path: document.storage_path,
      uploaded_by_user_id: state.currentUserId,
    }),
  });
}

async function uploadOpportunitySourceFile(entry) {
  if (!entry?.file || entry.storage_path) return entry;
  const payload = new FormData();
  payload.append("file", entry.file);
  payload.append("document_category", entry.document_category || inferDocumentCategoryFromFile(entry.file));
  payload.append("uploaded_by_user_id", String(state.currentUserId));
  payload.append("file_name", entry.name || entry.file.name);
  const uploaded = await request("/api/v1/shared-documents", {
    method: "POST",
    body: payload,
  });
  return {
    ...entry,
    name: uploaded.file_name || entry.name,
    storage_path: uploaded.storage_path || entry.storage_path,
    document_category: uploaded.document_category || entry.document_category,
  };
}

async function ensureOpportunityMaterialsUploaded() {
  const nextState = {};
  for (const key of ["deck", "financials", "other"]) {
    const items = (state.opportunitySourceFiles?.[key] || []);
    nextState[key] = [];
    for (const entry of items) {
      nextState[key].push(await uploadOpportunitySourceFile(entry));
    }
  }
  state.opportunitySourceFiles = nextState;
  return nextState;
}

function opportunityMaterialDocuments(payload) {
  const selectedFiles = state.opportunitySourceFiles || emptyOpportunitySourceFiles();
  return [
    ...selectedFiles.deck.map((file) => ({
      file: file.storage_path ? null : file.file,
      storage_path: file.storage_path,
      file_name: file.name,
      document_category: "deck",
    })),
    ...selectedFiles.financials.map((file) => ({
      file: file.storage_path ? null : file.file,
      storage_path: file.storage_path,
      file_name: file.name,
      document_category: "financials",
    })),
    ...selectedFiles.other.map((file) => ({
      file: file.storage_path ? null : file.file,
      storage_path: file.storage_path,
      file_name: file.name,
      document_category: file.document_category || inferDocumentCategoryFromFile(file.file),
    })),
    ...parseMaterialLinks(payload.deck_links, "deck"),
    ...parseMaterialLinks(payload.financial_links, "financials"),
    ...parseMaterialLinks(payload.material_links, "other"),
  ];
}

async function attachOpportunityMaterials(recordId, payload) {
  const documents = opportunityMaterialDocuments(payload);
  for (const document of documents) {
    await createSharedPipelineRecordDocument(recordId, document);
  }
  return documents.length;
}

function mergeOpportunityPrefill(prefill) {
  const current = state.opportunityForm || blankOpportunityForm();
  const next = { ...current };
  for (const [key, value] of Object.entries(prefill || {})) {
    if (!hasValue(current[key]) && hasValue(value)) {
      next[key] = value;
    }
  }
  state.opportunityForm = next;
}

async function runOpportunityIntakePrefill(formElement = document.querySelector("#opportunityForm")) {
  if (state.opportunityPrefillPending) return null;
  if (formElement) {
    syncOpportunityFormDraft(formElement);
  }
  state.opportunityPrefillPending = true;
  try {
    await ensureOpportunityMaterialsUploaded();
    const payload = {
      ...(state.opportunityForm || blankOpportunityForm()),
      documents: opportunityMaterialDocuments(state.opportunityForm || blankOpportunityForm()).map((document) => ({
        file_name: document.file_name,
        document_category: document.document_category,
        storage_path: document.storage_path || "",
      })),
    };
    const response = await request("/api/v1/intake-autofill-preview", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    mergeOpportunityPrefill(response.prefill);
    render();
    const warningCount = (response.source_summary?.warnings || []).length;
    toast(
      warningCount
        ? `Materials analyzed and intake draft updated, with ${warningCount} warning${warningCount === 1 ? "" : "s"}.`
        : "Materials analyzed and intake draft updated."
    );
    return response;
  } finally {
    state.opportunityPrefillPending = false;
  }
}

function currentUser() {
  return state.bootstrap?.users.find((user) => user.id === Number(state.currentUserId));
}

function activeNav() {
  const route = state.route.startsWith("opportunity") || state.route === "new-opportunity" ? "pipeline" : state.route;
  document.querySelectorAll("[data-nav]").forEach((link) => {
    link.classList.toggle("active", link.dataset.nav === route);
  });
}

function pageMeta() {
  const route = state.route;
  if (route === "dashboard") return ["Internal CRM", "Dashboard"];
  if (route === "pipeline") return ["Operating System", "Pipeline"];
  if (route.startsWith("opportunity")) return ["Opportunity", state.selectedOpportunity?.company_name || "Deal detail"];
  if (route === "new-opportunity") return ["Structured Intake", "Add Opportunity"];
  if (route === "intake") return ["Front Door", "Intake"];
  if (route === "relationships") return ["Network", "Relationships"];
  if (route === "organizations") return ["Canonical Entities", "Organizations"];
  if (route === "pipeline-records") return ["Operating System", "Pipeline Records"];
  if (route === "tasks") return ["Execution", "Tasks"];
  if (route === "companies") return ["Coverage", "Companies"];
  if (route === "reports") return ["Management", "Reflection / Report"];
  return ["System", "Settings"];
}

function emptyState(title, message, actionHtml = "") {
  return `
    <div class="empty-state">
      <div>
        <h3>${title}</h3>
        <p>${message}</p>
        ${actionHtml}
      </div>
    </div>
  `;
}

function metricCards(summary) {
  const items = [
    ["Active deals", summary.active_deals],
    ["Qualified+", summary.qualified_plus],
    ["IC review this week", summary.ic_review_this_week],
    ["Overdue tasks", summary.overdue_tasks],
    ["Deals missing data", summary.missing_data],
    ["Deals without owner", summary.deals_without_owner],
  ];
  return `<div class="grid-metrics">${items
    .map(
      ([label, value]) => `
        <article class="metric-card">
          <span class="eyebrow">${label}</span>
          <strong>${value}</strong>
        </article>
      `
    )
    .join("")}</div>`;
}

function renderDashboard() {
  if (!state.dashboard) {
    return emptyState("Loading dashboard", "The dashboard is still loading.");
  }
  return `
    ${metricCards(state.dashboard.summary)}
    <div class="dashboard-grid">
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Decision queue</span>
            <h3>Deals requiring decision</h3>
          </div>
          <button class="button button-secondary" data-route="reports">Open report</button>
        </div>
        ${state.dashboard.decision_queue.length
          ? dashboardDecisionTable(state.dashboard.decision_queue)
          : emptyState("No urgent decisions", "No decision-ready deals are currently blocked.")}
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Pipeline health</span>
            <h3>Stage bottlenecks</h3>
          </div>
        </div>
        <div class="list-stack">
          ${state.dashboard.stage_counts
            .map(
              (item) => `
                <div class="summary-block">
                  <strong>${item.label}</strong>
                  <p class="muted">${item.count} opportunities</p>
                </div>
              `
            )
            .join("")}
        </div>
        <div class="panel-header" style="margin-top:12px">
          <div>
            <span class="eyebrow">Data quality</span>
            <h3>Missing information</h3>
          </div>
        </div>
        <div class="list-stack">
          ${state.dashboard.missing_information.length
            ? state.dashboard.missing_information
                .map(
                  (deal) => `
                    <div class="report-row" ${openEntityActionAttributes(deal)}>
                      <strong>${deal.company_name}</strong>
                      <p class="muted">${deal.missing_fields.join(", ")}</p>
                    </div>
                  `
                )
                .join("")
            : `<div class="summary-block"><p class="muted">No missing critical fields currently flagged.</p></div>`}
        </div>
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Execution</span>
            <h3>My workload</h3>
          </div>
        </div>
        <div class="list-stack">
          ${state.dashboard.workload
            .slice(0, 4)
            .map(
              (row) => `
                <div class="summary-block">
                  <strong>${row.name}</strong>
                  <p class="muted">${row.active_deals} active deals · ${row.open_tasks} open tasks · ${row.overdue_tasks} overdue</p>
                </div>
              `
            )
            .join("")}
        </div>
        <div class="panel-header" style="margin-top:12px">
          <div>
            <span class="eyebrow">Recent activity</span>
            <h3>Latest movement</h3>
          </div>
        </div>
        <div class="timeline">
          ${state.dashboard.recent_activity
            .map(
              (item) => `
                <div class="timeline-item">
                  <strong>${item.company_name || "System"}</strong>
                  <p class="muted">${item.summary}</p>
                  <span class="muted">${formatDate(item.created_at)} · ${item.user_name || "Unknown"}</span>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
    </div>
  `;
}

function dashboardDecisionTable(items) {
  return `
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Company</th>
            <th>Stage</th>
            <th>Owner</th>
            <th>Next step</th>
            <th>Missing</th>
          </tr>
        </thead>
        <tbody>
          ${items
            .map(
              (deal) => `
                <tr ${openEntityActionAttributes(deal)}>
                  <td class="company-cell"><strong>${deal.company_name}</strong></td>
                  <td><span class="pill stage-${deal.stage}">${deal.stage_label}</span></td>
                  <td>${deal.owner_name || "Unassigned"}</td>
                  <td>${deal.next_step || "Not set"}</td>
                  <td>${deal.missing_fields.length ? deal.missing_fields.length : "Ready"}</td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function pipelineControls() {
  const filters = state.pipelineFilters;
  const recordStages = state.bootstrap?.pipeline_record_stages || [];
  const recordStatuses = state.bootstrap?.pipeline_record_statuses || [];
  return `
    <div class="panel">
      <div class="panel-header">
        <div>
          <span class="eyebrow">Pipeline operating system</span>
          <h3>Search, filter, sort canonical records</h3>
        </div>
        <div class="split-actions">
          <button class="button button-secondary" data-export="pipeline">Export Report</button>
          <button class="button button-primary" data-route="pipeline-records">Manage records</button>
        </div>
      </div>
      <div class="subnav">
        <button class="${state.viewMode === "table" ? "active" : ""}" data-set-view="table">Table</button>
        <button class="${state.viewMode === "kanban" ? "active" : ""}" data-set-view="kanban">Kanban</button>
      </div>
      <div class="status-tabs">
        ${["all", ...recordStages]
          .map(
            (stage) => `
              <button class="${filters.stage === stage ? "active" : ""}" data-stage-tab="${stage}">
                ${stage === "all" ? "All records" : titleCaseStatus(stage)}
              </button>
            `
          )
          .join("")}
      </div>
      <div class="filter-bar">
        <input name="q" id="pipelineSearch" placeholder="Search organization, record code, source, owner" value="${filters.q}" />
        <select id="pipelineOwnerFilter">
          <option value="all">All owners</option>
          ${state.bootstrap.users
            .map(
              (user) =>
                `<option value="${user.id}" ${String(filters.owner_user_id) === String(user.id) ? "selected" : ""}>${user.name}</option>`
            )
            .join("")}
        </select>
        <select id="pipelineStatusFilter">
          <option value="all">All statuses</option>
          ${recordStatuses
            .map(
              (status) =>
                `<option value="${status}" ${filters.status === status ? "selected" : ""}>${titleCaseStatus(status)}</option>`
            )
            .join("")}
        </select>
        <select id="pipelineSort">
          ${[
            ["updated_at", "Updated"],
            ["next_step_due_at", "Next step due"],
            ["organization_name", "Organization"],
            ["record_code", "Record code"],
            ["stage", "Stage"],
            ["status", "Status"],
          ]
            .map(
              ([value, label]) =>
                `<option value="${value}" ${filters.sort === value ? "selected" : ""}>Sort: ${label}</option>`
            )
            .join("")}
        </select>
        <label class="inline-field">
          <input type="checkbox" id="pipelineOverdueOnly" ${filters.overdue_only === "1" ? "checked" : ""}/>
          <span>Overdue only</span>
        </label>
        <label class="inline-field">
          <input type="checkbox" id="pipelineMissingOnly" ${filters.missing_info === "1" ? "checked" : ""}/>
          <span>Missing info only</span>
        </label>
        <button class="button button-secondary" data-clear-pipeline>Clear filters</button>
      </div>
    </div>
  `;
}

function labelStage(stage) {
  return state.bootstrap.stages.find((item) => item.id === stage)?.label || stage;
}

function renderPipeline() {
  const filtered = filteredPipelineRecords();
  const activeRecord = selectedPipelineRecord(filtered) || filtered[0] || null;
  const activeOrg = activeRecord
    ? state.organizations.find((item) => String(item.id) === String(activeRecord.organization_id))
    : null;
  const activeDocuments = activeRecord ? pipelineRecordDocuments(activeRecord) : [];
  return `
    ${pipelineControls()}
    <div class="pipeline-layout">
      <section class="table-card">
        ${
          state.viewMode === "table"
            ? renderPipelineTable(filtered)
            : renderKanbanBoard()
        }
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Selected canonical record</span>
            <h3>${activeRecord ? activeRecord.organization_name : "No record selected"}</h3>
          </div>
          ${
            activeRecord?.legacy_opportunity_id
              ? `<button class="button button-secondary" data-open-full-opportunity="${activeRecord.legacy_opportunity_id}">Open legacy deal</button>`
              : ""
          }
        </div>
        ${activeRecord
          ? `
            <div class="detail-stack">
              <div class="detail-card">
                <h4>${activeRecord.record_code}</h4>
                <p class="muted">${titleCaseStatus(activeRecord.stage)} · ${titleCaseStatus(activeRecord.status)} · ${activeRecord.owner_name || "Unassigned"}</p>
                <p class="muted">${activeRecord.next_step || "No next step yet."}</p>
                <p class="muted">Due ${formatDate(activeRecord.next_step_due_at)} · ${pipelineRecordMissingCount(activeRecord)} missing inputs</p>
              </div>
              <div class="detail-card">
                <h4>Organization</h4>
                <p class="muted">${activeOrg?.name || activeRecord.organization_name}</p>
                <p class="muted">${titleCaseStatus(activeOrg?.organization_type || "organization")} · ${activeOrg?.sector_primary || "—"} · ${activeOrg?.geography || "—"}</p>
                <div class="footer-actions">
                  <button class="button button-secondary" data-open-organization="${activeRecord.organization_id}">Open organization</button>
                </div>
              </div>
              <div class="detail-card">
                <h4>Decision frame</h4>
                <p class="muted">Outcome: ${titleCaseStatus(activeRecord.decision_outcome || "pending")}</p>
                <p class="muted">Fund fit: ${activeRecord.fund_fit || "—"}</p>
                <p class="muted">Source: ${titleCaseStatus(activeRecord.source_type || "unknown")}${activeRecord.source_detail ? ` · ${activeRecord.source_detail}` : ""}</p>
              </div>
              <div class="detail-card">
                <h4>Notes</h4>
                <p class="muted">${activeRecord.investment_thesis || "No investment thesis captured yet."}</p>
                <p class="muted">${activeRecord.key_concerns || "No key concerns captured yet."}</p>
              </div>
              ${renderFinancialOverviewCard(activeRecord, { name: activeOrg?.name || activeRecord.organization_name })}
              ${renderMaterialsHub("Company materials", activeDocuments, {
                intro: "This is the operating-system shelf for the company deck, finance pack, and diligence files tied to the selected pipeline record.",
              })}
              <div class="detail-card">
                <h4>Canonical workflow controls</h4>
                <p class="muted">These fields save directly on the canonical pipeline record using its own id.</p>
                ${renderPipelineRecordWorkflowEditor(activeRecord)}
              </div>
              ${renderPipelineRecordWorkflowWorkspace(activeRecord)}
              ${renderPipelineRecordWorkflowBridge(activeRecord)}
            </div>
          `
          : emptyState("No records match these filters", "Clear filters or create a canonical pipeline record.")}
      </section>
    </div>
  `;
}

function renderPipelineTable(items) {
  if (!items.length) {
    return emptyState("No pipeline records match these filters", "Clear filters or create a canonical record.", `<button class="button button-primary" data-route="pipeline-records">Manage records</button>`);
  }
  return `
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Organization</th>
            <th>Record</th>
            <th>Stage</th>
            <th>Status</th>
            <th>Owner</th>
            <th>Fund Fit</th>
            <th>Source</th>
            <th>Next Step</th>
            <th>Due</th>
            <th>Missing</th>
            <th>Last Update</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${items
            .map(
              (item) => `
                <tr data-select-pipeline-record="${item.id}">
                  <td class="company-cell"><strong>${item.organization_name}</strong><span class="muted">${item.record_code}</span></td>
                  <td>${item.record_code}</td>
                  <td><span class="pill stage-${item.stage}">${titleCaseStatus(item.stage)}</span></td>
                  <td><span class="pill status-${item.status}">${titleCaseStatus(item.status)}</span></td>
                  <td>${item.owner_name || "Unassigned"}</td>
                  <td>${item.fund_fit || "—"}</td>
                  <td>${titleCaseStatus(item.source_type || "—")}</td>
                  <td>${item.next_step || "—"}</td>
                  <td>${formatDate(item.next_step_due_at)}</td>
                  <td>${pipelineRecordMissingCount(item)}</td>
                  <td>${formatDate(item.updated_at || item.created_at)}</td>
                  <td>
                    <div class="table-actions">
                      <button class="button button-secondary" data-select-pipeline-record="${item.id}">Inspect</button>
                      <button class="button button-secondary" data-open-pipeline-record="${item.id}">Manage</button>
                      ${item.legacy_opportunity_id
                        ? `<button class="button button-primary" data-open-full-opportunity="${item.legacy_opportunity_id}">Legacy workflow</button>`
                        : `<span class="muted">No legacy workflow yet</span>`}
                    </div>
                  </td>
                </tr>
              `
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderKanbanBoard() {
  const grouped = filteredPipelineRecords().reduce((accumulator, item) => {
    accumulator[item.stage] = accumulator[item.stage] || [];
    accumulator[item.stage].push(item);
    return accumulator;
  }, {});
  return `
    <div class="kanban-board">
      ${(state.bootstrap?.pipeline_record_stages || []).map((stage) => {
        const items = grouped[stage] || [];
        return `
          <div class="kanban-column">
            <div class="panel-header">
              <div>
                <span class="eyebrow">${titleCaseStatus(stage)}</span>
                <h4>${items.length} records</h4>
              </div>
            </div>
            ${items.length
              ? items
                  .map(
                    (item) => `
                      <article class="kanban-card" data-select-pipeline-record="${item.id}">
                        <h5>${item.organization_name}</h5>
                        <p>${item.record_code} · ${item.owner_name || "Unassigned"}</p>
                        <p><span class="pill status-${item.status}">${titleCaseStatus(item.status)}</span></p>
                        <p>${item.next_step || "No next step"}</p>
                        <p>${pipelineRecordMissingCount(item)} missing inputs</p>
                      </article>
                    `
                  )
                  .join("")
              : `<div class="empty-state"><p>No records in ${titleCaseStatus(stage)}.</p></div>`}
          </div>
        `;
      }).join("")}
    </div>
  `;
}

function sideSummary(opportunity) {
  return `
    <div class="detail-stack">
      <div class="detail-card">
        <h4>Why this matters</h4>
        <p class="muted">${opportunity.investment_thesis || "No investment thesis written yet."}</p>
      </div>
      <div class="detail-card">
        <h4>Next step</h4>
        <p><strong>${opportunity.next_step || "Not set"}</strong></p>
        <p class="muted">Due ${formatDate(opportunity.next_step_due_at)}</p>
      </div>
      <div class="detail-card">
        <h4>Current status</h4>
        <p><span class="pill status-${opportunity.status}">${titleCaseStatus(opportunity.status)}</span></p>
      </div>
      <div class="detail-card">
        <h4>Immediate gaps</h4>
        ${
          opportunity.missing_fields.length
            ? opportunity.missing_fields.map((field) => `<span class="tag">${field}</span>`).join("")
            : `<p class="muted">No critical gaps for the current stage.</p>`
        }
      </div>
      <div class="detail-card">
        <h4>Recent activity</h4>
        <div class="timeline">
          ${opportunity.activities
            .slice(0, 3)
            .map(
              (item) => `
                <div class="timeline-item">
                  <strong>${item.summary}</strong>
                  <span class="muted">${formatDate(item.created_at)}</span>
                </div>
              `
            )
            .join("")}
        </div>
      </div>
    </div>
  `;
}

function renderOpportunityDetail() {
  const item = state.selectedOpportunity;
  if (!item) {
    return emptyState("Opportunity not found", "The selected opportunity could not be loaded.");
  }
  return `
    <div class="detail-stack">
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">${item.source_type.replaceAll("_", " ")}</span>
            <h2>${item.company_name}</h2>
            <p class="muted">${item.owner_name || "Unassigned"} · last activity ${formatDate(item.last_activity_at)}</p>
          </div>
          <div class="split-actions">
            <span class="pill stage-${item.stage}">${item.stage_label}</span>
            <span class="pill status-${item.status}">${titleCaseStatus(item.status)}</span>
            <span class="pill priority-${item.priority}">${item.priority_label} · ${item.priority_score}</span>
            <button class="button button-secondary" data-toggle-edit-opportunity>Edit</button>
            <button class="button button-secondary" data-export-opportunity="${item.id}">Export Report</button>
            <button class="button button-danger" data-delete-opportunity="${item.id}">Delete</button>
          </div>
        </div>
        <div class="detail-grid-main">
          <div class="detail-card">
            <h4>Company overview</h4>
            <p><strong>Sector:</strong> ${item.sector || "—"} / ${item.subsector || "—"}</p>
            <p><strong>Geography:</strong> ${item.geography || "—"} · ${item.hq_city || ""} ${item.hq_country || ""}</p>
            <p><strong>Business model:</strong> ${item.business_model || "—"}</p>
            <p><strong>Website:</strong> ${item.website ? `<a href="${item.website}" target="_blank">${item.website}</a>` : "—"}</p>
            <p><strong>Description:</strong> ${item.company_description || "No company description yet."}</p>
          </div>
          <div class="detail-card">
            <h4>Investment snapshot</h4>
            <p><strong>Round:</strong> ${item.round_name || "—"}</p>
            <p><strong>Ticket size:</strong> ${formatCurrency(item.ticket_size_target)}</p>
            <p><strong>Valuation:</strong> ${formatCurrency(item.valuation_min)} - ${formatCurrency(item.valuation_max)}</p>
            <p><strong>Ownership target:</strong> ${item.ownership_target_pct ? `${item.ownership_target_pct}%` : "—"}</p>
            <p><strong>Fund fit:</strong> ${item.fund_fit || "—"}</p>
            <p><strong>Source:</strong> ${item.source_detail}</p>
          </div>
          <div class="detail-card">
            <h4>Contacts</h4>
            <p><strong>${item.primary_contact_name || "No primary contact"}</strong></p>
            <p>${item.primary_contact_title || "—"}</p>
            <p>${item.primary_contact_email || "No email"}</p>
            <p>${item.primary_contact_phone || "No phone"}</p>
          </div>
          ${renderFinancialOverviewCard(item, { name: item.company_name })}
          <div class="detail-card">
            <h4>Deal status</h4>
            <p class="muted">This is now manual. Use the buttons below to set the operating status directly.</p>
            <div class="status-button-group">
              ${["active", "on_hold", "closed_won", "closed_lost"]
                .map(
                  (status) => `
                    <button
                      class="chip ${item.status === status ? "active" : ""}"
                      data-set-opportunity-status="${item.id}"
                      data-status-value="${status}"
                    >
                      ${titleCaseStatus(status)}
                    </button>
                  `
                )
                .join("")}
            </div>
          </div>
          <div class="detail-card">
            <h4>Workflow</h4>
            <label class="stack-field">
              <span>Change stage</span>
              <select id="changeStageSelect">
                ${state.bootstrap.stages.map((stage) => `<option value="${stage.id}" ${item.stage === stage.id ? "selected" : ""}>${stage.label}</option>`).join("")}
              </select>
            </label>
            <label class="stack-field">
              <span>Reason</span>
              <input id="changeStageReason" placeholder="Why are we moving this deal?" />
            </label>
            <button class="button button-primary" data-change-stage="${item.id}">Change Stage</button>
          </div>
        </div>
      </section>

      ${
        state.route === "new-opportunity" || state.editingOpportunity
          ? renderOpportunityForm()
          : ""
      }

      <div class="detail-grid">
        <section class="panel">
          <div class="panel-header">
            <div>
              <span class="eyebrow">Narrative</span>
              <h3>Thesis, concerns, and decisions</h3>
            </div>
          </div>
          <div class="detail-stack">
            <div class="detail-card">
              <h4>Investment thesis</h4>
              <p>${item.investment_thesis || "No thesis written yet."}</p>
            </div>
            <div class="detail-card">
              <h4>Key concerns</h4>
              <p>${item.key_concerns || "No concerns recorded yet."}</p>
            </div>
            <div class="detail-card">
              <h4>Decision history</h4>
              ${item.decision_logs.length
                ? item.decision_logs
                    .map(
                      (decision) => `
                        <div class="timeline-item">
                          <strong>${decision.decision_summary}</strong>
                          <p class="muted">${decision.rationale || "No rationale recorded."}</p>
                          <span class="muted">${decision.decided_by_name || "Unknown"} · ${formatDate(decision.created_at)}</span>
                        </div>
                      `
                    )
                    .join("")
                : `<p class="muted">No decisions logged yet.</p>`}
              <div class="quick-form">
                <div class="form-grid">
                  <label>
                    Decision type
                    <select id="decisionType">
                      <option value="advance">Advance</option>
                      <option value="hold">Hold</option>
                      <option value="pass">Pass</option>
                      <option value="term_sheet">Term Sheet</option>
                      <option value="close">Close</option>
                    </select>
                  </label>
                  <label class="full">
                    Summary
                    <input id="decisionSummary" placeholder="Record the actual decision" />
                  </label>
                  <label class="full">
                    Rationale
                    <textarea id="decisionRationale" rows="3" placeholder="Why was this decision made?"></textarea>
                  </label>
                </div>
                <button class="button button-secondary" data-add-decision="${item.id}">Log Decision</button>
              </div>
            </div>
            <div class="detail-card">
              <h4>Notes / comments</h4>
              ${item.notes.length
                ? item.notes
                    .map(
                      (note) => `
                        <div class="timeline-item">
                          <strong>${note.note_type}</strong>
                          <p>${note.body}</p>
                          <span class="muted">${note.author_name || "Unknown"} · ${formatDate(note.created_at)}</span>
                        </div>
                      `
                    )
                    .join("")
                : `<p class="muted">No analyst notes yet.</p>`}
              <div class="quick-form note-callout">
                <p class="muted">Notes should be intentional and readable in review, not silently appended to the activity feed.</p>
                <button class="button button-secondary" data-open-note-modal="${item.id}">Log analyst note</button>
              </div>
            </div>
          </div>
        </section>

        <section class="panel">
          <div class="panel-header">
            <div>
              <span class="eyebrow">Execution</span>
              <h3>Tasks, flags, activity, and documents</h3>
            </div>
          </div>
          <div class="detail-stack">
            <div class="detail-card">
              <h4>Risk flags and missing fields</h4>
              <div>${(item.risk_flags || []).map((flag) => `<span class="tag">${flag}</span>`).join("") || `<span class="muted">No risk flags.</span>`}</div>
              <div style="margin-top:8px">${(item.missing_fields || []).map((field) => `<span class="tag">${field}</span>`).join("") || `<span class="muted">No required fields missing.</span>`}</div>
            </div>
            <div class="detail-card">
              <h4>Tasks</h4>
              <div class="task-stack">
                ${item.tasks.length
                  ? item.tasks
                      .map(
                        (task) => `
                          <div class="task-item ${task.is_overdue ? "overdue" : ""}">
                            <div class="panel-header">
                              <div>
                                <strong>${task.title}</strong>
                                <p class="muted">${task.assignee_name || "Unassigned"} · due ${formatDate(task.due_at)}</p>
                              </div>
                              <div class="split-actions">
                                <span class="pill ${task.is_overdue ? "overdue" : ""}">${task.priority}</span>
                                <select data-task-status-select="${task.id}">
                                  ${state.bootstrap.task_statuses.map((status) => `<option value="${status}" ${task.status === status ? "selected" : ""}>${status}</option>`).join("")}
                                </select>
                              </div>
                            </div>
                            <p>${task.description || "No description."}</p>
                            ${task.comments.length ? `<p class="muted">Latest comment: ${task.comments[task.comments.length - 1].body}</p>` : ""}
                            <div class="quick-form">
                              <div class="form-grid">
                                <label class="full">
                                  Comment
                                  <input id="taskComment-${task.id}" placeholder="Add a task comment" />
                                </label>
                              </div>
                              <div class="split-actions">
                                <button class="button button-secondary" data-update-task="${task.id}">Save task</button>
                                <button class="button button-secondary" data-add-task-comment="${task.id}">Add comment</button>
                              </div>
                            </div>
                          </div>
                        `
                      )
                      .join("")
                  : `<p class="muted">No tasks yet.</p>`}
              </div>
              <div class="quick-form">
                <div class="form-grid">
                  <label>
                    Title
                    <input id="taskTitle" placeholder="Create analyst task" />
                  </label>
                  <label>
                    Assignee
                    <select id="taskAssignee">
                      <option value="">Unassigned</option>
                      ${state.bootstrap.users.map((user) => `<option value="${user.id}">${user.name}</option>`).join("")}
                    </select>
                  </label>
                  <label>
                    Priority
                    <select id="taskPriority">
                      ${state.bootstrap.task_priorities.map((priority) => `<option value="${priority}">${priority}</option>`).join("")}
                    </select>
                  </label>
                  <label>
                    Due date
                    <input id="taskDueDate" type="date" />
                  </label>
                  <label class="full">
                    Description
                    <textarea id="taskDescription" rows="3" placeholder="What should be done?"></textarea>
                  </label>
                </div>
                <button class="button button-secondary" data-add-task="${item.id}">Add Task</button>
              </div>
            </div>
            <div class="detail-card">
              <h4>Activity log</h4>
              <div class="timeline">
                ${item.activities
                  .map(
                    (activity) => `
                      <div class="timeline-item">
                        <strong>${activity.summary}</strong>
                        <span class="muted">${activity.user_name || "Unknown"} · ${formatDate(activity.created_at)}</span>
                      </div>
                    `
                  )
                  .join("")}
              </div>
            </div>
            <div class="detail-card">
              <h4>Stage history</h4>
              <div class="timeline">
                ${item.stage_history.length
                  ? item.stage_history
                      .map(
                        (entry) => `
                          <div class="timeline-item">
                            <strong>${labelStage(entry.to_stage)}</strong>
                            <p class="muted">${entry.from_stage ? `${labelStage(entry.from_stage)} -> ` : ""}${labelStage(entry.to_stage)}</p>
                            <p class="muted">${entry.reason || "No reason recorded."}</p>
                            <span class="muted">${entry.changed_by_name || "Unknown"} · ${formatDate(entry.created_at)}</span>
                          </div>
                        `
                      )
                      .join("")
                  : `<p class="muted">No stage movements logged yet.</p>`}
              </div>
            </div>
            <div class="detail-card">
              <h4>Documents</h4>
              ${item.documents.length
                ? item.documents
                    .map(
                      (document) => `
                        <div class="timeline-item">
                          <strong>${document.file_name}</strong>
                          <p class="muted">${document.document_category} · <a href="${document.storage_path}" target="_blank">${document.storage_path}</a></p>
                        </div>
                      `
                    )
                    .join("")
                : `<p class="muted">No documents attached yet.</p>`}
              <div class="quick-form">
                <div class="form-grid">
                  <label>
                    Document name
                    <input id="documentName" placeholder="Pitch deck" />
                  </label>
                  <label>
                    Category
                    <select id="documentCategory">
                      <option value="deck">Deck</option>
                      <option value="financials">Financials</option>
                      <option value="memo">Memo</option>
                      <option value="nda">NDA</option>
                      <option value="data_room">Data room</option>
                      <option value="other">Other</option>
                    </select>
                  </label>
                  <label class="full">
                    Upload shared file
                    <input id="documentFile" type="file" />
                  </label>
                  <label class="full">
                    Shared URL or reference path
                    <input id="documentPath" placeholder="https://... or /shared-documents/..." />
                  </label>
                </div>
                <p class="muted">Upload a pitch deck, financials, memo, or any other file and it will be available to everyone using this platform.</p>
                <button class="button button-secondary" data-add-document="${item.id}">Add Document</button>
              </div>
            </div>
          </div>
        </section>
      </div>
    </div>
  `;
}

function renderModal() {
  if (!state.modal) {
    modalHost.innerHTML = "";
    return;
  }
  if (state.modal.type === "company-detail") {
    const context = companyDetailContext(state.modal.companyId);
    if (!context) {
      modalHost.innerHTML = "";
      return;
    }
    const { company, organization, linkedRecords, linkedDocuments, activeRecord } = context;
    const companyName = companyDisplayName(context);
    const website = activeRecord?.website || organization?.website || "";
    const location = [activeRecord?.hq_city || organization?.hq_city, activeRecord?.hq_country || organization?.hq_country || company.geography]
      .filter(Boolean)
      .join(", ");
    const overviewSubject = activeRecord || organization || company;
    const deckCount = filterDocumentsByCategory(linkedDocuments, ["deck"]).length;
    const financialCount = filterDocumentsByCategory(linkedDocuments, ["financials"]).length;
    modalHost.innerHTML = `
      <div class="modal-backdrop" data-close-modal>
        <div class="modal-card modal-card-wide" role="dialog" aria-modal="true" aria-labelledby="companyDetailModalTitle">
          <div class="panel-header">
            <div>
              <span class="eyebrow">Company detail</span>
              <h3 id="companyDetailModalTitle">${companyName}</h3>
              <p class="muted">${company.owner_name || organization?.owner_name || "Unassigned"} · ${company.sector || organization?.sector_primary || "—"} · ${company.geography || organization?.geography || "—"}</p>
            </div>
            <div class="split-actions">
              <span class="pill">${labelStage(company.current_stage) || "No active stage"}</span>
              <button class="button button-secondary" data-open-company-edit="${company.id}">Edit</button>
              <button class="button button-ghost" data-close-modal>Close</button>
            </div>
          </div>
          <div class="company-detail-modal-body">
            <div class="company-detail-modal-grid">
              <div class="detail-card">
                <h4>Company overview</h4>
                <p><strong>Description:</strong> ${activeRecord?.company_description || organization?.description || "No company description yet."}</p>
                <p><strong>Website:</strong> ${website ? `<a href="${website}" target="_blank">${website}</a>` : "—"}</p>
                <p><strong>Coverage:</strong> ${linkedRecords.length} linked pipeline record${linkedRecords.length === 1 ? "" : "s"}${organization ? ` · canonical org ${organization.name}` : " · no canonical organization linked yet"}</p>
                <p><strong>Location:</strong> ${location || "—"}</p>
              </div>
              <div class="detail-card">
                <h4>Company materials</h4>
                <p class="muted">${linkedDocuments.length ? `${linkedDocuments.length} shared document${linkedDocuments.length === 1 ? "" : "s"} available across linked records.` : "No shared company materials linked yet."}</p>
                <div class="list-stack">
                  <div class="summary-block">
                    <strong>Decks</strong>
                    <p class="muted">${deckCount} linked</p>
                  </div>
                  <div class="summary-block">
                    <strong>Financials</strong>
                    <p class="muted">${financialCount} linked</p>
                  </div>
                  <div class="summary-block">
                    <strong>Supporting files</strong>
                    <p class="muted">${Math.max(linkedDocuments.length - deckCount - financialCount, 0)} linked</p>
                  </div>
                </div>
              </div>
              ${renderFinancialOverviewCard(overviewSubject, { name: company.name })}
              ${renderMaterialsHub("Company materials", linkedDocuments, {
                intro: "This focused company view keeps the latest deck, finance package, and supporting files together without leaving the Companies tab.",
              })}
              <div class="detail-card">
                <h4>Linked workflow</h4>
                ${
                  linkedRecords.length
                    ? `
                      <div class="list-stack">
                        ${linkedRecords
                          .slice(0, 5)
                          .map(
                            (record) => `
                              <div class="summary-block">
                                <div class="panel-header">
                                  <div>
                                    <strong>${record.record_code || `Record ${record.id}`}</strong>
                                    <p class="muted">${titleCaseStatus(record.stage)} · ${titleCaseStatus(record.status)} · ${record.owner_name || "Unassigned"}</p>
                                  </div>
                                  <button class="button button-secondary" data-open-pipeline-record="${record.id}">Open record</button>
                                </div>
                                <p class="muted">${record.next_step || "No next step yet."}</p>
                              </div>
                            `
                          )
                          .join("")}
                      </div>
                    `
                    : `<p class="muted">No linked pipeline records yet. The company detail view will deepen automatically once a canonical record exists.</p>`
                }
                ${organization ? `<div class="footer-actions"><button class="button button-secondary" data-open-organization="${organization.id}">Open canonical organization</button></div>` : ""}
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
    return;
  }
  if (state.modal.type === "company-edit") {
    const context = companyDetailContext(state.modal.companyId);
    if (!context) {
      modalHost.innerHTML = "";
      return;
    }
    modalHost.innerHTML = `
      <div class="modal-backdrop" data-close-modal>
        <div class="modal-card modal-card-wide modal-card-edit" role="dialog" aria-modal="true" aria-labelledby="companyEditModalTitle">
          <div class="panel-header">
            <div>
              <span class="eyebrow">Edit company</span>
              <h3 id="companyEditModalTitle">${companyDisplayName(context)}</h3>
              <p class="muted">Save the shared company overview on the canonical organization and the material-driven metrics on the most recent linked pipeline record.</p>
            </div>
            <div class="split-actions">
              <button class="button button-secondary" data-open-company-detail="${state.modal.companyId}">Cancel</button>
              <button class="button button-primary" data-save-company-edit="${state.modal.companyId}">Save changes</button>
            </div>
          </div>
          <div class="company-detail-modal-body">
            ${renderCompanyEditForm(context)}
          </div>
        </div>
      </div>
    `;
    return;
  }
  if (state.modal.type === "note") {
    const opportunity = state.selectedOpportunity;
    modalHost.innerHTML = `
      <div class="modal-backdrop" data-close-modal>
        <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="noteModalTitle">
          <div class="panel-header">
            <div>
              <span class="eyebrow">Analyst note</span>
              <h3 id="noteModalTitle">${opportunity?.company_name || "Opportunity"}</h3>
            </div>
            <button class="button button-ghost" data-close-modal>Close</button>
          </div>
          <div class="form-grid">
            <label>
              Note type
              <select id="modalNoteType">
                ${state.bootstrap.note_types
                  .map((type) => `<option value="${type}" ${state.modal.noteType === type ? "selected" : ""}>${type}</option>`)
                  .join("")}
              </select>
            </label>
            <label class="full">
              Note
              <textarea id="modalNoteBody" rows="7" placeholder="Write the note you actually want saved in the deal record.">${state.modal.body || ""}</textarea>
            </label>
          </div>
          <div class="footer-actions">
            <button class="button button-secondary" data-close-modal>Cancel</button>
            <button class="button button-primary" data-save-note="${state.modal.opportunityId}">Save note</button>
          </div>
        </div>
      </div>
    `;
    return;
  }
  modalHost.innerHTML = "";
}

function renderNewOpportunityPage() {
  return renderOpportunityForm();
}

function renderOpportunityForm() {
  const form = state.opportunityForm || blankOpportunityForm();
  const errors = state.opportunityFormErrors;
  const currentSection = state.intakeSection;
  const isCanonicalCreate = state.route === "new-opportunity";
  return `
    <section class="panel">
      <div class="panel-header">
        <div>
          <span class="eyebrow">${isCanonicalCreate ? "Canonical intake" : "Edit opportunity"}</span>
          <h3>${isCanonicalCreate ? "New canonical opportunity" : "Edit details"}</h3>
        </div>
        <span class="muted">${isCanonicalCreate ? "This creates a canonical organization and pipeline record first." : "Save draft, then advance only when data is strong enough."}</span>
      </div>
      <div class="form-shell">
        <div class="section-steps">
          ${opportunitySections
            .map(
              (section) => `
                <button class="${currentSection === section.id ? "active" : ""}" data-intake-section="${section.id}">${section.label}</button>
              `
            )
            .join("")}
        </div>
        <form id="opportunityForm">
          ${renderFormSection(currentSection, form, errors)}
          <div class="footer-actions">
            <div class="split-actions">
              <button type="button" class="button button-secondary" data-prev-section>Previous</button>
              <button type="button" class="button button-secondary" data-next-section>Next</button>
            </div>
            <div class="split-actions">
              <button type="button" class="button button-secondary" data-cancel-opportunity-form>Cancel</button>
              <button type="submit" class="button button-primary">${isCanonicalCreate ? "Create Canonical Record" : "Save"}</button>
            </div>
          </div>
        </form>
      </div>
    </section>
  `;
}

function fieldError(errors, field) {
  return errors[field] ? `<span class="field-error">${errors[field]}</span>` : "";
}

function renderFormSection(section, form, errors) {
  if (section === "materials") {
    const selectedFiles = state.opportunitySourceFiles || emptyOpportunitySourceFiles();
    return `
      <div class="detail-card">
        <h4>Load company materials first</h4>
        <p class="muted">Start with the deck, financials, and supporting files. After you create the canonical record, the system will attach these materials and autofill blank fields from them automatically.</p>
      </div>
      <div class="form-grid">
        <label class="full">
          Company website
          <input name="website" value="${form.website || ""}" placeholder="https://company.com" />
        </label>
        <label class="full">
          Pitch deck files
          <input id="opportunityDeckFiles" type="file" multiple accept=".pdf,.ppt,.pptx,.key" />
          ${renderOpportunityFileSummary(selectedFiles.deck, "No deck files selected yet.")}
        </label>
        <label class="full">
          Pitch deck links
          <textarea name="deck_links" rows="3" placeholder="One deck URL or shared path per line">${form.deck_links || ""}</textarea>
        </label>
        <label class="full">
          Financial files
          <input id="opportunityFinancialFiles" type="file" multiple accept=".pdf,.xls,.xlsx,.csv,.tsv" />
          ${renderOpportunityFileSummary(selectedFiles.financials, "No financial files selected yet.")}
        </label>
        <label class="full">
          Financial links
          <textarea name="financial_links" rows="3" placeholder="One financial package URL or shared path per line">${form.financial_links || ""}</textarea>
        </label>
        <label class="full">
          Other supporting files
          <input id="opportunityOtherFiles" type="file" multiple />
          ${renderOpportunityFileSummary(selectedFiles.other, "No additional files selected yet.")}
        </label>
        <label class="full">
          Other supporting links
          <textarea name="material_links" rows="3" placeholder="Data room, memo, product docs, diligence files, or any shared reference">${form.material_links || ""}</textarea>
        </label>
        <div class="full footer-actions">
          <p class="muted">${state.opportunityPrefillPending ? "Analyzing website and documents..." : "The draft will prefill automatically after you add materials. You can also trigger it manually here."}</p>
          <button type="button" class="button button-secondary" data-run-opportunity-prefill ${state.opportunityPrefillPending ? "disabled" : ""}>Analyze materials now</button>
        </div>
      </div>
    `;
  }
  if (section === "company") {
    return `
      <div class="form-grid">
        <label>
          Company name
          <input name="company_name" value="${form.company_name || ""}" />
          ${fieldError(errors, "company_name")}
        </label>
        <label>
          Sector
          <input name="sector" value="${form.sector || ""}" />
          ${fieldError(errors, "sector")}
        </label>
        <label>
          Subsector
          <input name="subsector" value="${form.subsector || ""}" />
        </label>
        <label>
          Geography
          <input name="geography" value="${form.geography || ""}" />
          ${fieldError(errors, "geography")}
        </label>
        <label>
          HQ city
          <input name="hq_city" value="${form.hq_city || ""}" />
        </label>
        <label>
          HQ country
          <input name="hq_country" value="${form.hq_country || ""}" />
        </label>
        <label>
          Business model
          <input name="business_model" value="${form.business_model || ""}" />
        </label>
        <label class="full">
          Description
          <textarea name="company_description" rows="4">${form.company_description || ""}</textarea>
        </label>
      </div>
    `;
  }
  if (section === "contacts") {
    return `
      <div class="form-grid">
        <label>
          Primary contact name
          <input name="primary_contact_name" value="${form.primary_contact_name || ""}" />
          ${fieldError(errors, "primary_contact_name")}
        </label>
        <label>
          Title
          <input name="primary_contact_title" value="${form.primary_contact_title || ""}" />
        </label>
        <label>
          Email
          <input name="primary_contact_email" value="${form.primary_contact_email || ""}" />
        </label>
        <label>
          Phone
          <input name="primary_contact_phone" value="${form.primary_contact_phone || ""}" />
        </label>
      </div>
    `;
  }
  if (section === "deal") {
    return `
      <div class="form-grid">
        <label>
          Stage
          <select name="stage">
            ${state.bootstrap.stages.map((stage) => `<option value="${stage.id}" ${form.stage === stage.id ? "selected" : ""}>${stage.label}</option>`).join("")}
          </select>
          ${fieldError(errors, "stage")}
        </label>
        <label>
          Priority
          <select name="priority">
            ${state.bootstrap.priorities.map((priority) => `<option value="${priority}" ${form.priority === priority ? "selected" : ""}>${priority}</option>`).join("")}
          </select>
        </label>
        <label>
          Owner
          <select name="owner_user_id">
            <option value="">Unassigned</option>
            ${state.bootstrap.users.map((user) => `<option value="${user.id}" ${String(form.owner_user_id) === String(user.id) ? "selected" : ""}>${user.name}</option>`).join("")}
          </select>
        </label>
        <label>
          Source type
          <select name="source_type">
            ${["inbound", "outbound", "network", "partner_referral", "conference", "desk_research"].map((type) => `<option value="${type}" ${form.source_type === type ? "selected" : ""}>${type.replaceAll("_", " ")}</option>`).join("")}
          </select>
        </label>
        <label class="full">
          Source detail
          <input name="source_detail" value="${form.source_detail || ""}" />
          ${fieldError(errors, "source_detail")}
        </label>
        <label>
          Round
          <input name="round_name" value="${form.round_name || ""}" />
        </label>
        <label>
          Fund fit
          <input name="fund_fit" value="${form.fund_fit || ""}" />
        </label>
      </div>
    `;
  }
  if (section === "fit") {
    return `
      <div class="form-grid">
        <label>
          Ticket target
          <input name="ticket_size_target" value="${form.ticket_size_target || ""}" />
        </label>
        <label>
          Ownership target %
          <input name="ownership_target_pct" value="${form.ownership_target_pct || ""}" />
        </label>
        <label>
          Valuation min
          <input name="valuation_min" value="${form.valuation_min || ""}" />
        </label>
        <label>
          Valuation max
          <input name="valuation_max" value="${form.valuation_max || ""}" />
        </label>
        <label>
          Revenue / ARR
          <input name="annual_recurring_revenue" type="number" step="0.01" value="${inputValue(form.annual_recurring_revenue)}" />
        </label>
        <label>
          Revenue growth %
          <input name="revenue_growth_pct" type="number" step="0.1" value="${inputValue(form.revenue_growth_pct)}" />
        </label>
        <label>
          Gross margin %
          <input name="gross_margin_pct" type="number" step="0.1" value="${inputValue(form.gross_margin_pct)}" />
        </label>
        <label>
          EBITDA margin %
          <input name="ebitda_margin_pct" type="number" step="0.1" value="${inputValue(form.ebitda_margin_pct)}" />
        </label>
        <label>
          Rule of 40 %
          <input name="rule_of_40_pct" type="number" step="0.1" value="${inputValue(form.rule_of_40_pct)}" />
        </label>
        <label>
          Monthly burn
          <input name="monthly_burn" type="number" step="0.01" value="${inputValue(form.monthly_burn)}" />
        </label>
        <label>
          Cash runway months
          <input name="cash_runway_months" type="number" step="0.1" value="${inputValue(form.cash_runway_months)}" />
        </label>
        <label>
          Financials last updated
          <input name="financials_updated_at" type="date" value="${form.financials_updated_at || ""}" />
        </label>
        <label class="full">
          Investment thesis
          <textarea name="investment_thesis" rows="4">${form.investment_thesis || ""}</textarea>
        </label>
        <label class="full">
          Key concerns
          <textarea name="key_concerns" rows="4">${form.key_concerns || ""}</textarea>
        </label>
      </div>
    `;
  }
  if (section === "risks") {
    return `
      <div class="form-grid">
        <label class="full">
          Relationship / conflict notes
          <textarea name="relationship_notes" rows="3">${form.relationship_notes || ""}</textarea>
        </label>
        <label>
          NDA required
          <select name="nda_required">
            <option value="0" ${String(form.nda_required) === "0" ? "selected" : ""}>No</option>
            <option value="1" ${String(form.nda_required) === "1" ? "selected" : ""}>Yes</option>
          </select>
        </label>
        <label>
          NDA status
          <select name="nda_status">
            ${["not_required", "awaiting_signature", "signed"].map((status) => `<option value="${status}" ${form.nda_status === status ? "selected" : ""}>${status.replaceAll("_", " ")}</option>`).join("")}
          </select>
        </label>
        <label class="full">
          Risk flags (comma separated)
          <input name="risk_flags" value="${(form.risk_flags || []).join(", ")}" />
        </label>
        <label class="full">
          Tags (comma separated)
          <input name="tags" value="${(form.tags || []).join(", ")}" />
        </label>
      </div>
    `;
  }
  return `
    <div class="form-grid">
      <label class="full">
        Next step
        <input name="next_step" value="${form.next_step || ""}" />
      </label>
      <label>
        Next step due
        <input name="next_step_due_at" type="date" value="${form.next_step_due_at || ""}" />
      </label>
      <label>
        Decision due
        <input name="decision_due_at" type="date" value="${form.decision_due_at || ""}" />
      </label>
      <div class="full detail-card">
        <h4>Readiness review</h4>
        <p class="muted">${state.route === "new-opportunity"
          ? "Submitting now creates the canonical organization and pipeline record, attaches the materials from the first step, and autofills blank canonical fields from those sources."
          : "Saving now stores the opportunity locally in SQLite. Stage advancement will stay blocked if required fields for the target stage are missing."}</p>
      </div>
    </div>
  `;
}

function renderTasks() {
  return `
    <section class="panel">
      <div class="panel-header">
        <div>
          <span class="eyebrow">Execution queue</span>
          <h3>Tasks</h3>
        </div>
      </div>
      <div class="filter-bar">
        <select id="tasksAssigneeFilter">
          <option value="all">All assignees</option>
          ${state.bootstrap.users.map((user) => `<option value="${user.id}" ${String(state.taskFilters.assigned_user_id) === String(user.id) ? "selected" : ""}>${user.name}</option>`).join("")}
        </select>
        <select id="tasksStatusFilter">
          <option value="all">All statuses</option>
          ${state.bootstrap.task_statuses.map((status) => `<option value="${status}" ${state.taskFilters.status === status ? "selected" : ""}>${status}</option>`).join("")}
        </select>
        <select id="tasksPriorityFilter">
          <option value="all">All priorities</option>
          ${state.bootstrap.task_priorities.map((priority) => `<option value="${priority}" ${state.taskFilters.priority === priority ? "selected" : ""}>${priority}</option>`).join("")}
        </select>
        <label class="inline-field">
          <input type="checkbox" id="tasksOverdueOnly" ${state.taskFilters.overdue_only === "1" ? "checked" : ""}/>
          <span>Overdue only</span>
        </label>
      </div>
      ${
        state.tasks.length
          ? `
            <div class="task-stack">
              ${state.tasks
                .map(
                  (task) => `
                    <div class="task-item ${task.is_overdue ? "overdue" : ""}">
                      <div class="panel-header">
                        <div>
                          <strong>${task.title}</strong>
                          <p class="muted">${task.company_name} · ${task.assignee_name || "Unassigned"} · due ${formatDate(task.due_at)}</p>
                        </div>
                        <div class="split-actions">
                          <span class="pill ${task.is_overdue ? "overdue" : ""}">${task.priority}</span>
                          <span class="pill">${task.status.replaceAll("_", " ")}</span>
                        </div>
                      </div>
                      <p>${task.description || "No description."}</p>
                      <p class="muted">${task.latest_comment || "No comments yet."}</p>
                      <div class="split-actions">
                        <button class="button button-secondary" ${openEntityActionAttributes(task)}>${openEntityActionLabel(task)}</button>
                      </div>
                    </div>
                  `
                )
                .join("")}
            </div>
          `
          : emptyState("No tasks match these filters", "Try broadening the task filters.")
      }
    </section>
  `;
}

function renderCompanies() {
  return `
    <section class="panel">
      <div class="panel-header">
        <div>
          <span class="eyebrow">Coverage</span>
          <h3>Companies</h3>
        </div>
      </div>
      <div class="list-stack">
        ${state.companies
          .map((company) => {
            const organization = linkedOrganizationForCompany(company);
            const linkedRecords = organization ? relatedPipelineRecords(organization.id) : [];
            const linkedDocuments = linkedRecords.flatMap((item) => pipelineRecordDocuments(item));
            const deckDocuments = filterDocumentsByCategory(linkedDocuments, ["deck"]);
            const financialDocuments = filterDocumentsByCategory(linkedDocuments, ["financials"]);
            return `
              <div class="company-card company-card-button" data-open-company-detail="${company.id}" role="button" tabindex="0" aria-label="Open ${company.name} details">
                <div class="panel-header">
                  <div>
                    <strong>${company.name}</strong>
                    <p class="muted">${company.sector || "—"} · ${company.geography || "—"}</p>
                  </div>
                  <span class="pill">${labelStage(company.current_stage) || "No active stage"}</span>
                </div>
                <p class="muted">Owner: ${company.owner_name || "Unassigned"} · ${company.total_opportunities} tracked opportunity records</p>
                <p class="muted">Materials room: ${deckDocuments.length ? `${deckDocuments.length} deck file${deckDocuments.length === 1 ? "" : "s"} ready` : "pitch deck slot ready"} · ${financialDocuments.length ? `${financialDocuments.length} financial file${financialDocuments.length === 1 ? "" : "s"} ready` : "financials slot ready"}</p>
                <p class="muted">Financial overview: this card is ready for revenue, growth, margin, and runway once the company starts sharing numbers.</p>
              </div>
            `
          })
          .join("")}
      </div>
    </section>
  `;
}

function renderIntake() {
  return `
    <div class="tasks-layout">
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Operating system</span>
            <h3>New intake submission</h3>
          </div>
        </div>
        <form id="intakeSubmissionForm" class="quick-form">
          <div class="form-grid">
            <label>
              Company name
              <input name="company_name" required />
            </label>
            <label>
              Source type
              <select name="source_type">
                <option value="inbound">inbound</option>
                <option value="network">network</option>
                <option value="partner_referral">partner referral</option>
                <option value="conference">conference</option>
                <option value="desk_research">desk research</option>
              </select>
            </label>
            <label>
              Intake kind
              <select name="intake_kind">
                <option value="direct_deal">direct deal</option>
                <option value="co_investment">co-investment</option>
                <option value="relationship_first">relationship first</option>
              </select>
            </label>
            <label>
              Owner
              <select name="owner_user_id">
                <option value="">Unassigned</option>
                ${state.bootstrap.users.map((user) => `<option value="${user.id}" ${user.id === Number(state.currentUserId) ? "selected" : ""}>${user.name}</option>`).join("")}
              </select>
            </label>
            <label>
              Submitted by
              <input name="submitted_by_name" />
            </label>
            <label>
              Email
              <input name="submitted_by_email" />
            </label>
            <label class="full">
              Summary
              <textarea name="summary" rows="3" placeholder="Why this came in and what it might become."></textarea>
            </label>
            <label class="full">
              Notes
              <textarea name="notes" rows="3" placeholder="Optional internal notes."></textarea>
            </label>
          </div>
          <div class="footer-actions">
            <button type="submit" class="button button-primary">Create Intake Item</button>
          </div>
        </form>
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Queue</span>
            <h3>Intake submissions</h3>
          </div>
        </div>
        <div class="list-stack">
          ${state.intakeSubmissions.length
            ? state.intakeSubmissions.map((item) => `
              <div class="company-card">
                <div class="panel-header">
                  <div>
                    <strong>${item.company_name}</strong>
                    <p class="muted">${titleCaseStatus(item.intake_kind)} · ${item.owner_name || "Unassigned"} · ${formatDate(item.created_at)}</p>
                  </div>
                  <span class="pill">${titleCaseStatus(item.status)}</span>
                </div>
                <p class="muted">${item.summary || "No summary yet."}</p>
                <p class="muted">Outcome: ${titleCaseStatus(item.outcome)}${item.converted_deal_code ? ` · Converted to ${item.converted_deal_code}` : ""}</p>
              </div>
            `).join("")
            : emptyState("No intake items yet", "Create the first intake item to start separating raw inbound from real pipeline records.")}
        </div>
      </section>
    </div>
  `;
}

function renderRelationships() {
  const legacyOrganizations = legacyOrganizationOptions();
  const legacyPipelineRecords = legacyPipelineRecordOptions();
  return `
    <div class="tasks-layout">
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Network graph</span>
            <h3>New relationship link</h3>
          </div>
        </div>
        <form id="relationshipLinkForm" class="quick-form">
          <div class="form-grid">
            <label>
              Anchor company
              <select name="company_id">
                <option value="">None</option>
                ${legacyOrganizations.map((company) => `<option value="${company.value}">${company.label}</option>`).join("")}
              </select>
            </label>
            <label>
              Anchor opportunity
              <select name="opportunity_id">
                <option value="">None</option>
                ${legacyPipelineRecords.map((item) => `<option value="${item.value}">${item.label}</option>`).join("")}
              </select>
            </label>
            <label>
              Related company
              <select name="related_company_id" required>
                <option value="">Select related company</option>
                ${legacyOrganizations.map((company) => `<option value="${company.value}">${company.label}</option>`).join("")}
              </select>
            </label>
            <label>
              Link type
              <select name="link_type">
                ${state.bootstrap.relationship_link_types.map((type) => `<option value="${type}">${type.replaceAll("_", " ")}</option>`).join("")}
              </select>
            </label>
            <label>
              Status
              <select name="relationship_status">
                ${state.bootstrap.relationship_statuses.map((status) => `<option value="${status}">${status.replaceAll("_", " ")}</option>`).join("")}
              </select>
            </label>
            <label>
              Owner
              <select name="owner_user_id">
                <option value="">Unassigned</option>
                ${state.bootstrap.users.map((user) => `<option value="${user.id}" ${user.id === Number(state.currentUserId) ? "selected" : ""}>${user.name}</option>`).join("")}
              </select>
            </label>
            <label>
              Warmth
              <input name="warmth" placeholder="warm / strong / cautious" />
            </label>
            <label>
              Next touch
              <input type="date" name="next_touch_at" />
            </label>
            <label class="full">
              Notes
              <textarea name="notes" rows="3" placeholder="What makes this relationship useful or sensitive?"></textarea>
            </label>
          </div>
          <div class="footer-actions">
            <button type="submit" class="button button-primary">Create Relationship Link</button>
          </div>
        </form>
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Mapped links</span>
            <h3>Relationships</h3>
          </div>
        </div>
        <div class="list-stack">
          ${state.relationshipLinks.length
            ? state.relationshipLinks.map((item) => `
              <div class="company-card">
                <div class="panel-header">
                  <div>
                    <strong>${item.company_name || item.opportunity_deal_code || "Anchor"}</strong>
                    <p class="muted">${titleCaseStatus(item.link_type)} -> ${item.related_company_name || item.related_contact_name || "Unknown"}</p>
                  </div>
                  <span class="pill">${titleCaseStatus(item.relationship_status)}</span>
                </div>
                <p class="muted">Owner: ${item.owner_name || "Unassigned"}${item.warmth ? ` · Warmth: ${item.warmth}` : ""}${item.next_touch_at ? ` · Next touch ${formatDate(item.next_touch_at)}` : ""}</p>
                <p class="muted">${item.notes || "No notes yet."}</p>
              </div>
            `).join("")
            : emptyState("No relationship links yet", "Start capturing introducers, co-investors, advisors, and internal sponsors separately from deals.")}
        </div>
      </section>
    </div>
  `;
}

function renderOrganizations() {
  const activeOrganization = selectedOrganization() || state.organizations[0] || null;
  const organizationRecords = activeOrganization ? relatedPipelineRecords(activeOrganization.id) : [];
  const organizationDocuments = organizationRecords.flatMap((item) => pipelineRecordDocuments(item));
  return `
    <div class="tasks-layout">
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Canonical entities</span>
            <h3>Organizations</h3>
          </div>
        </div>
        <div class="grid-metrics">
          <article class="metric-card"><span class="eyebrow">Organizations</span><strong>${state.organizations.length}</strong></article>
          <article class="metric-card"><span class="eyebrow">Selected linked records</span><strong>${organizationRecords.length}</strong></article>
          <article class="metric-card"><span class="eyebrow">Missing website or sector</span><strong>${state.organizations.filter((item) => !item.website || !item.sector_primary).length}</strong></article>
          <article class="metric-card"><span class="eyebrow">Unassigned owners</span><strong>${state.organizations.filter((item) => !item.owner_user_id).length}</strong></article>
        </div>
        <div class="list-stack">
          ${state.organizations.length
            ? state.organizations.map((item) => `
              <div class="company-card" data-select-organization="${item.id}">
                <div class="panel-header">
                  <div>
                    <strong>${item.name}</strong>
                    <p class="muted">${titleCaseStatus(item.organization_type)} · ${item.sector_primary || "—"} · ${item.geography || "—"}</p>
                  </div>
                  <span class="pill">${titleCaseStatus(item.relationship_status)}</span>
                </div>
                <p class="muted">Owner: ${item.owner_name || "Unassigned"} · ${item.pipeline_record_count} pipeline records${item.legacy_company_id ? ` · legacy company ${item.legacy_company_id}` : ""}</p>
              </div>
            `).join("")
            : emptyState("No organizations yet", "Organizations are the canonical entities that pipeline records attach to.")}
        </div>
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Selected entity</span>
            <h3>${activeOrganization ? activeOrganization.name : "No organization selected"}</h3>
          </div>
        </div>
        ${activeOrganization
          ? `
            <div class="detail-stack">
              <div class="detail-card">
                <h4>${titleCaseStatus(activeOrganization.organization_type)}</h4>
                <p class="muted">${activeOrganization.description || "No description captured yet."}</p>
                <p class="muted">${activeOrganization.website || "No website"} · ${activeOrganization.hq_country || activeOrganization.geography || "No HQ set"}</p>
                <p class="muted">Owner: ${activeOrganization.owner_name || "Unassigned"} · Status: ${titleCaseStatus(activeOrganization.relationship_status)}</p>
              </div>
              <div class="detail-card">
                <h4>Primary contact</h4>
                <p><strong>${activeOrganization.primary_contact?.name || "No primary contact"}</strong></p>
                <p class="muted">${activeOrganization.primary_contact?.title || "—"}</p>
                <p class="muted">${activeOrganization.primary_contact?.email || "No email"}</p>
                <p class="muted">${activeOrganization.primary_contact?.phone || "No phone"}</p>
              </div>
              <div class="detail-card">
                <h4>Linked pipeline records</h4>
                ${
                  organizationRecords.length
                    ? `
                      <div class="list-stack">
                        ${organizationRecords.map((item) => `
                          <div class="summary-block" data-select-pipeline-record="${item.id}">
                            <strong>${item.record_code}</strong>
                            <p class="muted">${titleCaseStatus(item.stage)} · ${titleCaseStatus(item.status)} · ${item.owner_name || "Unassigned"}</p>
                            <p class="muted">${item.next_step || "No next step yet."}</p>
                          </div>
                        `).join("")}
                      </div>
                    `
                    : `<p class="muted">No linked pipeline records yet.</p>`
                }
              </div>
              ${renderFinancialOverviewCard(activeOrganization, { name: activeOrganization.name })}
              ${renderMaterialsHub("Shared materials", organizationDocuments, {
                intro: "This is the organization-level view of the files your team should expect to find here over time: deck, financials, and supporting diligence.",
              })}
              <div class="detail-card">
                <h4>Edit company profile</h4>
                <p class="muted">Keep the canonical company profile current here. Workflow-specific fields still live on the linked pipeline record.</p>
                ${renderOrganizationForm(activeOrganization)}
              </div>
            </div>
          `
          : `
            <div class="detail-stack">
              <div class="detail-card">
                <h4>No organization selected</h4>
                <p class="muted">Create the first canonical organization to start the operating-system layer.</p>
              </div>
              <div class="detail-card">
                <h4>Create organization</h4>
                ${renderOrganizationForm()}
              </div>
            </div>
          `}
      </section>
    </div>
  `;
}

function renderPipelineRecords() {
  const activeRecord = selectedPipelineRecord() || state.pipelineRecords[0] || null;
  return `
    <div class="tasks-layout">
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Operating system</span>
            <h3>New pipeline record</h3>
          </div>
        </div>
        ${renderPipelineRecordForm()}
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Canonical workflow</span>
            <h3>Pipeline records</h3>
          </div>
        </div>
        ${activeRecord
          ? `
            <div class="detail-stack" style="margin-bottom:16px">
              <div class="detail-card">
                <h4>${activeRecord.organization_name}</h4>
                <p class="muted">${activeRecord.record_code} · ${titleCaseStatus(activeRecord.stage)} · ${titleCaseStatus(activeRecord.status)}</p>
                <p class="muted">${activeRecord.next_step || "No next step yet."}</p>
                <div class="footer-actions">
                  <button class="button button-secondary" data-select-pipeline-record="${activeRecord.id}">Inspect in pipeline</button>
                  <button class="button button-secondary" data-open-organization="${activeRecord.organization_id}">Open organization</button>
                </div>
              </div>
              <div class="detail-card">
                <h4>Canonical workflow controls</h4>
                <p class="muted">Use the pipeline-record id to keep stage, status, decision, and next step current without dropping into the legacy deal first.</p>
                ${renderPipelineRecordWorkflowEditor(activeRecord, "Save pipeline record")}
              </div>
              ${renderPipelineRecordWorkflowWorkspace(activeRecord)}
              ${renderPipelineRecordWorkflowBridge(activeRecord)}
            </div>
          `
          : ""}
        <div class="list-stack">
          ${state.pipelineRecords.length
            ? state.pipelineRecords.map((item) => `
              <div class="company-card" data-select-pipeline-record="${item.id}">
                <div class="panel-header">
                  <div>
                    <strong>${item.organization_name}</strong>
                    <p class="muted">${item.record_code} · ${titleCaseStatus(item.stage)} · ${item.owner_name || "Unassigned"}</p>
                  </div>
                  <span class="pill">${titleCaseStatus(item.status)}</span>
                </div>
                <p class="muted">Decision: ${titleCaseStatus(item.decision_outcome)}${item.fund_fit ? ` · Fund fit: ${item.fund_fit}` : ""}${item.legacy_opportunity_id ? ` · legacy opportunity ${item.legacy_opportunity_id}` : ""}</p>
                <p class="muted">${item.next_step || "No next step yet."}</p>
                <div class="footer-actions">
                  <button class="button button-secondary" data-open-organization="${item.organization_id}">Organization</button>
                  <button class="button button-secondary" data-select-pipeline-record="${item.id}">Inspect</button>
                  <button class="button button-secondary" data-open-pipeline-record="${item.id}">Manage</button>
                  ${item.legacy_opportunity_id
                    ? `<button class="button button-primary" data-open-full-opportunity="${item.legacy_opportunity_id}">Legacy workflow</button>`
                    : ""}
                </div>
              </div>
            `).join("")
            : emptyState("No pipeline records yet", "Pipeline records are the canonical evaluations attached to organizations.")}
        </div>
      </section>
    </div>
  `;
}

function renderReports() {
  if (!state.report) return emptyState("Loading report", "Gathering pipeline health metrics.");
  return `
    <div class="grid-metrics">
      <article class="metric-card"><span class="eyebrow">Active opportunities</span><strong>${state.report.executive_summary.active_opportunities}</strong></article>
      <article class="metric-card"><span class="eyebrow">Awaiting decision</span><strong>${state.report.executive_summary.deals_awaiting_decision}</strong></article>
      <article class="metric-card"><span class="eyebrow">Overdue tasks</span><strong>${state.report.executive_summary.overdue_tasks}</strong></article>
      <article class="metric-card"><span class="eyebrow">Stale opportunities</span><strong>${state.report.executive_summary.stale_opportunities}</strong></article>
      <article class="metric-card"><span class="eyebrow">Avg days in stage</span><strong>${state.report.executive_summary.avg_days_in_stage}</strong></article>
      <article class="metric-card"><span class="eyebrow">Execution risk</span><strong>${state.report.executive_summary.execution_risk}</strong></article>
    </div>
    <div class="report-grid">
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Executive summary</span>
            <h3>Pipeline health</h3>
          </div>
          <button class="button button-secondary" data-export="reflection">Export Summary</button>
        </div>
        <div class="detail-stack">
          ${state.report.pipeline_health
            .map(
              (row) => `
                <div class="summary-block">
                  <strong>${row.label}</strong>
                  <p class="muted">${row.count} deals · ${formatCurrency(row.target_ticket_total)} target ticket</p>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Bottlenecks</span>
            <h3>Stale deals and missing info</h3>
          </div>
        </div>
        <div class="list-stack">
          ${state.report.bottlenecks
            .map(
              (deal) => `
                <div class="report-row" ${openEntityActionAttributes(deal)}>
                  <strong>${deal.company_name}</strong>
                  <p class="muted">${deal.stage_label} · last activity ${formatDate(deal.last_activity_at)}</p>
                </div>
              `
            )
            .join("")}
          ${state.report.missing_information
            .map(
              (deal) => `
                <div class="report-row" ${openEntityActionAttributes(deal)}>
                  <strong>${deal.company_name}</strong>
                  <p class="muted">Missing: ${deal.missing_fields.join(", ")}</p>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Analyst workload</span>
            <h3>Team load</h3>
          </div>
        </div>
        <div class="list-stack">
          ${state.report.analyst_workload
            .map(
              (row) => `
                <div class="summary-block">
                  <strong>${row.name}</strong>
                  <p class="muted">${row.active_deals} active deals · ${row.open_tasks} open tasks · ${row.overdue_tasks} overdue · ${row.load_flag}</p>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Decision agenda</span>
            <h3>Deals requiring decision</h3>
          </div>
        </div>
        <div class="list-stack">
          ${state.report.deals_requiring_decision
            .map(
              (deal) => `
                <div class="report-row" ${openEntityActionAttributes(deal)}>
                  <strong>${deal.company_name}</strong>
                  <p class="muted">${deal.owner_name || "Unassigned"} · ${deal.stage_label} · next step ${deal.next_step || "Not set"}</p>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
    </div>
  `;
}

function renderSettings() {
  const user = currentUser();
  return `
    <div class="settings-grid">
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Users</span>
            <h3>Current working user</h3>
          </div>
        </div>
        <div class="summary-block">
          <strong>${user?.name || "Unknown"}</strong>
          <p class="muted">${user?.email || ""}</p>
        </div>
      </section>
      <section class="panel">
        <div class="panel-header">
          <div>
            <span class="eyebrow">Local mode</span>
            <h3>Deployment readiness</h3>
          </div>
        </div>
        <p class="muted">The app is structured as a local Python + SQLite system for now. The backend API is separated from the frontend so it can move behind a proper deployed server later.</p>
      </section>
    </div>
  `;
}

function blankOpportunityForm() {
  return {
    company_name: "",
    website: "",
    sector: "",
    subsector: "",
    geography: "",
    hq_city: "",
    hq_country: "",
    business_model: "",
    company_description: "",
    deck_links: "",
    financial_links: "",
    material_links: "",
    primary_contact_name: "",
    primary_contact_title: "",
    primary_contact_email: "",
    primary_contact_phone: "",
    stage: "new",
    priority: "medium",
    owner_user_id: String(state.currentUserId),
    source_type: "inbound",
    source_detail: "",
    round_name: "",
    fund_fit: "",
    ticket_size_target: "",
    ownership_target_pct: "",
    valuation_min: "",
    valuation_max: "",
    annual_recurring_revenue: "",
    revenue_growth_pct: "",
    gross_margin_pct: "",
    ebitda_margin_pct: "",
    rule_of_40_pct: "",
    monthly_burn: "",
    cash_runway_months: "",
    financials_updated_at: "",
    investment_thesis: "",
    key_concerns: "",
    relationship_notes: "",
    nda_required: "0",
    nda_status: "not_required",
    risk_flags: [],
    tags: [],
    next_step: "",
    next_step_due_at: "",
    decision_due_at: "",
  };
}

function loadOpportunityIntoForm(opportunity) {
  state.opportunitySourceFiles = emptyOpportunitySourceFiles();
  state.opportunityForm = {
    company_name: opportunity.company_name || "",
    website: opportunity.website || "",
    sector: opportunity.sector || "",
    subsector: opportunity.subsector || "",
    geography: opportunity.geography || "",
    hq_city: opportunity.hq_city || "",
    hq_country: opportunity.hq_country || "",
    business_model: opportunity.business_model || "",
    company_description: opportunity.company_description || "",
    deck_links: "",
    financial_links: "",
    material_links: "",
    primary_contact_name: opportunity.primary_contact_name || "",
    primary_contact_title: opportunity.primary_contact_title || "",
    primary_contact_email: opportunity.primary_contact_email || "",
    primary_contact_phone: opportunity.primary_contact_phone || "",
    stage: opportunity.stage || "new",
    priority: opportunity.priority || "medium",
    owner_user_id: opportunity.owner_user_id || "",
    source_type: opportunity.source_type || "inbound",
    source_detail: opportunity.source_detail || "",
    round_name: opportunity.round_name || "",
    fund_fit: opportunity.fund_fit || "",
    ticket_size_target: opportunity.ticket_size_target || "",
    ownership_target_pct: opportunity.ownership_target_pct || "",
    valuation_min: opportunity.valuation_min || "",
    valuation_max: opportunity.valuation_max || "",
    annual_recurring_revenue: opportunity.annual_recurring_revenue || "",
    revenue_growth_pct: opportunity.revenue_growth_pct || "",
    gross_margin_pct: opportunity.gross_margin_pct || "",
    ebitda_margin_pct: opportunity.ebitda_margin_pct || "",
    rule_of_40_pct: opportunity.rule_of_40_pct || "",
    monthly_burn: opportunity.monthly_burn || "",
    cash_runway_months: opportunity.cash_runway_months || "",
    financials_updated_at: opportunity.financials_updated_at || "",
    investment_thesis: opportunity.investment_thesis || "",
    key_concerns: opportunity.key_concerns || "",
    relationship_notes: opportunity.relationship_notes || "",
    nda_required: String(opportunity.nda_required || "0"),
    nda_status: opportunity.nda_status || "not_required",
    risk_flags: opportunity.risk_flags || [],
    tags: opportunity.tags || [],
    next_step: opportunity.next_step || "",
    next_step_due_at: opportunity.next_step_due_at || "",
    decision_due_at: opportunity.decision_due_at || "",
  };
}

function collectOpportunityForm(formElement) {
  const formData = new FormData(formElement);
  const payload = {
    ...(state.opportunityForm || blankOpportunityForm()),
    ...Object.fromEntries(formData.entries()),
  };
  payload.actor_user_id = state.currentUserId;
  payload.owner_user_id = payload.owner_user_id || "";
  payload.risk_flags = Array.isArray(payload.risk_flags)
    ? payload.risk_flags.filter(Boolean)
    : String(payload.risk_flags || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
  payload.tags = Array.isArray(payload.tags)
    ? payload.tags.filter(Boolean)
    : String(payload.tags || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
  return payload;
}

function syncOpportunityFormDraft(formElement = document.querySelector("#opportunityForm")) {
  if (!formElement) return;
  state.opportunityForm = {
    ...(state.opportunityForm || blankOpportunityForm()),
    ...collectOpportunityForm(formElement),
  };
}

function canonicalPipelineStageFromOpportunityStage(stage) {
  const mapping = {
    new: "intake",
    screening: "screening",
    qualified: "qualified",
    ic_review: "ic_preparation",
    term_sheet: "closing",
    closed_won: "invested",
    closed_lost: "passed",
  };
  return mapping[stage] || "intake";
}

function canonicalPipelineStatusFromOpportunityStage(stage) {
  if (stage === "closed_won") return "approved";
  if (stage === "closed_lost") return "passed";
  return "active";
}

function generatePipelineRecordCode() {
  const now = new Date();
  const parts = [
    now.getFullYear(),
    String(now.getMonth() + 1).padStart(2, "0"),
    String(now.getDate()).padStart(2, "0"),
    String(now.getHours()).padStart(2, "0"),
    String(now.getMinutes()).padStart(2, "0"),
    String(now.getSeconds()).padStart(2, "0"),
    String(now.getMilliseconds()).padStart(3, "0"),
  ];
  return `PR-${parts.join("")}`;
}

function compactPayload(payload) {
  return Object.fromEntries(
    Object.entries(payload).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      return value !== "" && value !== null && value !== undefined;
    })
  );
}

function findMatchingOrganizationForOpportunity(payload) {
  const website = normalizeText(payload.website);
  const companyName = normalizeText(payload.company_name);
  return state.organizations.find((item) => {
    if (website && normalizeText(item.website) === website) return true;
    return companyName && normalizeText(item.name) === companyName;
  }) || null;
}

function buildCanonicalOrganizationPayload(payload) {
  return compactPayload({
    actor_user_id: state.currentUserId,
    owner_user_id: payload.owner_user_id || "",
    name: payload.company_name,
    organization_type: "company",
    relationship_status: "active",
    website: payload.website,
    sector_primary: payload.sector,
    subsector: payload.subsector,
    geography: payload.geography,
    hq_city: payload.hq_city,
    hq_country: payload.hq_country,
    business_model: payload.business_model,
    description: payload.company_description,
  });
}

async function createOrUpdateCanonicalOrganization(payload) {
  const organizationPayload = buildCanonicalOrganizationPayload(payload);
  const existing = findMatchingOrganizationForOpportunity(payload);
  if (!existing) {
    return request("/api/v1/organizations", {
      method: "POST",
      body: JSON.stringify(organizationPayload),
    });
  }
  const patchPayload = compactPayload({
    actor_user_id: state.currentUserId,
    owner_user_id: organizationPayload.owner_user_id || existing.owner_user_id || "",
    website: organizationPayload.website,
    sector_primary: organizationPayload.sector_primary,
    subsector: organizationPayload.subsector,
    geography: organizationPayload.geography,
    hq_city: organizationPayload.hq_city,
    hq_country: organizationPayload.hq_country,
    business_model: organizationPayload.business_model,
    description: organizationPayload.description,
  });
  if (Object.keys(patchPayload).length <= 1) {
    return existing;
  }
  return request(`/api/v1/organizations/${existing.id}`, {
    method: "PATCH",
    body: JSON.stringify(patchPayload),
  });
}

function buildCanonicalPipelineRecordPayload(payload, organizationId) {
  const stage = canonicalPipelineStageFromOpportunityStage(payload.stage);
  return compactPayload({
    actor_user_id: state.currentUserId,
    organization_id: organizationId,
    record_code: generatePipelineRecordCode(),
    stage,
    status: canonicalPipelineStatusFromOpportunityStage(payload.stage),
    decision_outcome: payload.stage === "closed_won" ? "approved" : payload.stage === "closed_lost" ? "declined" : "pending",
    owner_user_id: payload.owner_user_id || "",
    priority: payload.priority || "medium",
    fund_fit: payload.fund_fit,
    source_type: payload.source_type,
    source_detail: payload.source_detail,
    next_step: payload.next_step,
    next_step_due_at: payload.next_step_due_at,
    round_name: payload.round_name,
    ticket_size_target: payload.ticket_size_target,
    ownership_target_pct: payload.ownership_target_pct,
    valuation_min: payload.valuation_min,
    valuation_max: payload.valuation_max,
    annual_recurring_revenue: payload.annual_recurring_revenue,
    revenue_growth_pct: payload.revenue_growth_pct,
    gross_margin_pct: payload.gross_margin_pct,
    ebitda_margin_pct: payload.ebitda_margin_pct,
    rule_of_40_pct: payload.rule_of_40_pct,
    monthly_burn: payload.monthly_burn,
    cash_runway_months: payload.cash_runway_months,
    financials_updated_at: payload.financials_updated_at,
    investment_thesis: payload.investment_thesis,
    key_concerns: payload.key_concerns,
    relationship_notes: payload.relationship_notes,
    nda_required: payload.nda_required,
    nda_status: payload.nda_status,
    risk_flags: payload.risk_flags,
    tags: payload.tags,
    decision_due_at: payload.decision_due_at,
    primary_contact_name: payload.primary_contact_name,
    primary_contact_title: payload.primary_contact_title,
    primary_contact_email: payload.primary_contact_email,
    primary_contact_phone: payload.primary_contact_phone,
  });
}

function buildOpportunityOverflowNote(payload) {
  const lines = [
    "Structured intake fields captured from Add Opportunity.",
  ].filter(Boolean);
  return lines.join("\n");
}

async function createCanonicalOpportunityFromForm(payload) {
  const organization = await createOrUpdateCanonicalOrganization(payload);
  const pipelineRecord = await request("/api/v1/pipeline-records", {
    method: "POST",
    body: JSON.stringify(buildCanonicalPipelineRecordPayload(payload, organization.id)),
  });
  const attachedDocumentCount = await attachOpportunityMaterials(pipelineRecord.id, payload);
  const overflowNote = buildOpportunityOverflowNote(payload);
  if (overflowNote.replaceAll("\n", "").trim() !== "Structured intake fields captured from Add Opportunity.") {
    await request(`/api/v1/pipeline-records/${pipelineRecord.id}/notes`, {
      method: "POST",
      body: JSON.stringify({
        author_user_id: state.currentUserId,
        note_type: "general",
        body: overflowNote,
        is_pinned: 1,
      }),
    });
  }
  if (organization.website || attachedDocumentCount > 0) {
    await request(`/api/v1/pipeline-records/${pipelineRecord.id}/autofill`, {
      method: "POST",
      body: JSON.stringify({
        actor_user_id: state.currentUserId,
      }),
    });
  }
  return { organization, pipelineRecord, attachedDocumentCount };
}

function render() {
  ensureCanonicalSelections();
  activeNav();
  const [eyebrow, title] = pageMeta();
  pageEyebrow.textContent = eyebrow;
  pageTitle.textContent = title;
  renderBanner();

  if (state.route === "dashboard") {
    app.innerHTML = renderDashboard();
  } else if (state.route === "pipeline") {
    app.innerHTML = renderPipeline();
  } else if (state.route === "intake") {
    app.innerHTML = renderIntake();
  } else if (state.route === "relationships") {
    app.innerHTML = renderRelationships();
  } else if (state.route === "organizations") {
    app.innerHTML = renderOrganizations();
  } else if (state.route === "pipeline-records") {
    app.innerHTML = renderPipelineRecords();
  } else if (state.route === "tasks") {
    app.innerHTML = renderTasks();
  } else if (state.route === "companies") {
    app.innerHTML = renderCompanies();
  } else if (state.route === "reports") {
    app.innerHTML = renderReports();
  } else if (state.route === "settings") {
    app.innerHTML = renderSettings();
  } else if (state.route === "new-opportunity") {
    app.innerHTML = renderNewOpportunityPage();
  } else if (state.route.startsWith("opportunity")) {
    app.innerHTML = renderOpportunityDetail();
  }
  renderModal();
}

function setRouteFromHash() {
  const hash = window.location.hash.replace(/^#/, "") || "dashboard";
  if (hash.startsWith("opportunity/")) {
    state.route = "opportunity";
    const id = Number(hash.split("/")[1]);
    openOpportunity(id);
    return;
  }
  if (hash === "new-opportunity") {
    state.route = "new-opportunity";
    state.selectedOpportunity = null;
    state.editingOpportunity = true;
    state.intakeSection = "materials";
    state.opportunityForm = blankOpportunityForm();
    state.opportunityFormErrors = {};
    state.opportunitySourceFiles = emptyOpportunitySourceFiles();
    render();
    return;
  }
  state.route = hash;
  state.editingOpportunity = false;
  render();
}

async function loadAll() {
  state.bootstrap = await request("/api/v1/bootstrap");
  state.currentUserId = state.bootstrap.current_user_id;
  currentUserSelect.innerHTML = state.bootstrap.users
    .map((user) => `<option value="${user.id}">${user.name}</option>`)
    .join("");
  currentUserSelect.value = String(state.currentUserId);
  await Promise.all([loadDashboard(), loadPipeline(), loadTasks(), loadCompanies(), loadOrganizations(), loadPipelineRecords(), loadIntakeSubmissions(), loadRelationshipLinks(), loadReport()]);
  ensureCanonicalSelections();
}

async function loadDashboard() {
  state.dashboard = await request("/api/v1/dashboard");
}

async function loadPipeline() {
  const query = new URLSearchParams(state.pipelineFilters).toString();
  state.opportunities = await request(`/api/v1/opportunities?${query}`);
  state.kanban = await request(`/api/v1/pipeline/kanban?${query}`);
  if (!state.selectedOpportunity && state.opportunities.length) {
    state.selectedOpportunity = await request(`/api/v1/opportunities/${state.opportunities[0].id}`);
  }
}

async function loadTasks() {
  const query = new URLSearchParams(state.taskFilters).toString();
  state.tasks = await request(`/api/v1/tasks?${query}`);
}

async function loadCompanies() {
  state.companies = await request("/api/v1/companies");
}

async function loadOrganizations() {
  state.organizations = await request("/api/v1/organizations");
}

async function loadPipelineRecords() {
  state.pipelineRecords = await request("/api/v1/pipeline-records");
}

async function hydratePipelineRecordWorkflow(recordId) {
  const workflow = await requestOptional(`/api/v1/pipeline-records/${recordId}/workflow`);
  if (!workflow) return null;
  const detail = selectedPipelineRecord() || state.pipelineRecords.find((item) => String(item.id) === String(recordId)) || {};
  const merged = {
    ...detail,
    ...workflow,
    notes: workflow.notes || detail.notes || [],
    tasks: workflow.tasks || detail.tasks || [],
    documents: workflow.documents || detail.documents || [],
    decision_logs: workflow.decision_logs || workflow.decisions || detail.decision_logs || detail.decisions || [],
    activities: workflow.activities || detail.activities || [],
    workflow_available: true,
  };
  upsertPipelineRecord(merged);
  return merged;
}

async function openPipelineRecord(id, options = {}) {
  const detail = await request(`/api/v1/pipeline-records/${id}`);
  upsertPipelineRecord(detail);
  state.selectedPipelineRecordId = detail.id;
  if (detail.organization_id) {
    state.selectedOrganizationId = detail.organization_id;
  }
  await hydratePipelineRecordWorkflow(detail.id);
  if (options.route) {
    if (state.route !== options.route || window.location.hash !== `#${options.route}`) {
      window.location.hash = options.route;
    } else {
      render();
    }
    return;
  }
  render();
}

async function loadIntakeSubmissions() {
  state.intakeSubmissions = await request("/api/v1/intake-submissions");
}

async function loadRelationshipLinks() {
  state.relationshipLinks = await request("/api/v1/relationship-links");
}

async function loadReport() {
  state.report = await request("/api/v1/reports/reflection");
}

async function openOpportunity(id) {
  state.selectedOpportunity = await request(`/api/v1/opportunities/${id}`);
  render();
}

async function refreshCurrentData() {
  await Promise.all([loadDashboard(), loadPipeline(), loadTasks(), loadCompanies(), loadOrganizations(), loadPipelineRecords(), loadIntakeSubmissions(), loadRelationshipLinks(), loadReport()]);
  if (state.selectedOpportunity?.id) {
    state.selectedOpportunity = await request(`/api/v1/opportunities/${state.selectedOpportunity.id}`);
  }
  ensureCanonicalSelections();
  if (state.selectedPipelineRecordId) {
    await hydratePipelineRecordWorkflow(state.selectedPipelineRecordId);
  }
  render();
}

document.addEventListener("click", async (event) => {
  const backdrop = event.target.closest("[data-close-modal]");
  if (backdrop && event.target === backdrop) {
    closeModal();
    return;
  }
  const target = event.target.closest("[data-route], [data-open-opportunity], [data-open-full-opportunity], [data-preview-opportunity], [data-open-pipeline-record], [data-open-company-detail], [data-open-company-edit], [data-save-company-edit], [data-set-view], [data-stage-tab], [data-clear-pipeline], [data-toggle-edit-opportunity], [data-intake-section], [data-prev-section], [data-next-section], [data-cancel-opportunity-form], [data-change-stage], [data-open-note-modal], [data-save-note], [data-close-modal], [data-add-task], [data-add-task-comment], [data-add-document], [data-add-decision], [data-export], [data-export-opportunity], [data-delete-opportunity], [data-update-task], [data-set-opportunity-status], [data-select-organization], [data-select-pipeline-record], [data-open-organization], [data-refresh-pipeline-record], [data-autofill-pipeline-record], [data-save-pipeline-record-workflow], [data-add-pipeline-record-note], [data-add-pipeline-record-task], [data-update-pipeline-record-task], [data-add-pipeline-record-task-comment], [data-delete-pipeline-record-task], [data-add-pipeline-record-document], [data-add-pipeline-record-decision], [data-run-opportunity-prefill]");
  if (!target) return;
  event.preventDefault();

  try {
    if (target.hasAttribute("data-close-modal")) {
      closeModal();
      return;
    }
    if (target.dataset.route) {
      window.location.hash = target.dataset.route;
      return;
    }
    if (target.dataset.openFullOpportunity) {
      const id = Number(target.dataset.openFullOpportunity);
      await openOpportunity(id);
      window.location.hash = `opportunity/${id}`;
      return;
    }
    if (target.dataset.previewOpportunity) {
      const id = Number(target.dataset.previewOpportunity);
      await openOpportunity(id);
      if (state.route !== "pipeline") {
        window.location.hash = "pipeline";
      } else {
        render();
      }
      return;
    }
    if (target.dataset.openOpportunity) {
      const id = Number(target.dataset.openOpportunity);
      await openOpportunity(id);
      if (state.route === "pipeline") {
        render();
      } else {
        window.location.hash = `opportunity/${id}`;
      }
      return;
    }
    if (target.dataset.openCompanyDetail) {
      openModal({ type: "company-detail", companyId: Number(target.dataset.openCompanyDetail) });
      return;
    }
    if (target.dataset.openCompanyEdit) {
      openModal({ type: "company-edit", companyId: Number(target.dataset.openCompanyEdit) });
      return;
    }
    if (target.dataset.saveCompanyEdit) {
      await saveCompanyEdit(Number(target.dataset.saveCompanyEdit));
      return;
    }
    if (target.dataset.selectOrganization) {
      state.selectedOrganizationId = Number(target.dataset.selectOrganization);
      const related = relatedPipelineRecords(state.selectedOrganizationId)[0];
      if (related) state.selectedPipelineRecordId = related.id;
      render();
      return;
    }
    if (target.dataset.selectPipelineRecord) {
      const recordId = Number(target.dataset.selectPipelineRecord);
      await openPipelineRecord(recordId, { route: state.route !== "pipeline" ? "pipeline" : null });
      return;
    }
    if (target.dataset.openPipelineRecord) {
      if (state.modal?.type === "company-detail" || state.modal?.type === "company-edit") closeModal();
      await openPipelineRecord(Number(target.dataset.openPipelineRecord), { route: "pipeline-records" });
      return;
    }
    if (target.dataset.openOrganization) {
      if (state.modal?.type === "company-detail" || state.modal?.type === "company-edit") closeModal();
      state.selectedOrganizationId = Number(target.dataset.openOrganization);
      const related = relatedPipelineRecords(state.selectedOrganizationId)[0];
      if (related) state.selectedPipelineRecordId = related.id;
      window.location.hash = "organizations";
      return;
    }
    if (target.dataset.setView) {
      state.viewMode = target.dataset.setView;
      render();
      return;
    }
    if (target.dataset.stageTab) {
      state.pipelineFilters.stage = target.dataset.stageTab;
      render();
      return;
    }
    if (target.hasAttribute("data-clear-pipeline")) {
      state.pipelineFilters = { q: "", stage: "all", status: "all", owner_user_id: "all", overdue_only: "0", missing_info: "0", sort: "updated_at", order: "desc" };
      render();
      return;
    }
    if (target.hasAttribute("data-toggle-edit-opportunity")) {
      state.editingOpportunity = !state.editingOpportunity;
      if (state.editingOpportunity) loadOpportunityIntoForm(state.selectedOpportunity);
      render();
      return;
    }
    if (target.dataset.intakeSection) {
      state.intakeSection = target.dataset.intakeSection;
      render();
      return;
    }
    if (target.hasAttribute("data-prev-section")) {
      const current = opportunitySections.findIndex((section) => section.id === state.intakeSection);
      state.intakeSection = opportunitySections[Math.max(0, current - 1)].id;
      render();
      return;
    }
    if (target.hasAttribute("data-next-section")) {
      const current = opportunitySections.findIndex((section) => section.id === state.intakeSection);
      state.intakeSection = opportunitySections[Math.min(opportunitySections.length - 1, current + 1)].id;
      render();
      return;
    }
    if (target.dataset.runOpportunityPrefill !== undefined) {
      await runOpportunityIntakePrefill(document.querySelector("#opportunityForm"));
      return;
    }
    if (target.hasAttribute("data-cancel-opportunity-form")) {
      state.editingOpportunity = false;
      state.opportunityFormErrors = {};
      state.opportunitySourceFiles = emptyOpportunitySourceFiles();
      if (state.route === "new-opportunity") {
        window.location.hash = "pipeline";
      } else {
        render();
      }
      return;
    }
    if (target.dataset.changeStage) {
      const payload = {
        actor_user_id: state.currentUserId,
        to_stage: document.querySelector("#changeStageSelect").value,
        reason: document.querySelector("#changeStageReason").value,
      };
      state.selectedOpportunity = await request(`/api/v1/opportunities/${target.dataset.changeStage}/change-stage`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      toast("Stage changed");
      await refreshCurrentData();
      return;
    }
    if (target.dataset.refreshPipelineRecord) {
      await openPipelineRecord(Number(target.dataset.refreshPipelineRecord));
      toast("Canonical record refreshed");
      return;
    }
    if (target.dataset.autofillPipelineRecord) {
      const recordId = Number(target.dataset.autofillPipelineRecord);
      const result = await runPipelineRecordAutofill(recordId);
      await openPipelineRecord(recordId, { route: state.route === "pipeline-records" ? "pipeline-records" : null });
      toast(result.summary);
      return;
    }
    if (target.dataset.savePipelineRecordWorkflow) {
      const recordId = Number(target.dataset.savePipelineRecordWorkflow);
      const payload = {
        actor_user_id: state.currentUserId,
        stage: document.querySelector(`#pipelineRecordStage-${recordId}`).value,
        status: document.querySelector(`#pipelineRecordStatus-${recordId}`).value,
        priority: document.querySelector(`#pipelineRecordPriority-${recordId}`).value,
        decision_outcome: document.querySelector(`#pipelineRecordDecision-${recordId}`).value,
        source_type: document.querySelector(`#pipelineRecordSourceType-${recordId}`).value,
        primary_contact_name: document.querySelector(`#pipelineRecordPrimaryContactName-${recordId}`).value,
        primary_contact_title: document.querySelector(`#pipelineRecordPrimaryContactTitle-${recordId}`).value,
        primary_contact_email: document.querySelector(`#pipelineRecordPrimaryContactEmail-${recordId}`).value,
        primary_contact_phone: document.querySelector(`#pipelineRecordPrimaryContactPhone-${recordId}`).value,
        fund_fit: document.querySelector(`#pipelineRecordFundFit-${recordId}`).value,
        source_detail: document.querySelector(`#pipelineRecordSourceDetail-${recordId}`).value,
        round_name: document.querySelector(`#pipelineRecordRoundName-${recordId}`).value,
        ticket_size_target: document.querySelector(`#pipelineRecordTicketTarget-${recordId}`).value,
        ownership_target_pct: document.querySelector(`#pipelineRecordOwnershipTarget-${recordId}`).value,
        valuation_min: document.querySelector(`#pipelineRecordValuationMin-${recordId}`).value,
        valuation_max: document.querySelector(`#pipelineRecordValuationMax-${recordId}`).value,
        annual_recurring_revenue: readNumberInputValue(`#pipelineRecordRevenueArr-${recordId}`),
        revenue_growth_pct: readNumberInputValue(`#pipelineRecordRevenueGrowth-${recordId}`),
        gross_margin_pct: readNumberInputValue(`#pipelineRecordGrossMargin-${recordId}`),
        ebitda_margin_pct: readNumberInputValue(`#pipelineRecordEbitdaMargin-${recordId}`),
        rule_of_40_pct: readNumberInputValue(`#pipelineRecordRuleOf40-${recordId}`),
        monthly_burn: readNumberInputValue(`#pipelineRecordMonthlyBurn-${recordId}`),
        cash_runway_months: readNumberInputValue(`#pipelineRecordCashRunway-${recordId}`),
        financials_updated_at: document.querySelector(`#pipelineRecordFinancialsUpdatedAt-${recordId}`).value || null,
        next_step: document.querySelector(`#pipelineRecordNextStep-${recordId}`).value,
        next_step_due_at: document.querySelector(`#pipelineRecordDueDate-${recordId}`).value || null,
        decision_due_at: document.querySelector(`#pipelineRecordDecisionDueDate-${recordId}`).value || null,
        investment_thesis: document.querySelector(`#pipelineRecordInvestmentThesis-${recordId}`).value,
        key_concerns: document.querySelector(`#pipelineRecordKeyConcerns-${recordId}`).value,
        relationship_notes: document.querySelector(`#pipelineRecordRelationshipNotes-${recordId}`).value,
        nda_required: document.querySelector(`#pipelineRecordNdaRequired-${recordId}`).value,
        nda_status: document.querySelector(`#pipelineRecordNdaStatus-${recordId}`).value,
        risk_flags: document.querySelector(`#pipelineRecordRiskFlags-${recordId}`).value.split(",").map((item) => item.trim()).filter(Boolean),
        tags: document.querySelector(`#pipelineRecordTags-${recordId}`).value.split(",").map((item) => item.trim()).filter(Boolean),
      };
      const updated = await request(`/api/v1/pipeline-records/${recordId}`, {
        method: "PATCH",
        body: JSON.stringify(payload),
      });
      upsertPipelineRecord(updated);
      state.selectedPipelineRecordId = updated.id;
      state.selectedOrganizationId = updated.organization_id || state.selectedOrganizationId;
      toast("Canonical workflow saved");
      await refreshCurrentData();
      return;
    }
    if (target.dataset.addPipelineRecordNote) {
      const recordId = Number(target.dataset.addPipelineRecordNote);
      await request(`/api/v1/pipeline-records/${recordId}/notes`, {
        method: "POST",
        body: JSON.stringify({
          body: document.querySelector(`#pipelineRecordNoteBody-${recordId}`).value.trim(),
          note_type: document.querySelector(`#pipelineRecordNoteType-${recordId}`).value,
          author_user_id: state.currentUserId,
        }),
      });
      await openPipelineRecord(recordId);
      toast("Note added");
      return;
    }
    if (target.dataset.addPipelineRecordTask) {
      const recordId = Number(target.dataset.addPipelineRecordTask);
      await request(`/api/v1/pipeline-records/${recordId}/tasks`, {
        method: "POST",
        body: JSON.stringify({
          title: document.querySelector(`#pipelineRecordTaskTitle-${recordId}`).value,
          description: document.querySelector(`#pipelineRecordTaskDescription-${recordId}`).value,
          assigned_user_id: document.querySelector(`#pipelineRecordTaskAssignee-${recordId}`).value || null,
          priority: document.querySelector(`#pipelineRecordTaskPriority-${recordId}`).value,
          due_at: document.querySelector(`#pipelineRecordTaskDueDate-${recordId}`).value || null,
          created_by_user_id: state.currentUserId,
        }),
      });
      await openPipelineRecord(recordId);
      toast("Task created");
      return;
    }
    if (target.dataset.updatePipelineRecordTask) {
      const taskId = Number(target.dataset.updatePipelineRecordTask);
      const recordId = Number(target.dataset.pipelineRecordId);
      const record = selectedPipelineRecord() || state.pipelineRecords.find((item) => String(item.id) === String(recordId));
      const task = pipelineRecordTasks(record).find((item) => String(item.id) === String(taskId));
      const status = document.querySelector(`[data-pipeline-record-task-status-select="${taskId}"]`)?.value;
      if (!task || !status) return;
      if (pipelineRecordTaskActionMode(task) === "canonical") {
        await request(`/api/v1/pipeline-record-tasks/${taskId}`, {
          method: "PATCH",
          body: JSON.stringify({ status, updated_by_user_id: state.currentUserId }),
        });
      } else {
        await request(`/api/v1/tasks/${taskId}`, {
          method: "PATCH",
          body: JSON.stringify({ status, updated_by_user_id: state.currentUserId }),
        });
      }
      await openPipelineRecord(recordId, { route: state.route === "pipeline-records" ? "pipeline-records" : null });
      toast("Task updated");
      return;
    }
    if (target.dataset.addPipelineRecordTaskComment) {
      const taskId = Number(target.dataset.addPipelineRecordTaskComment);
      const recordId = Number(target.dataset.pipelineRecordId);
      const record = selectedPipelineRecord() || state.pipelineRecords.find((item) => String(item.id) === String(recordId));
      const task = pipelineRecordTasks(record).find((item) => String(item.id) === String(taskId));
      const input = document.querySelector(`#pipelineRecordTaskComment-${taskId}`);
      const body = input?.value.trim();
      if (!task || !body) return;
      if (pipelineRecordTaskActionMode(task) === "canonical") {
        await request(`/api/v1/pipeline-record-tasks/${taskId}/comments`, {
          method: "POST",
          body: JSON.stringify({ body, user_id: state.currentUserId }),
        });
      } else {
        await request(`/api/v1/tasks/${taskId}/comments`, {
          method: "POST",
          body: JSON.stringify({ body, user_id: state.currentUserId }),
        });
      }
      await openPipelineRecord(recordId, { route: state.route === "pipeline-records" ? "pipeline-records" : null });
      toast("Task comment added");
      return;
    }
    if (target.dataset.deletePipelineRecordTask) {
      const taskId = Number(target.dataset.deletePipelineRecordTask);
      const recordId = Number(target.dataset.pipelineRecordId);
      const record = selectedPipelineRecord() || state.pipelineRecords.find((item) => String(item.id) === String(recordId));
      const task = pipelineRecordTasks(record).find((item) => String(item.id) === String(taskId));
      if (!task || pipelineRecordTaskActionMode(task) !== "canonical") return;
      if (!window.confirm("Delete this canonical task?")) return;
      await request(`/api/v1/pipeline-record-tasks/${taskId}`, {
        method: "DELETE",
      });
      await openPipelineRecord(recordId, { route: state.route === "pipeline-records" ? "pipeline-records" : null });
      toast("Task deleted");
      return;
    }
    if (target.dataset.addPipelineRecordDocument) {
      const recordId = Number(target.dataset.addPipelineRecordDocument);
      const addedDocument = await submitDocument(`/api/v1/pipeline-records/${recordId}/documents`, {
        nameInput: document.querySelector(`#pipelineRecordDocumentName-${recordId}`),
        categoryInput: document.querySelector(`#pipelineRecordDocumentCategory-${recordId}`),
        pathInput: document.querySelector(`#pipelineRecordDocumentPath-${recordId}`),
        fileInput: document.querySelector(`#pipelineRecordDocumentFile-${recordId}`),
      });
      if (["deck", "financials"].includes(String(addedDocument?.document_category || "").toLowerCase())) {
        const result = await runPipelineRecordAutofill(recordId);
        toast(result.summary);
      }
      await openPipelineRecord(recordId);
      toast("Document added");
      return;
    }
    if (target.dataset.addPipelineRecordDecision) {
      const recordId = Number(target.dataset.addPipelineRecordDecision);
      await request(`/api/v1/pipeline-records/${recordId}/decision`, {
        method: "POST",
        body: JSON.stringify({
          decision_type: document.querySelector(`#pipelineRecordDecisionType-${recordId}`).value,
          decision_summary: document.querySelector(`#pipelineRecordDecisionSummary-${recordId}`).value,
          rationale: document.querySelector(`#pipelineRecordDecisionRationale-${recordId}`).value,
          decided_by_user_id: state.currentUserId,
        }),
      });
      await openPipelineRecord(recordId);
      toast("Decision logged");
      return;
    }
    if (target.dataset.openNoteModal) {
      openModal({
        type: "note",
        opportunityId: Number(target.dataset.openNoteModal),
        noteType: "general",
        body: "",
      });
      return;
    }
    if (target.dataset.saveNote) {
      const body = document.querySelector("#modalNoteBody").value.trim();
      const noteType = document.querySelector("#modalNoteType").value;
      await request(`/api/v1/opportunities/${target.dataset.saveNote}/notes`, {
        method: "POST",
        body: JSON.stringify({ body, note_type: noteType, author_user_id: state.currentUserId }),
      });
      toast("Note added");
      closeModal();
      state.selectedOpportunity = await request(`/api/v1/opportunities/${target.dataset.saveNote}`);
      await refreshCurrentData();
      render();
      return;
    }
    if (target.dataset.setOpportunityStatus) {
      const opportunityId = Number(target.dataset.setOpportunityStatus);
      const status = target.dataset.statusValue;
      state.selectedOpportunity = await request(`/api/v1/opportunities/${opportunityId}`, {
        method: "PATCH",
        body: JSON.stringify({ status, actor_user_id: state.currentUserId }),
      });
      toast(`Status set to ${titleCaseStatus(status)}`);
      if (status === "closed_won" || status === "closed_lost") {
        state.editingOpportunity = false;
      }
      await refreshCurrentData();
      render();
      return;
    }
    if (target.dataset.addTask) {
      await request(`/api/v1/opportunities/${target.dataset.addTask}/tasks`, {
        method: "POST",
        body: JSON.stringify({
          title: document.querySelector("#taskTitle").value,
          description: document.querySelector("#taskDescription").value,
          assigned_user_id: document.querySelector("#taskAssignee").value || null,
          priority: document.querySelector("#taskPriority").value,
          due_at: document.querySelector("#taskDueDate").value || null,
          created_by_user_id: state.currentUserId,
        }),
      });
      toast("Task created");
      await refreshCurrentData();
      return;
    }
    if (target.dataset.addTaskComment) {
      const taskId = target.dataset.addTaskComment;
      const input = document.querySelector(`#taskComment-${taskId}`);
      await request(`/api/v1/tasks/${taskId}/comments`, {
        method: "POST",
        body: JSON.stringify({ body: input.value, user_id: state.currentUserId }),
      });
      toast("Task comment added");
      state.selectedOpportunity = await request(`/api/v1/opportunities/${state.selectedOpportunity.id}`);
      render();
      return;
    }
    if (target.dataset.addDocument) {
      await submitDocument(`/api/v1/opportunities/${target.dataset.addDocument}/documents`, {
        nameInput: document.querySelector("#documentName"),
        categoryInput: document.querySelector("#documentCategory"),
        pathInput: document.querySelector("#documentPath"),
        fileInput: document.querySelector("#documentFile"),
      });
      toast("Document added");
      state.selectedOpportunity = await request(`/api/v1/opportunities/${state.selectedOpportunity.id}`);
      render();
      return;
    }
    if (target.dataset.addDecision) {
      await request(`/api/v1/opportunities/${target.dataset.addDecision}/decision`, {
        method: "POST",
        body: JSON.stringify({
          decision_type: document.querySelector("#decisionType").value,
          decision_summary: document.querySelector("#decisionSummary").value,
          rationale: document.querySelector("#decisionRationale").value,
          decided_by_user_id: state.currentUserId,
        }),
      });
      toast("Decision logged");
      state.selectedOpportunity = await request(`/api/v1/opportunities/${state.selectedOpportunity.id}`);
      await refreshCurrentData();
      return;
    }
    if (target.dataset.export === "reflection") {
      const report = await request("/api/v1/reports/reflection/export");
      downloadText("vcc-reflection-report.md", report);
      return;
    }
    if (target.dataset.export === "pipeline") {
      const csv = await request("/api/v1/reports/pipeline.csv");
      downloadText("vcc-pipeline.csv", csv, "text/csv");
      return;
    }
    if (target.dataset.exportOpportunity) {
      const report = await request(`/api/v1/opportunities/${target.dataset.exportOpportunity}/export`);
      downloadText(`${state.selectedOpportunity.company_name.replace(/\s+/g, "-").toLowerCase()}-report.md`, report);
      return;
    }
    if (target.dataset.deleteOpportunity) {
      if (!window.confirm("Delete this opportunity from the active pipeline?")) return;
      await request(`/api/v1/opportunities/${target.dataset.deleteOpportunity}?actor_user_id=${state.currentUserId}`, { method: "DELETE" });
      toast("Opportunity deleted");
      state.selectedOpportunity = null;
      window.location.hash = "pipeline";
      await refreshCurrentData();
      return;
    }
    if (target.dataset.updateTask) {
      const taskId = target.dataset.updateTask;
      const status = document.querySelector(`[data-task-status-select="${taskId}"]`).value;
      await request(`/api/v1/tasks/${taskId}`, {
        method: "PATCH",
        body: JSON.stringify({ status, updated_by_user_id: state.currentUserId }),
      });
      toast("Task updated");
      await refreshCurrentData();
      return;
    }
  } catch (error) {
    state.opportunityFormErrors = error.fields || {};
    setBanner("error", error.message);
    render();
  }
});

document.addEventListener("change", async (event) => {
  if (event.target.id === "pipelineOwnerFilter") {
    state.pipelineFilters.owner_user_id = event.target.value;
    render();
  } else if (event.target.id === "pipelineStatusFilter") {
    state.pipelineFilters.status = event.target.value;
    render();
  } else if (event.target.id === "pipelineSort") {
    state.pipelineFilters.sort = event.target.value;
    render();
  } else if (event.target.id === "pipelineOverdueOnly") {
    state.pipelineFilters.overdue_only = event.target.checked ? "1" : "0";
    render();
  } else if (event.target.id === "pipelineMissingOnly") {
    state.pipelineFilters.missing_info = event.target.checked ? "1" : "0";
    render();
  } else if (event.target.id === "tasksAssigneeFilter") {
    state.taskFilters.assigned_user_id = event.target.value;
    await loadTasks();
    render();
  } else if (event.target.id === "tasksStatusFilter") {
    state.taskFilters.status = event.target.value;
    await loadTasks();
    render();
  } else if (event.target.id === "tasksPriorityFilter") {
    state.taskFilters.priority = event.target.value;
    await loadTasks();
    render();
  } else if (event.target.id === "tasksOverdueOnly") {
    state.taskFilters.overdue_only = event.target.checked ? "1" : "0";
    await loadTasks();
    render();
  } else if (event.target.id === "currentUserSelect") {
    state.currentUserId = Number(event.target.value);
    render();
  } else if (event.target.id.startsWith("pipelineRecordDocumentFile-")) {
    const recordId = Number(event.target.id.replace("pipelineRecordDocumentFile-", ""));
    const file = event.target.files?.[0];
    const nameInput = document.querySelector(`#pipelineRecordDocumentName-${recordId}`);
    const categoryInput = document.querySelector(`#pipelineRecordDocumentCategory-${recordId}`);
    const pathInput = document.querySelector(`#pipelineRecordDocumentPath-${recordId}`);
    const label = document.querySelector(`#pipelineRecordDocumentShareLabel-${recordId}`);
    if (!file) {
      if (label) {
        label.textContent = "Choose any file from your computer or paste a shared link. New documents added here are meant to be visible to everyone using this platform.";
      }
      return;
    }
    if (nameInput && !nameInput.value.trim()) {
      nameInput.value = file.name;
    }
    if (categoryInput && categoryInput.value === "other") {
      categoryInput.value = inferDocumentCategoryFromFile(file);
    }
    if (pathInput && !pathInput.value.trim()) {
      pathInput.value = sharedWorkspacePath(file.name);
    }
    if (label) {
      label.textContent = `${file.name} selected from your computer. When you add it, it will appear in the shared materials list for everyone on this platform.`;
    }
  } else if (event.target.id === "opportunityDeckFiles") {
    state.opportunitySourceFiles = {
      ...(state.opportunitySourceFiles || emptyOpportunitySourceFiles()),
      deck: Array.from(event.target.files || []).map((file) => createOpportunitySourceFileEntry(file, "deck")),
    };
    await runOpportunityIntakePrefill(event.target.form);
    render();
  } else if (event.target.id === "opportunityFinancialFiles") {
    state.opportunitySourceFiles = {
      ...(state.opportunitySourceFiles || emptyOpportunitySourceFiles()),
      financials: Array.from(event.target.files || []).map((file) => createOpportunitySourceFileEntry(file, "financials")),
    };
    await runOpportunityIntakePrefill(event.target.form);
    render();
  } else if (event.target.id === "opportunityOtherFiles") {
    state.opportunitySourceFiles = {
      ...(state.opportunitySourceFiles || emptyOpportunitySourceFiles()),
      other: Array.from(event.target.files || []).map((file) => createOpportunitySourceFileEntry(file, inferDocumentCategoryFromFile(file))),
    };
    await runOpportunityIntakePrefill(event.target.form);
    render();
  } else if (event.target.form?.id === "opportunityForm") {
    syncOpportunityFormDraft(event.target.form);
    if (
      state.route === "new-opportunity"
      && ["website", "deck_links", "financial_links", "material_links"].includes(event.target.name)
      && hasValue(event.target.value)
    ) {
      await runOpportunityIntakePrefill(event.target.form);
    }
  }
});

document.addEventListener("input", async (event) => {
  if (event.target.id === "pipelineSearch") {
    state.pipelineFilters.q = event.target.value;
    render();
  } else if (event.target.form?.id === "opportunityForm") {
    syncOpportunityFormDraft(event.target.form);
  }
});

document.addEventListener("submit", async (event) => {
  if (!["opportunityForm", "intakeSubmissionForm", "relationshipLinkForm", "organizationForm", "pipelineRecordForm"].includes(event.target.id)) return;
  event.preventDefault();
  clearBanner();
  try {
    if (event.target.id === "opportunityForm") {
      const payload = collectOpportunityForm(event.target);
      if (state.route === "new-opportunity") {
        const created = await createCanonicalOpportunityFromForm(payload);
        toast("Canonical opportunity created");
        state.selectedOrganizationId = created.organization.id;
        state.selectedPipelineRecordId = created.pipelineRecord.id;
        state.opportunityForm = blankOpportunityForm();
        state.opportunitySourceFiles = emptyOpportunitySourceFiles();
        state.intakeSection = "materials";
        state.editingOpportunity = false;
        await refreshCurrentData();
        await openPipelineRecord(created.pipelineRecord.id, { route: "pipeline-records" });
      } else if (state.selectedOpportunity) {
        state.selectedOpportunity = await request(`/api/v1/opportunities/${state.selectedOpportunity.id}`, {
          method: "PATCH",
          body: JSON.stringify(payload),
        });
        toast("Opportunity saved");
        state.editingOpportunity = false;
        await refreshCurrentData();
      }
      return;
    }

    const payload = Object.fromEntries(new FormData(event.target).entries());
    payload.actor_user_id = state.currentUserId;

    if (event.target.id === "intakeSubmissionForm") {
      payload.raw_payload = {
        submitted_by_name: payload.submitted_by_name || "",
        submitted_by_email: payload.submitted_by_email || "",
        summary: payload.summary || "",
      };
      await request("/api/v1/intake-submissions", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      toast("Intake item created");
      event.target.reset();
      await refreshCurrentData();
      return;
    }

    if (event.target.id === "relationshipLinkForm") {
      await request("/api/v1/relationship-links", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      toast("Relationship link created");
      event.target.reset();
      await refreshCurrentData();
      return;
    }

    if (event.target.id === "organizationForm") {
      const organizationId = Number(event.target.dataset.organizationId || 0);
      const saved = await request(organizationId ? `/api/v1/organizations/${organizationId}` : "/api/v1/organizations", {
        method: organizationId ? "PATCH" : "POST",
        body: JSON.stringify(payload),
      });
      toast(organizationId ? "Company profile saved" : "Organization created");
      state.selectedOrganizationId = saved.id;
      if (!organizationId) {
        event.target.reset();
      }
      await refreshCurrentData();
      return;
    }

    if (event.target.id === "pipelineRecordForm") {
      const created = await request("/api/v1/pipeline-records", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      toast("Pipeline record created");
      state.selectedPipelineRecordId = created.id;
      state.selectedOrganizationId = created.organization_id || state.selectedOrganizationId;
      event.target.reset();
      await refreshCurrentData();
      return;
    }
  } catch (error) {
    state.opportunityFormErrors = error.fields || {};
    setBanner("error", error.message);
    render();
  }
});

function downloadText(filename, content, type = "text/plain") {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

document.querySelector("#refreshAppButton").addEventListener("click", async () => {
  await refreshCurrentData();
  toast("Data refreshed");
});

document.querySelector("#globalAddOpportunityButton").addEventListener("click", () => {
  window.location.hash = "new-opportunity";
});

window.addEventListener("hashchange", setRouteFromHash);
window.addEventListener("keydown", (event) => {
  const companyCard = event.target.closest?.("[data-open-company-detail]");
  if (companyCard && (event.key === "Enter" || event.key === " ")) {
    event.preventDefault();
    openModal({ type: "company-detail", companyId: Number(companyCard.dataset.openCompanyDetail) });
    return;
  }
  if (event.key === "Escape" && state.modal) {
    closeModal();
  }
});

async function start() {
  try {
    await loadAll();
    setRouteFromHash();
    render();
  } catch (error) {
    setBanner("error", `Unable to load CRM: ${error.message}`);
  }
}

start();
