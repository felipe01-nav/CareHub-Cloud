//! CareHub forms.
//!
//! The open-source server has no Form support (upstream moved it to a closed
//! codebase), so CareHub implements the small set of routes the open-source
//! web app needs:
//!
//! * adding any database view type (Form, List, Gallery, ...) to a database
//! * creating / changing / revoking a form's share link (workspace members)
//! * showing a shared form and accepting answers (the public form page)
//!
//! Form questions are stored by the web app inside the database view itself
//! (`form_field_settings`), so this file only reads them. Each answer becomes
//! a new row of the database. Existing views and rows are never rewritten.

use crate::api::util::realtime_user_for_web_request;
use crate::biz::authentication::jwt::UserUuid;
use crate::biz::collab::ops::get_latest_workspace_database;
use crate::biz::collab::utils::{collab_to_bin, get_latest_collab_database_body};
use crate::biz::workspace::page_view::create_database_view_for_layout;
use crate::state::AppState;
use actix_web::http::StatusCode;
use actix_web::web::{self, Data, Json};
use actix_web::{HttpRequest, HttpResponse, Result as ActixResult};
use app_error::AppError;
use appflowy_collaborate::ws2::{CollabUpdatePublisher, WorkspaceCollabInstanceCache};
use chrono::{DateTime, NaiveDate, Utc};
use collab::core::collab::{default_client_id, CollabOptions};
use collab::preclude::{Any, Collab, MapExt, MapRef, ToJson};
use collab_database::database::DatabaseBody;
use collab_database::entity::FieldType;
use collab_database::fields::select_type_option::{SelectOptionIds, SingleSelectTypeOption};
use collab_database::fields::Field;
use collab_database::rows::{new_cell_builder, Cell, CreateRowParams, DatabaseRowBody, Row, RowId};
use collab_database::template::entity::CELL_DATA;
use collab_database::views::{DatabaseLayout, OrderObjectPosition};
use collab_entity::CollabType;
use collab_folder::CollabOrigin;
use database::collab::GetCollabOrigin;
use database_entity::dto::{AFRole, CollabParams};
use serde::Deserialize;
use serde_json::{json, Map as JsonMap, Value};
use shared_entity::dto::workspace_dto::ViewLayout;
use shared_entity::response::AppResponse;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const FORM_DECIDED_SENTINEL: &str = "__form_decided__";
const FORM_DESCRIPTION_SENTINEL: &str = "__form_description__";
const LEGACY_ORDER: f64 = 4294967295.0;
const MAX_TEXT_BYTES: usize = 32 * 1024;

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/workspace/public-form/{token}")
        .route(web::get().to(public_form_handler)),
    )
    .service(
      web::resource("/api/workspace/public-form/{token}/submit")
        .route(web::post().to(public_form_submit_handler)),
    )
    .service(
      web::resource("/api/workspace/public-form/{token}/upload-url")
        .route(web::post().to(public_form_upload_url_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/page-view/{view_id}/database-view")
        .route(web::post().to(create_database_view_handler)),
    )
    .service(
      web::resource(
        "/api/workspace/{workspace_id}/database/{database_id}/view/{view_id}/form/share",
      )
      .route(web::get().to(get_share_handler))
      .route(web::post().to(mint_share_handler))
      .route(web::patch().to(patch_share_handler))
      .route(web::delete().to(delete_share_handler)),
    )
    .service(
      web::resource(
        "/api/workspace/{workspace_id}/database/{database_id}/view/{view_id}/form/share/reset",
      )
      .route(web::post().to(reset_share_handler)),
    );
}

// ---------------------------------------------------------------------------
// Adding a database view of any type
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CreateDatabaseViewBody {
  layout: ViewLayout,
  name: Option<String>,
  #[serde(default)]
  parent_view_id: Option<Uuid>,
}

/// POST /api/workspace/{workspace_id}/page-view/{view_id}/database-view
/// Returns `{ view_id, database_id, database_update }` (what the web app expects).
async fn create_database_view_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  body: Json<CreateDatabaseViewBody>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, view_id) = path.into_inner();
  let body = body.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role_weak(&uid, &workspace_id, AFRole::Member)
    .await?;
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  let parent_view_id = match body.parent_view_id {
    Some(parent) => {
      let folder = state.ws_server.get_folder(workspace_id).await?;
      if folder.get_view(&parent.to_string(), uid).is_some() {
        Some(parent)
      } else {
        None
      }
    },
    None => None,
  };
  let (new_view_id, database_id, database_update) = create_database_view_for_layout(
    &state,
    user,
    workspace_id,
    &view_id,
    parent_view_id,
    &body.layout,
    body.name.as_deref(),
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "view_id": new_view_id.to_string(),
    "database_id": database_id.to_string(),
    "database_update": database_update,
  }))))
}

// ---------------------------------------------------------------------------
// Storage for share links
// ---------------------------------------------------------------------------

static SCHEMA_READY: AtomicBool = AtomicBool::new(false);

const SCHEMA_SQL: [&str; 3] = [
  "CREATE TABLE IF NOT EXISTS carehub_form_share (
    token UUID PRIMARY KEY,
    workspace_id UUID NOT NULL,
    database_id UUID NOT NULL,
    view_id UUID NOT NULL,
    tier TEXT NOT NULL,
    anonymous BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMPTZ NULL,
    created_by BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ NULL
  )",
  "CREATE INDEX IF NOT EXISTS carehub_form_share_view_idx
    ON carehub_form_share (workspace_id, database_id, view_id)",
  "CREATE TABLE IF NOT EXISTS carehub_form_submission (
    token UUID NOT NULL,
    idempotency_key UUID NOT NULL,
    submission_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (token, idempotency_key)
  )",
];

/// Creates the two CareHub form tables the first time they are needed.
async fn ensure_schema(pg_pool: &PgPool) -> Result<(), AppError> {
  if SCHEMA_READY.load(Ordering::Acquire) {
    return Ok(());
  }
  for attempt in 0..2 {
    let mut result: Result<(), sqlx::Error> = Ok(());
    for sql in SCHEMA_SQL {
      if let Err(err) = sqlx::query(sql).execute(pg_pool).await {
        result = Err(err);
        break;
      }
    }
    match result {
      Ok(()) => {
        SCHEMA_READY.store(true, Ordering::Release);
        return Ok(());
      },
      // Two first requests at the same moment can collide; try once more.
      Err(err) if attempt == 0 => tracing::warn!("carehub_forms: retrying table setup: {}", err),
      Err(err) => return Err(AppError::Internal(anyhow::anyhow!("form tables: {}", err))),
    }
  }
  Ok(())
}

#[derive(Debug, Clone)]
struct ShareRow {
  token: Uuid,
  workspace_id: Uuid,
  database_id: Uuid,
  view_id: Uuid,
  tier: String,
  anonymous: bool,
  expires_at: Option<DateTime<Utc>>,
  created_by: i64,
  created_at: DateTime<Utc>,
  revoked_at: Option<DateTime<Utc>>,
}

type ShareTuple = (
  Uuid,
  Uuid,
  Uuid,
  Uuid,
  String,
  bool,
  Option<DateTime<Utc>>,
  i64,
  DateTime<Utc>,
  Option<DateTime<Utc>>,
);

const SELECT_ACTIVE_SHARE: &str = "SELECT token, workspace_id, database_id, view_id, tier, \
  anonymous, expires_at, created_by, created_at, revoked_at FROM carehub_form_share \
  WHERE workspace_id = $1 AND database_id = $2 AND view_id = $3 AND revoked_at IS NULL \
  ORDER BY created_at DESC LIMIT 1";
const SELECT_SHARE_BY_TOKEN: &str = "SELECT token, workspace_id, database_id, view_id, tier, \
  anonymous, expires_at, created_by, created_at, revoked_at FROM carehub_form_share \
  WHERE token = $1";

fn share_from_tuple(t: ShareTuple) -> ShareRow {
  ShareRow {
    token: t.0,
    workspace_id: t.1,
    database_id: t.2,
    view_id: t.3,
    tier: t.4,
    anonymous: t.5,
    expires_at: t.6,
    created_by: t.7,
    created_at: t.8,
    revoked_at: t.9,
  }
}

async fn select_active_share(
  pg_pool: &PgPool,
  workspace_id: Uuid,
  database_id: Uuid,
  view_id: Uuid,
) -> Result<Option<ShareRow>, AppError> {
  ensure_schema(pg_pool).await?;
  let row = sqlx::query_as::<_, ShareTuple>(SELECT_ACTIVE_SHARE)
    .bind(workspace_id)
    .bind(database_id)
    .bind(view_id)
    .fetch_optional(pg_pool)
    .await?;
  Ok(row.map(share_from_tuple))
}

async fn select_share_by_token(pg_pool: &PgPool, token: Uuid) -> Result<Option<ShareRow>, AppError> {
  ensure_schema(pg_pool).await?;
  let row = sqlx::query_as::<_, ShareTuple>(SELECT_SHARE_BY_TOKEN)
    .bind(token)
    .fetch_optional(pg_pool)
    .await?;
  Ok(row.map(share_from_tuple))
}

#[allow(clippy::too_many_arguments)]
async fn insert_share(
  pg_pool: &PgPool,
  workspace_id: Uuid,
  database_id: Uuid,
  view_id: Uuid,
  tier: &str,
  anonymous: bool,
  expires_at: Option<DateTime<Utc>>,
  created_by: i64,
) -> Result<ShareRow, AppError> {
  ensure_schema(pg_pool).await?;
  let token = Uuid::new_v4();
  sqlx::query(
    "INSERT INTO carehub_form_share
      (token, workspace_id, database_id, view_id, tier, anonymous, expires_at, created_by)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
  )
  .bind(token)
  .bind(workspace_id)
  .bind(database_id)
  .bind(view_id)
  .bind(tier)
  .bind(anonymous)
  .bind(expires_at)
  .bind(created_by)
  .execute(pg_pool)
  .await?;
  select_share_by_token(pg_pool, token)
    .await?
    .ok_or_else(|| AppError::Internal(anyhow::anyhow!("form share was not saved")))
}

async fn revoke_share(pg_pool: &PgPool, token: Uuid) -> Result<(), AppError> {
  sqlx::query("UPDATE carehub_form_share SET revoked_at = NOW() WHERE token = $1")
    .bind(token)
    .execute(pg_pool)
    .await?;
  Ok(())
}

fn web_base_url(req: &HttpRequest) -> String {
  match std::env::var("APPFLOWY_WEB_URL") {
    Ok(url) if !url.trim().is_empty() => url.trim().trim_end_matches('/').to_string(),
    _ => {
      let info = req.connection_info();
      format!("{}://{}", info.scheme(), info.host())
    },
  }
}

fn share_info(req: &HttpRequest, share: &ShareRow) -> Value {
  let mut info = json!({
    "token": share.token.to_string(),
    "tier": share.tier,
    "anonymous": share.anonymous,
    "submission_access": "none",
    "share_url": format!("{}/form/{}", web_base_url(req), share.token),
    "created_at": share.created_at.to_rfc3339(),
  });
  if let Some(expires_at) = share.expires_at {
    info["expires_at"] = json!(expires_at.to_rfc3339());
  }
  info
}

fn parse_tier(value: Option<&Value>) -> Result<Option<String>, AppError> {
  match value {
    None | Some(Value::Null) => Ok(None),
    Some(Value::String(s)) if s == "workspace" || s == "public" || s == "closed" => {
      Ok(Some(s.clone()))
    },
    Some(other) => Err(AppError::InvalidRequest(format!(
      "Unknown form share tier: {}",
      other
    ))),
  }
}

/// `None` = not given, `Some(None)` = clear, `Some(Some(t))` = set.
fn parse_expiry(body: &JsonMap<String, Value>) -> Result<Option<Option<DateTime<Utc>>>, AppError> {
  match body.get("expires_at") {
    None => Ok(None),
    Some(Value::Null) => Ok(Some(None)),
    Some(Value::String(s)) => DateTime::parse_from_rfc3339(s)
      .map(|t| Some(Some(t.with_timezone(&Utc))))
      .map_err(|_| AppError::InvalidRequest("Invalid expires_at".to_string())),
    Some(_) => Err(AppError::InvalidRequest("Invalid expires_at".to_string())),
  }
}

fn body_object(body: Option<Json<Value>>) -> JsonMap<String, Value> {
  match body.map(|b| b.into_inner()) {
    Some(Value::Object(map)) => map,
    _ => JsonMap::new(),
  }
}

/// Checks the caller is a workspace member and that the view is a view of this
/// database inside this workspace. Returns the caller's uid.
async fn authorize_share_request(
  state: &AppState,
  user_uuid: &UserUuid,
  workspace_id: Uuid,
  database_id: Uuid,
  view_id: Uuid,
) -> Result<i64, AppError> {
  let uid = state.user_cache.get_user_uid(user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role_weak(&uid, &workspace_id, AFRole::Member)
    .await?;
  let (_, workspace_database) = get_latest_workspace_database(
    &state.collab_storage,
    &state.pg_pool,
    GetCollabOrigin::User { uid },
    workspace_id,
  )
  .await?;
  let meta = workspace_database
    .get_database_meta_with_view_id(&view_id.to_string())
    .ok_or_else(|| AppError::RecordNotFound(format!("Form view {} not found", view_id)))?;
  if meta.database_id != database_id.to_string() {
    return Err(AppError::RecordNotFound(format!(
      "Form view {} does not belong to database {}",
      view_id, database_id
    )));
  }
  Ok(uid)
}

type SharePath = web::Path<(Uuid, Uuid, Uuid)>;

/// GET .../form/share — the active share link, or null.
async fn get_share_handler(
  user_uuid: UserUuid,
  path: SharePath,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, database_id, view_id) = path.into_inner();
  authorize_share_request(&state, &user_uuid, workspace_id, database_id, view_id).await?;
  let share = select_active_share(&state.pg_pool, workspace_id, database_id, view_id).await?;
  let data = share.map(|s| share_info(&req, &s)).unwrap_or(Value::Null);
  Ok(Json(AppResponse::Ok().with_data(data)))
}

/// POST .../form/share — create the share link (only workspace members can
/// open it unless `tier` says otherwise).
async fn mint_share_handler(
  user_uuid: UserUuid,
  path: SharePath,
  body: Option<Json<Value>>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, database_id, view_id) = path.into_inner();
  let uid = authorize_share_request(&state, &user_uuid, workspace_id, database_id, view_id).await?;
  if let Some(existing) =
    select_active_share(&state.pg_pool, workspace_id, database_id, view_id).await?
  {
    return Ok(Json(AppResponse::Ok().with_data(share_info(&req, &existing))));
  }
  let body = body_object(body);
  let tier = parse_tier(body.get("tier"))?.unwrap_or_else(|| "workspace".to_string());
  let anonymous = body
    .get("anonymous")
    .and_then(|v| v.as_bool())
    .unwrap_or(false);
  let expires_at = parse_expiry(&body)?.flatten();
  let share = insert_share(
    &state.pg_pool,
    workspace_id,
    database_id,
    view_id,
    &tier,
    anonymous,
    expires_at,
    uid,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(share_info(&req, &share))))
}

/// PATCH .../form/share — change who can open the link.
async fn patch_share_handler(
  user_uuid: UserUuid,
  path: SharePath,
  body: Option<Json<Value>>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, database_id, view_id) = path.into_inner();
  authorize_share_request(&state, &user_uuid, workspace_id, database_id, view_id).await?;
  let share = match select_active_share(&state.pg_pool, workspace_id, database_id, view_id).await? {
    Some(share) => share,
    None => return Ok(Json(AppResponse::Ok().with_data(Value::Null))),
  };
  let body = body_object(body);
  let tier = parse_tier(body.get("tier"))?.unwrap_or_else(|| share.tier.clone());
  let anonymous = body
    .get("anonymous")
    .and_then(|v| v.as_bool())
    .unwrap_or(share.anonymous);
  let expires_at = match parse_expiry(&body)? {
    Some(value) => value,
    None => share.expires_at,
  };
  sqlx::query(
    "UPDATE carehub_form_share SET tier = $2, anonymous = $3, expires_at = $4 WHERE token = $1",
  )
  .bind(share.token)
  .bind(&tier)
  .bind(anonymous)
  .bind(expires_at)
  .execute(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  let updated = select_share_by_token(&state.pg_pool, share.token).await?;
  let data = updated.map(|s| share_info(&req, &s)).unwrap_or(Value::Null);
  Ok(Json(AppResponse::Ok().with_data(data)))
}

/// DELETE .../form/share — switch the link off for good.
async fn delete_share_handler(
  user_uuid: UserUuid,
  path: SharePath,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<()>>> {
  let (workspace_id, database_id, view_id) = path.into_inner();
  authorize_share_request(&state, &user_uuid, workspace_id, database_id, view_id).await?;
  if let Some(share) =
    select_active_share(&state.pg_pool, workspace_id, database_id, view_id).await?
  {
    revoke_share(&state.pg_pool, share.token).await?;
  }
  Ok(Json(AppResponse::Ok()))
}

/// POST .../form/share/reset — replace the link with a new one.
async fn reset_share_handler(
  user_uuid: UserUuid,
  path: SharePath,
  body: Option<Json<Value>>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, database_id, view_id) = path.into_inner();
  let uid = authorize_share_request(&state, &user_uuid, workspace_id, database_id, view_id).await?;
  let current = select_active_share(&state.pg_pool, workspace_id, database_id, view_id).await?;
  let body = body_object(body);
  let tier = parse_tier(body.get("tier"))?
    .or_else(|| current.as_ref().map(|s| s.tier.clone()))
    .unwrap_or_else(|| "workspace".to_string());
  let anonymous = body
    .get("anonymous")
    .and_then(|v| v.as_bool())
    .or_else(|| current.as_ref().map(|s| s.anonymous))
    .unwrap_or(false);
  let expires_at = match parse_expiry(&body)? {
    Some(value) => value,
    None => current.as_ref().and_then(|s| s.expires_at),
  };
  if let Some(current) = current {
    revoke_share(&state.pg_pool, current.token).await?;
  }
  let share = insert_share(
    &state.pg_pool,
    workspace_id,
    database_id,
    view_id,
    &tier,
    anonymous,
    expires_at,
    uid,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(share_info(&req, &share))))
}

// ---------------------------------------------------------------------------
// The form itself (read from the database view)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum QuestionKind {
  Text,
  Number,
  Url,
  Checkbox,
  SingleSelect,
  MultiSelect,
  Date,
}

impl QuestionKind {
  fn from_field_type(field_type: FieldType) -> Option<Self> {
    match field_type {
      FieldType::RichText => Some(Self::Text),
      FieldType::Number => Some(Self::Number),
      FieldType::URL => Some(Self::Url),
      FieldType::Checkbox => Some(Self::Checkbox),
      FieldType::SingleSelect => Some(Self::SingleSelect),
      FieldType::MultiSelect => Some(Self::MultiSelect),
      FieldType::DateTime => Some(Self::Date),
      _ => None,
    }
  }

  fn wire_name(&self) -> &'static str {
    match self {
      Self::Text => "text",
      Self::Number => "number",
      Self::Url => "url",
      Self::Checkbox => "checkbox",
      Self::SingleSelect => "single_select",
      Self::MultiSelect => "multi_select",
      Self::Date => "date",
    }
  }

  fn field_type(&self) -> FieldType {
    match self {
      Self::Text => FieldType::RichText,
      Self::Number => FieldType::Number,
      Self::Url => FieldType::URL,
      Self::Checkbox => FieldType::Checkbox,
      Self::SingleSelect => FieldType::SingleSelect,
      Self::MultiSelect => FieldType::MultiSelect,
      Self::Date => FieldType::DateTime,
    }
  }
}

#[derive(Debug, Clone)]
struct FormOption {
  id: String,
  label: String,
  color: Option<String>,
}

#[derive(Debug, Clone)]
struct FormQuestion {
  id: String,
  field_id: String,
  label: String,
  description: Option<String>,
  kind: QuestionKind,
  required: bool,
  long_answer: bool,
  options: Vec<FormOption>,
  show_if: Option<ShowIf>,
}

/// CareHub conditional logic: "show this question only when ...".
#[derive(Debug, Clone)]
struct ShowCondition {
  question_id: String,
  op: String,
  value: String,
}

#[derive(Debug, Clone)]
struct ShowIf {
  any: bool,
  conditions: Vec<ShowCondition>,
}

/// Reads the rule the web form builder saved as a JSON string under
/// `carehub_show_if`. Conditions point at database fields; they are turned
/// into the public question ids here.
fn parse_show_if(value: Option<&Value>, token: &Uuid) -> Option<ShowIf> {
  let raw = value?.as_str()?;
  let parsed: Value = serde_json::from_str(raw).ok()?;
  let any = parsed.get("match").and_then(|m| m.as_str()) == Some("any");
  let conditions: Vec<ShowCondition> = parsed
    .get("conditions")?
    .as_array()?
    .iter()
    .filter_map(|c| {
      let field_id = c.get("field_id")?.as_str()?;
      let op = c.get("op")?.as_str()?;
      Some(ShowCondition {
        question_id: Uuid::new_v5(token, field_id.as_bytes()).to_string(),
        op: op.to_string(),
        value: c
          .get("value")
          .and_then(|v| v.as_str())
          .unwrap_or_default()
          .trim()
          .to_string(),
      })
    })
    .collect();
  if conditions.is_empty() {
    None
  } else {
    Some(ShowIf { any, conditions })
  }
}

fn op_allowed(kind: &QuestionKind, op: &str) -> bool {
  let ops: &[&str] = match kind {
    QuestionKind::Text | QuestionKind::Url => {
      &["is", "is_not", "contains", "not_contains", "empty", "not_empty"]
    },
    QuestionKind::Number => &["is", "is_not", "gt", "lt", "empty", "not_empty"],
    QuestionKind::Checkbox => &["checked", "unchecked"],
    QuestionKind::SingleSelect => &["is", "is_not", "empty", "not_empty"],
    QuestionKind::MultiSelect => &["contains", "not_contains", "empty", "not_empty"],
    QuestionKind::Date => &["is", "gt", "lt", "empty", "not_empty"],
  };
  ops.contains(&op)
}

fn answer_text(answer: Option<&Value>) -> String {
  answer
    .and_then(|a| a.get("value"))
    .and_then(|v| v.as_str())
    .unwrap_or_default()
    .trim()
    .to_string()
}

fn answer_number(answer: Option<&Value>) -> Option<f64> {
  match answer.and_then(|a| a.get("value")) {
    Some(Value::Number(n)) => n.as_f64(),
    Some(Value::String(s)) => s.trim().parse::<f64>().ok(),
    _ => None,
  }
}

fn answer_option_ids(answer: Option<&Value>) -> Vec<String> {
  let answer = match answer {
    Some(answer) => answer,
    None => return vec![],
  };
  if let Some(id) = answer.get("option_id").and_then(|v| v.as_str()) {
    return if id.is_empty() { vec![] } else { vec![id.to_string()] };
  }
  answer
    .get("option_ids")
    .and_then(|v| v.as_array())
    .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
    .unwrap_or_default()
}

fn answer_day(answer: Option<&Value>) -> String {
  let iso = answer
    .and_then(|a| a.get("iso"))
    .and_then(|v| v.as_str())
    .unwrap_or_default()
    .trim();
  iso.chars().take(10).collect()
}

fn answer_checked(answer: Option<&Value>) -> bool {
  answer
    .and_then(|a| a.get("value"))
    .and_then(|v| v.as_bool())
    .unwrap_or(false)
}

fn answer_is_empty(kind: &QuestionKind, answer: Option<&Value>) -> bool {
  match kind {
    QuestionKind::Text | QuestionKind::Url => answer_text(answer).is_empty(),
    QuestionKind::Number => answer_number(answer).is_none(),
    QuestionKind::Checkbox => !answer_checked(answer),
    QuestionKind::SingleSelect | QuestionKind::MultiSelect => answer_option_ids(answer).is_empty(),
    QuestionKind::Date => answer_day(answer).is_empty(),
  }
}

/// `None` = the condition is incomplete and is ignored (same as the web app).
fn condition_result(source: &FormQuestion, op: &str, value: &str, answer: Option<&Value>) -> Option<bool> {
  if !op_allowed(&source.kind, op) {
    return None;
  }
  let needs_value = !matches!(op, "empty" | "not_empty" | "checked" | "unchecked");
  if needs_value && value.is_empty() {
    return None;
  }
  match op {
    "empty" => return Some(answer_is_empty(&source.kind, answer)),
    "not_empty" => return Some(!answer_is_empty(&source.kind, answer)),
    "checked" => return Some(answer_checked(answer)),
    "unchecked" => return Some(!answer_checked(answer)),
    _ => {},
  }
  match source.kind {
    QuestionKind::Number => {
      let target = value.parse::<f64>().ok()?;
      let number = answer_number(answer);
      match (op, number) {
        ("is_not", None) => Some(true),
        (_, None) => Some(false),
        ("is", Some(n)) => Some(n == target),
        ("is_not", Some(n)) => Some(n != target),
        ("gt", Some(n)) => Some(n > target),
        ("lt", Some(n)) => Some(n < target),
        _ => None,
      }
    },
    QuestionKind::SingleSelect | QuestionKind::MultiSelect => {
      let ids = answer_option_ids(answer);
      let has = ids.iter().any(|id| id == value);
      match op {
        "is" | "contains" => Some(has),
        "is_not" | "not_contains" => Some(!has),
        _ => None,
      }
    },
    QuestionKind::Date => {
      let day = answer_day(answer);
      let target: String = value.chars().take(10).collect();
      if day.is_empty() {
        return Some(false);
      }
      match op {
        "is" => Some(day == target),
        "gt" => Some(day > target),
        "lt" => Some(day < target),
        _ => None,
      }
    },
    QuestionKind::Text | QuestionKind::Url => {
      let text = answer_text(answer).to_lowercase();
      let target = value.to_lowercase();
      match op {
        "is" => Some(text == target),
        "is_not" => Some(text != target),
        "contains" => Some(text.contains(&target)),
        "not_contains" => Some(!text.contains(&target)),
        _ => None,
      }
    },
    QuestionKind::Checkbox => None,
  }
}

/// Ids of the questions that are shown for these answers. Hidden questions
/// count as unanswered, so a chain of rules hides everything below it.
fn visible_question_ids(questions: &[FormQuestion], answers: &HashMap<String, Value>) -> HashSet<String> {
  let by_id: HashMap<&str, &FormQuestion> = questions.iter().map(|q| (q.id.as_str(), q)).collect();
  let mut visible: HashSet<String> = questions.iter().map(|q| q.id.clone()).collect();
  for _ in 0..=questions.len() {
    let mut next: HashSet<String> = HashSet::new();
    for q in questions {
      let results: Vec<bool> = match &q.show_if {
        Some(rule) => rule
          .conditions
          .iter()
          .filter_map(|c| {
            let source = by_id.get(c.question_id.as_str())?;
            if source.id == q.id {
              return None;
            }
            let answer = if visible.contains(&source.id) {
              answers.get(&source.id)
            } else {
              None
            };
            condition_result(source, &c.op, &c.value, answer)
          })
          .collect(),
        None => vec![],
      };
      let any = q.show_if.as_ref().map(|r| r.any).unwrap_or(false);
      let shown = results.is_empty()
        || if any {
          results.iter().any(|r| *r)
        } else {
          results.iter().all(|r| *r)
        };
      if shown {
        next.insert(q.id.clone());
      }
    }
    let same = next == visible;
    visible = next;
    if same {
      break;
    }
  }
  visible
}

struct FormSchema {
  title: String,
  description: Option<String>,
  questions: Vec<FormQuestion>,
}

fn json_bool(value: Option<&Value>, default: bool) -> bool {
  value.and_then(|v| v.as_bool()).unwrap_or(default)
}

fn json_str(value: Option<&Value>) -> String {
  value
    .and_then(|v| v.as_str())
    .unwrap_or_default()
    .trim()
    .to_string()
}

fn select_options(field: &Field, field_type: FieldType) -> Vec<FormOption> {
  let type_option = match field.get_any_type_option(field_type.type_id()) {
    Some(data) => SingleSelectTypeOption::from(data.clone()),
    None => SingleSelectTypeOption::default(),
  };
  type_option
    .options
    .iter()
    .map(|option| FormOption {
      id: option.id.clone(),
      label: option.name.clone(),
      color: serde_json::to_value(&option.color)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string())),
    })
    .collect()
}

/// Reads the form (title, description, questions) that the web form builder
/// saved in the database view. Mirrors the web app's own reading rules.
fn read_form_schema(
  collab: &Collab,
  db_body: &DatabaseBody,
  view_id: &str,
  token: &Uuid,
) -> Result<FormSchema, AppError> {
  let txn = collab.transact();
  if db_body.views.get_database_view_layout(&txn, view_id) != DatabaseLayout::Form {
    return Err(AppError::RecordNotFound("This view is not a form".to_string()));
  }
  let view_map: MapRef = db_body
    .views
    .get_with_txn(&txn, view_id)
    .ok_or_else(|| AppError::RecordNotFound("Form view not found".to_string()))?;
  let view_json: Value = serde_json::to_value(view_map.to_json(&txn)).unwrap_or_default();
  let settings = view_json
    .get("form_field_settings")
    .and_then(|v| v.as_object())
    .cloned()
    .unwrap_or_default();
  let decided = settings.contains_key(FORM_DECIDED_SENTINEL);
  let form_meta = settings.get(FORM_DESCRIPTION_SENTINEL);
  let mut title = json_str(form_meta.and_then(|m| m.get("title")));
  if title.is_empty() {
    title = json_str(view_json.get("name"));
  }
  if title.is_empty() {
    title = "Untitled form".to_string();
  }
  let description = Some(json_str(form_meta.and_then(|m| m.get("description"))))
    .filter(|d| !d.is_empty());

  let fields: HashMap<String, Field> = db_body
    .fields
    .get_all_fields(&txn)
    .into_iter()
    .map(|f| (f.id.clone(), f))
    .collect();
  let mut seen = HashSet::new();
  let field_ids: Vec<String> = db_body
    .views
    .get_field_orders(&txn, view_id)
    .into_iter()
    .map(|o| o.id)
    .filter(|id| seen.insert(id.clone()))
    .collect();

  let mut ranked: Vec<(f64, usize, FormQuestion)> = Vec::new();
  for (index, field_id) in field_ids.iter().enumerate() {
    let explicit = settings.get(field_id).and_then(|v| v.as_object());
    if explicit.is_none() && decided {
      continue;
    }
    let get = |key: &str| explicit.and_then(|e| e.get(key));
    if !json_bool(get("included"), true) {
      continue;
    }
    let field = match fields.get(field_id) {
      Some(field) => field,
      None => continue,
    };
    let field_type = FieldType::from(field.field_type);
    let kind = match QuestionKind::from_field_type(field_type) {
      Some(kind) => kind,
      None => continue,
    };
    let mut label = json_str(get("title"));
    if label.is_empty() {
      label = field.name.clone();
    }
    let description = if json_bool(get("description_visible"), false) {
      Some(json_str(get("description"))).filter(|d| !d.is_empty())
    } else {
      None
    };
    let options = match kind {
      QuestionKind::SingleSelect | QuestionKind::MultiSelect => select_options(field, field_type),
      _ => vec![],
    };
    let order = get("order")
      .and_then(|v| v.as_f64())
      .filter(|o| o.is_finite() && *o >= 0.0)
      .unwrap_or(LEGACY_ORDER);
    ranked.push((
      order,
      index,
      FormQuestion {
        id: Uuid::new_v5(token, field_id.as_bytes()).to_string(),
        field_id: field_id.clone(),
        label,
        description,
        long_answer: kind == QuestionKind::Text && json_bool(get("long_answer"), false),
        required: json_bool(get("required"), false),
        kind,
        options,
        show_if: parse_show_if(get("carehub_show_if"), token),
      },
    ));
  }
  ranked.sort_by(|a, b| {
    a.0
      .partial_cmp(&b.0)
      .unwrap_or(std::cmp::Ordering::Equal)
      .then(a.1.cmp(&b.1))
  });
  Ok(FormSchema {
    title,
    description,
    questions: ranked.into_iter().map(|(_, _, q)| q).collect(),
  })
}

fn question_json(q: &FormQuestion) -> Value {
  let mut value = json!({
    "id": q.id,
    "label": q.label,
    "kind": q.kind.wire_name(),
    "required": q.required,
    "long_answer": q.long_answer,
    "input_style": "auto",
  });
  if let Some(description) = &q.description {
    value["description"] = json!(description);
  }
  if matches!(q.kind, QuestionKind::SingleSelect | QuestionKind::MultiSelect) {
    let options: Vec<Value> = q
      .options
      .iter()
      .map(|o| {
        let mut option = json!({ "id": o.id, "label": o.label });
        if let Some(color) = &o.color {
          option["color"] = json!(color);
        }
        option
      })
      .collect();
    value["options"] = json!(options);
  }
  if let Some(rule) = &q.show_if {
    let conditions: Vec<Value> = rule
      .conditions
      .iter()
      .map(|c| json!({ "question_id": c.question_id, "op": c.op, "value": c.value }))
      .collect();
    value["show_if"] = json!({
      "match": if rule.any { "any" } else { "all" },
      "conditions": conditions,
    });
  }
  value
}

// ---------------------------------------------------------------------------
// Public form routes (no sign-in needed for "public" links)
// ---------------------------------------------------------------------------

fn error_response(status: StatusCode, code: &str) -> HttpResponse {
  HttpResponse::build(status).json(json!({ "error": code }))
}

enum Gate {
  Open { share: ShareRow, respondent_uid: Option<i64> },
  Respond(HttpResponse),
}

/// Finds the share link and decides whether this visitor may use it.
async fn open_form(
  state: &AppState,
  user_uuid: Option<UserUuid>,
  token: &str,
  for_submit: bool,
) -> Result<Gate, AppError> {
  let token = match Uuid::parse_str(token) {
    Ok(token) => token,
    Err(_) => return Ok(Gate::Respond(error_response(StatusCode::NOT_FOUND, "not_found"))),
  };
  let share = match select_share_by_token(&state.pg_pool, token).await? {
    Some(share) => share,
    None => return Ok(Gate::Respond(error_response(StatusCode::NOT_FOUND, "not_found"))),
  };
  let expired = share.expires_at.map(|t| t <= Utc::now()).unwrap_or(false);
  if share.revoked_at.is_some() || expired {
    return Ok(Gate::Respond(error_response(StatusCode::GONE, "form_revoked")));
  }
  if share.tier == "closed" {
    return Ok(Gate::Respond(if for_submit {
      error_response(StatusCode::FORBIDDEN, "form_closed")
    } else {
      HttpResponse::Ok().json(json!({
        "kind": "closed",
        "message": "This form is no longer accepting responses.",
      }))
    }));
  }
  let respondent_uid = match user_uuid {
    Some(user_uuid) => state.user_cache.get_user_uid(&user_uuid).await.ok(),
    None => None,
  };
  if share.tier == "workspace" {
    let uid = match respondent_uid {
      Some(uid) => uid,
      None => {
        return Ok(Gate::Respond(if for_submit {
          HttpResponse::Unauthorized().json(json!({
            "error": "auth_required",
            "login_url": "/login",
          }))
        } else {
          HttpResponse::Ok().json(json!({ "kind": "auth_required", "login_url": "/login" }))
        }));
      },
    };
    let is_member = state
      .workspace_access_control
      .enforce_role_weak(&uid, &share.workspace_id, AFRole::Member)
      .await
      .is_ok();
    if !is_member {
      return Ok(Gate::Respond(error_response(
        StatusCode::FORBIDDEN,
        "not_a_workspace_member",
      )));
    }
  }
  Ok(Gate::Open {
    share,
    respondent_uid,
  })
}

async fn load_form(
  state: &AppState,
  share: &ShareRow,
) -> Result<(Collab, DatabaseBody, FormSchema), AppError> {
  let (collab, db_body) =
    get_latest_collab_database_body(&state.collab_storage, share.workspace_id, share.database_id)
      .await?;
  let schema = read_form_schema(&collab, &db_body, &share.view_id.to_string(), &share.token)?;
  Ok((collab, db_body, schema))
}

/// GET /api/workspace/public-form/{token}
async fn public_form_handler(
  user_uuid: Option<UserUuid>,
  token: web::Path<String>,
  state: Data<AppState>,
) -> ActixResult<HttpResponse> {
  let share = match open_form(&state, user_uuid, &token, false).await? {
    Gate::Open { share, .. } => share,
    Gate::Respond(response) => return Ok(response),
  };
  let schema = match load_form(&state, &share).await {
    Ok((_, _, schema)) => schema,
    Err(err) => {
      tracing::warn!("carehub_forms: form {} unavailable: {}", share.token, err);
      return Ok(error_response(StatusCode::NOT_FOUND, "not_found"));
    },
  };
  let mut body = json!({
    "kind": "active",
    "form_id": share.token.to_string(),
    "tier": share.tier,
    "anonymous": share.anonymous,
    "title": schema.title,
    "questions": schema.questions.iter().map(question_json).collect::<Vec<_>>(),
    "submit_label": "Submit",
    "submit_color": "primary",
    "confirmation_title": "Thank you – your response was sent.",
    "allow_another_response": true,
    "hide_branding": true,
  });
  if let Some(description) = schema.description {
    body["description"] = json!(description);
  }
  Ok(HttpResponse::Ok().json(body))
}

/// POST /api/workspace/public-form/{token}/upload-url
/// File questions are not offered yet, so there is nothing to upload.
async fn public_form_upload_url_handler(_token: web::Path<String>) -> HttpResponse {
  error_response(StatusCode::BAD_REQUEST, "no_upload_capable_question")
}

#[derive(Debug, Deserialize)]
struct SubmitBody {
  #[serde(default)]
  answers: HashMap<String, Value>,
}

fn text_cell(field_type: FieldType, text: String) -> Cell {
  let mut cell = new_cell_builder(field_type);
  cell.insert(CELL_DATA.into(), text.into());
  cell
}

fn parse_date(iso: &str) -> Option<(i64, bool)> {
  let iso = iso.trim();
  if let Ok(t) = DateTime::parse_from_rfc3339(iso) {
    return Some((t.timestamp(), true));
  }
  let date_part = iso.get(0..10).unwrap_or(iso);
  NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
    .ok()
    .and_then(|d| d.and_hms_opt(0, 0, 0))
    .map(|dt| (dt.and_utc().timestamp(), false))
}

/// Turns one answer into a database cell. `Ok(None)` = left empty.
fn answer_to_cell(q: &FormQuestion, answer: &Value) -> Result<Option<Cell>, ()> {
  let kind = answer.get("kind").and_then(|k| k.as_str()).unwrap_or_default();
  match q.kind {
    QuestionKind::Text | QuestionKind::Url => {
      let text = match kind {
        "text" => answer.get("value").and_then(|v| v.as_str()).ok_or(())?,
        _ => return Err(()),
      };
      let text = text.trim();
      if text.len() > MAX_TEXT_BYTES {
        return Err(());
      }
      if text.is_empty() {
        return Ok(None);
      }
      Ok(Some(text_cell(q.kind.field_type(), text.to_string())))
    },
    QuestionKind::Number => {
      let number = match (kind, answer.get("value")) {
        (_, None) | (_, Some(Value::Null)) => return Ok(None),
        ("number", Some(Value::Number(n))) => n.to_string(),
        ("text", Some(Value::String(s))) if s.trim().is_empty() => return Ok(None),
        ("text", Some(Value::String(s))) => {
          s.trim().parse::<f64>().map_err(|_| ())?;
          s.trim().to_string()
        },
        _ => return Err(()),
      };
      Ok(Some(text_cell(FieldType::Number, number)))
    },
    QuestionKind::Checkbox => {
      if kind != "checkbox" {
        return Err(());
      }
      let checked = answer.get("value").and_then(|v| v.as_bool()).unwrap_or(false);
      if !checked {
        return Ok(None);
      }
      Ok(Some(text_cell(FieldType::Checkbox, "Yes".to_string())))
    },
    QuestionKind::SingleSelect => {
      if kind != "single_select" {
        return Err(());
      }
      let option_id = match answer.get("option_id").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id,
        _ => return Ok(None),
      };
      if !q.options.iter().any(|o| o.id == option_id) {
        return Err(());
      }
      Ok(Some(
        SelectOptionIds::from(vec![option_id.to_string()]).to_cell(FieldType::SingleSelect),
      ))
    },
    QuestionKind::MultiSelect => {
      if kind != "multi_select" {
        return Err(());
      }
      let ids: Vec<String> = answer
        .get("option_ids")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default();
      if ids.is_empty() {
        return Ok(None);
      }
      if !ids.iter().all(|id| q.options.iter().any(|o| &o.id == id)) {
        return Err(());
      }
      Ok(Some(SelectOptionIds::from(ids).to_cell(FieldType::MultiSelect)))
    },
    QuestionKind::Date => {
      if kind != "date" {
        return Err(());
      }
      let iso = match answer.get("iso").and_then(|v| v.as_str()) {
        Some(iso) if !iso.trim().is_empty() => iso,
        _ => return Ok(None),
      };
      let (timestamp, include_time) = parse_date(iso).ok_or(())?;
      let mut cell = text_cell(FieldType::DateTime, timestamp.to_string());
      cell.insert("include_time".into(), Any::Bool(include_time));
      cell.insert("is_range".into(), Any::Bool(false));
      Ok(Some(cell))
    },
  }
}

/// POST /api/workspace/public-form/{token}/submit — adds one row per response.
async fn public_form_submit_handler(
  user_uuid: Option<UserUuid>,
  token: web::Path<String>,
  body: Json<SubmitBody>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<HttpResponse> {
  let (share, respondent_uid) = match open_form(&state, user_uuid, &token, true).await? {
    Gate::Open {
      share,
      respondent_uid,
    } => (share, respondent_uid),
    Gate::Respond(response) => return Ok(response),
  };

  let idempotency_key = req
    .headers()
    .get("Idempotency-Key")
    .and_then(|v| v.to_str().ok())
    .and_then(|v| Uuid::parse_str(v.trim()).ok())
    .unwrap_or_else(Uuid::new_v4);
  let previous: Option<(Uuid,)> = sqlx::query_as(
    "SELECT submission_id FROM carehub_form_submission WHERE token = $1 AND idempotency_key = $2",
  )
  .bind(share.token)
  .bind(idempotency_key)
  .fetch_optional(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  if let Some((submission_id,)) = previous {
    return Ok(HttpResponse::Ok().json(json!({
      "submission_id": submission_id.to_string(),
      "status": "accepted",
    })));
  }

  let (mut db_collab, db_body, schema) = match load_form(&state, &share).await {
    Ok(loaded) => loaded,
    Err(err) => {
      tracing::warn!("carehub_forms: form {} unavailable: {}", share.token, err);
      return Ok(error_response(StatusCode::NOT_FOUND, "not_found"));
    },
  };

  let answers = body.into_inner().answers;
  let mut cells: Vec<(String, Cell)> = Vec::new();
  let mut missing: Vec<String> = Vec::new();
  // CareHub: questions hidden by their rule are skipped (not required, not saved).
  let visible = visible_question_ids(&schema.questions, &answers);
  for q in &schema.questions {
    if !visible.contains(&q.id) {
      continue;
    }
    let cell = match answers.get(&q.id) {
      Some(answer) => match answer_to_cell(q, answer) {
        Ok(cell) => cell,
        Err(()) => return Ok(error_response(StatusCode::BAD_REQUEST, "invalid_answers")),
      },
      None => None,
    };
    match cell {
      Some(cell) => cells.push((q.field_id.clone(), cell)),
      None if q.required => missing.push(q.id.clone()),
      None => {},
    }
  }
  if !missing.is_empty() {
    return Ok(HttpResponse::BadRequest().json(json!({
      "error": "missing_required_answers",
      "question_ids": missing,
    })));
  }

  // Build the new row.
  let workspace_id = share.workspace_id;
  let carehub_form_view = share.view_id;
  let database_id = share.database_id;
  let storage_uid = respondent_uid.unwrap_or(share.created_by);
  let row_uuid = Uuid::new_v4();
  let row_id = RowId::from(row_uuid.to_string());
  let client_id = default_client_id();
  let now = Utc::now().timestamp();
  let options = CollabOptions::new(row_uuid.to_string(), client_id);
  let mut row_collab = Collab::new_with_options(CollabOrigin::Empty, options)
    .map_err(|err| AppError::Internal(err.into()))?;
  let row_body = DatabaseRowBody::create(
    row_id.clone(),
    &mut row_collab,
    Row::empty(row_id.clone(), &database_id.to_string()),
  );
  {
    let mut txn = row_collab.transact_mut();
    row_body.update(&mut txn, |row_update| {
      row_update.set_created_at(now);
    });
    row_body.update(&mut txn, |row_update| {
      row_update.set_last_modified(now);
    });
    for (field_id, cell) in cells {
      row_body.update(&mut txn, |row_update| {
        row_update.update_cells(|cells_update| {
          cells_update.insert_cell(&field_id, cell);
        });
      });
    }
  }
  let row_cells = row_body.cells(&row_collab.transact()).unwrap_or_default();
  let row_order = db_body
    .create_row(
      CreateRowParams {
        id: row_id.clone(),
        database_id: database_id.to_string(),
        cells: row_cells,
        height: 30,
        visibility: true,
        row_position: OrderObjectPosition::End,
        created_at: now,
        modified_at: now,
      },
      client_id,
    )
    .await
    .map_err(|err| AppError::Internal(anyhow::anyhow!("Failed to create row: {:?}", err)))?;
  let row_bin = collab_to_bin(row_collab, CollabType::DatabaseRow).await?;

  // Save the row first, then add it (at the end) to every view of the
  // database. Views are only appended to, never rewritten, so the form's
  // own settings stay untouched.
  let mut pg_txn = state.pg_pool.begin().await.map_err(AppError::from)?;
  state
    .collab_storage
    .upsert_new_collab_with_transaction(
      workspace_id,
      &storage_uid,
      CollabParams {
        object_id: row_uuid,
        encoded_collab_v1: row_bin.into(),
        collab_type: CollabType::DatabaseRow,
        updated_at: None,
      },
      &mut pg_txn,
      "carehub form response",
    )
    .await?;
  sqlx::query(
    "INSERT INTO carehub_form_submission (token, idempotency_key, submission_id)
      VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
  )
  .bind(share.token)
  .bind(idempotency_key)
  .bind(row_uuid)
  .execute(&mut *pg_txn)
  .await
  .map_err(AppError::from)?;
  pg_txn.commit().await.map_err(AppError::from)?;

  let db_update = {
    let mut txn = db_collab.transact_mut();
    db_body.views.update_all_views(&mut txn, |_view_id, update| {
      update.insert_row_order(&row_order, &OrderObjectPosition::End);
    });
    txn.encode_update_v1()
  };
  state
    .ws_server
    .publish_update(
      workspace_id,
      database_id,
      CollabType::Database,
      &CollabOrigin::Server,
      db_update,
    )
    .await
    .map_err(AppError::from)?;

  // CareHub automations: "When a form gets a response".
  crate::api::carehub_extras::on_form_submitted(state.pg_pool.clone(), workspace_id, carehub_form_view);

  Ok(HttpResponse::Ok().json(json!({
    "submission_id": row_uuid.to_string(),
    "status": "accepted",
  })))
}
