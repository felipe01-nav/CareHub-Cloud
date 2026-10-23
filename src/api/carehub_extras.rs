//! CareHub extras: roles & permissions, page analytics, page versions,
//! reminders, automations, the status page and data retention.
//!
//! Everything is stored in a few small `carehub_*` tables that are created on
//! first use. A background task (started from application.rs) delivers
//! reminders, runs scheduled automations, records uptime and cleans up old
//! data once an hour.

use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Bytes, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use base64::Engine;
use chrono::{Datelike, Duration as ChronoDuration, NaiveTime};
use database_entity::dto::AFRole;
use serde::Deserialize;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use uuid::Uuid;

static READY: AtomicBool = AtomicBool::new(false);
static STARTED_MS: AtomicI64 = AtomicI64::new(0);
static LAST_BACKUP_ALERT_MS: AtomicI64 = AtomicI64::new(0);

const DAY_MS: i64 = 86_400_000;
const BUCKET_MS: i64 = 300_000;
const MAX_VERSION_BYTES: usize = 32 * 1024 * 1024;
const MAX_SETTING_BYTES: usize = 4 * 1024 * 1024;
const AUTO_VERSION_GAP_MS: i64 = 4 * 60_000;
const VIEW_DEDUPE_MS: i64 = 10 * 60_000;
const MAX_AUTO_VERSIONS_PER_PAGE: i64 = 200;
const SYSTEM_CHAT_NAME: &str = "🔔 Care Hub";
const PERMS: [&str; 12] = [
  "edit",
  "create",
  "comment",
  "share",
  "ai",
  "templates",
  "analytics",
  "automations",
  "learning_build",
  "learning_assign",
  "learning_analytics",
  "admin",
];

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(web::resource("/api/carehub/status").route(web::get().to(status_handler)))
    .service(web::resource("/api/carehub/ws/{ws}/me").route(web::get().to(me_handler)))
    .service(
      web::resource("/api/carehub/ws/{ws}/settings").route(web::get().to(get_settings_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/settings/{key}")
        .app_data(web::JsonConfig::default().limit(MAX_SETTING_BYTES))
        .route(web::put().to(put_setting_handler)),
    )
    .service(web::resource("/api/carehub/ws/{ws}/members").route(web::get().to(members_handler)))
    .service(
      web::resource("/api/carehub/ws/{ws}/analytics").route(web::get().to(analytics_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/analytics/view")
        .route(web::post().to(record_view_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/analytics/page/{page}")
        .route(web::get().to(page_analytics_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/pages/{page}/versions")
        .app_data(web::PayloadConfig::new(MAX_VERSION_BYTES + 1024))
        .route(web::get().to(list_versions_handler))
        .route(web::post().to(save_version_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/versions/{id}")
        .route(web::get().to(get_version_handler))
        .route(web::put().to(rename_version_handler))
        .route(web::delete().to(delete_version_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/reminders")
        .route(web::get().to(list_reminders_handler))
        .route(web::post().to(create_reminder_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/reminders/{id}")
        .route(web::delete().to(delete_reminder_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/automations/status")
        .route(web::get().to(automation_status_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/automations/{rule}/run")
        .route(web::post().to(run_automation_handler)),
    );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn now_ms() -> i64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map(|d| d.as_millis() as i64)
    .unwrap_or(0)
}

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn clip(s: &str, max: usize) -> String {
  s.chars().take(max).collect()
}

fn ok(data: Value) -> ActixResult<Json<AppResponse<Value>>> {
  Ok(Json(AppResponse::Ok().with_data(data)))
}

fn admin_emails() -> Vec<String> {
  std::env::var("CAREHUB_ADMIN_EMAILS")
    .unwrap_or_default()
    .split(',')
    .map(|s| s.trim().to_lowercase())
    .filter(|s| !s.is_empty())
    .collect()
}

/// A page card in a message, the same format the Messages page uses.
fn page_card(ws: Uuid, page: Uuid, title: &str) -> String {
  let clean: String = title
    .chars()
    .filter(|c| *c != '|' && *c != ']' && *c != '[' && *c != '\n')
    .collect();
  let clean = if clean.trim().is_empty() {
    "Untitled".to_string()
  } else {
    clip(clean.trim(), 120)
  };
  format!("[[page:{}:{}|{}]]", ws, page, clean)
}

fn str_of<'a>(v: &'a Value, key: &str) -> &'a str {
  v.get(key).and_then(|x| x.as_str()).unwrap_or("")
}

fn duration_text(ms: i64) -> String {
  let minutes = (ms / 60_000).max(1);
  let (h, m) = (minutes / 60, minutes % 60);
  if h >= 24 {
    format!("{} day(s) {} h", h / 24, h % 24)
  } else if h > 0 {
    format!("{} h {} min", h, m)
  } else {
    format!("{} min", m)
  }
}

pub(crate) async fn ensure_tables(pool: &PgPool) -> Result<(), AppError> {
  if READY.load(Ordering::Relaxed) {
    return Ok(());
  }
  let statements = [
    "CREATE TABLE IF NOT EXISTS carehub_setting (\
       workspace_id UUID NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL DEFAULT '', \
       updated_ms BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (workspace_id, key))",
    "CREATE TABLE IF NOT EXISTS carehub_page_view (\
       id BIGSERIAL PRIMARY KEY, workspace_id UUID NOT NULL, page_id UUID NOT NULL, \
       uid BIGINT NOT NULL, kind TEXT NOT NULL DEFAULT 'view', title TEXT NOT NULL DEFAULT '', \
       ts_ms BIGINT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS carehub_page_view_ws_ts ON carehub_page_view (workspace_id, ts_ms)",
    "CREATE INDEX IF NOT EXISTS carehub_page_view_page_ts ON carehub_page_view (page_id, ts_ms)",
    "CREATE TABLE IF NOT EXISTS carehub_page_version (\
       id UUID PRIMARY KEY, workspace_id UUID NOT NULL, page_id UUID NOT NULL, \
       uid BIGINT NOT NULL, name TEXT NOT NULL DEFAULT '', auto BOOLEAN NOT NULL DEFAULT TRUE, \
       size INT NOT NULL DEFAULT 0, state BYTEA NOT NULL, created_ms BIGINT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS carehub_page_version_page ON carehub_page_version (page_id, created_ms)",
    "CREATE TABLE IF NOT EXISTS carehub_reminder (\
       id UUID PRIMARY KEY, workspace_id UUID NOT NULL, uid BIGINT NOT NULL, page_id UUID, \
       title TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '', remind_ms BIGINT NOT NULL, \
       created_ms BIGINT NOT NULL, sent_ms BIGINT NOT NULL DEFAULT 0)",
    "CREATE INDEX IF NOT EXISTS carehub_reminder_due ON carehub_reminder (sent_ms, remind_ms)",
    "CREATE TABLE IF NOT EXISTS carehub_automation_state (\
       workspace_id UUID NOT NULL, rule_id TEXT NOT NULL, last_run_ms BIGINT NOT NULL DEFAULT 0, \
       runs BIGINT NOT NULL DEFAULT 0, chat_id UUID, last_error TEXT NOT NULL DEFAULT '', \
       PRIMARY KEY (workspace_id, rule_id))",
    "CREATE TABLE IF NOT EXISTS carehub_heartbeat (ts_ms BIGINT PRIMARY KEY)",
  ];
  for sql in statements {
    sqlx::query(sql).execute(pool).await?;
  }
  READY.store(true, Ordering::Relaxed);
  Ok(())
}

async fn get_setting(pool: &PgPool, ws: Uuid, key: &str) -> Result<Value, AppError> {
  let raw: Option<String> =
    sqlx::query_scalar("SELECT value FROM carehub_setting WHERE workspace_id = $1 AND key = $2")
      .bind(ws)
      .bind(key)
      .fetch_optional(pool)
      .await?;
  Ok(
    raw
      .and_then(|s| serde_json::from_str::<Value>(&s).ok())
      .unwrap_or(Value::Null),
  )
}

async fn put_setting(pool: &PgPool, ws: Uuid, key: &str, value: &Value) -> Result<(), AppError> {
  let text = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
  sqlx::query(
    "INSERT INTO carehub_setting (workspace_id, key, value, updated_ms) VALUES ($1, $2, $3, $4) \
     ON CONFLICT (workspace_id, key) DO UPDATE SET value = EXCLUDED.value, updated_ms = EXCLUDED.updated_ms",
  )
  .bind(ws)
  .bind(key)
  .bind(text)
  .bind(now_ms())
  .execute(pool)
  .await?;
  Ok(())
}

fn retention_of(v: &Value) -> (i64, i64, i64) {
  let num = |key: &str, default: i64| {
    v.get(key)
      .and_then(|x| x.as_i64())
      .unwrap_or(default)
      .clamp(0, 3650)
  };
  (num("versions_days", 30), num("analytics_days", 365), num("trash_days", 0))
}

// ---------------------------------------------------------------------------
// roles & permissions
// ---------------------------------------------------------------------------

fn preset_roles() -> Vec<Value> {
  vec![
    json!({ "id": "admin", "name": "Admin", "builtin": true, "perms": PERMS.to_vec(),
      "description": "Everything, including roles, automations, analytics and data settings." }),
    json!({ "id": "editor", "name": "Editor", "builtin": true,
      "perms": ["edit", "create", "comment", "share", "ai", "templates"],
      "description": "Create and edit pages, comment, share and use Claude." }),
    json!({ "id": "commenter", "name": "Commenter", "builtin": true, "perms": ["comment", "ai"],
      "description": "Read pages and leave comments, but not change them." }),
    json!({ "id": "viewer", "name": "Viewer", "builtin": true, "perms": [],
      "description": "Read only." }),
  ]
}

fn clean_perms(v: Option<&Value>) -> Vec<String> {
  let mut out: Vec<String> = Vec::new();
  if let Some(list) = v.and_then(|x| x.as_array()) {
    for p in list {
      if let Some(p) = p.as_str() {
        if PERMS.contains(&p) && !out.iter().any(|x| x == p) {
          out.push(p.to_string());
        }
      }
    }
  }
  out
}

fn roles_list(cfg: &Value) -> Vec<Value> {
  let mut list = preset_roles();
  if let Some(custom) = cfg.get("custom").and_then(|v| v.as_array()) {
    for r in custom.iter().take(30) {
      let id = str_of(r, "id");
      if id.is_empty() || list.iter().any(|x| str_of(x, "id") == id) {
        continue;
      }
      list.push(json!({
        "id": clip(id, 40),
        "name": clip(str_of(r, "name"), 40),
        "builtin": false,
        "perms": clean_perms(r.get("perms")),
        "description": clip(str_of(r, "description"), 200),
      }));
    }
  }
  list
}

fn role_info(list: &[Value], id: &str) -> Option<(String, String, Vec<String>)> {
  list.iter().find(|r| str_of(r, "id") == id).map(|r| {
    (
      id.to_string(),
      str_of(r, "name").to_string(),
      clean_perms(r.get("perms")),
    )
  })
}

/// Which role someone has: CAREHUB_ADMIN_EMAILS are always admins, guests are
/// always viewers, then the assignment, then owner = admin, then the default.
fn resolve_role_id(cfg: &Value, email: &str, af_role: i32) -> String {
  let owner: i32 = AFRole::Owner.into();
  let guest: i32 = AFRole::Guest.into();
  if !email.is_empty() && admin_emails().iter().any(|e| e == email) {
    return "admin".to_string();
  }
  if af_role == guest {
    return "viewer".to_string();
  }
  if let Some(id) = cfg
    .get("assign")
    .and_then(|a| a.get(email))
    .and_then(|v| v.as_str())
  {
    return id.to_string();
  }
  if af_role == owner {
    return "admin".to_string();
  }
  cfg
    .get("default")
    .and_then(|v| v.as_str())
    .unwrap_or("editor")
    .to_string()
}

#[derive(Clone, Debug)]
pub(crate) struct Me {
  pub uid: i64,
  pub email: String,
  pub af_role: i32,
  pub role_id: String,
  pub role_name: String,
  pub perms: Vec<String>,
}

impl Me {
  fn can(&self, perm: &str) -> bool {
    self.perms.iter().any(|p| p == perm)
  }
}

pub(crate) async fn me_in(pool: &PgPool, ws: Uuid, uid: i64) -> Result<Me, AppError> {
  ensure_tables(pool).await?;
  let row: Option<(i32, String)> = sqlx::query_as(
    "SELECT m.role_id::INT, LOWER(COALESCE(u.email, '')) FROM af_workspace_member m \
     JOIN af_user u ON u.uid = m.uid WHERE m.workspace_id = $1 AND m.uid = $2",
  )
  .bind(ws)
  .bind(uid)
  .fetch_optional(pool)
  .await?;
  let (af_role, email) = row.ok_or(AppError::NotEnoughPermissions)?;
  let cfg = get_setting(pool, ws, "roles").await?;
  let list = roles_list(&cfg);
  let wanted = resolve_role_id(&cfg, email.trim(), af_role);
  let (role_id, role_name, perms) = role_info(&list, &wanted)
    .or_else(|| role_info(&list, "editor"))
    .unwrap_or_else(|| ("editor".to_string(), "Editor".to_string(), Vec::new()));
  Ok(Me {
    uid,
    email: email.trim().to_string(),
    af_role,
    role_id,
    role_name,
    perms,
  })
}

async fn me_for(state: &AppState, user_uuid: &UserUuid, ws: Uuid) -> Result<Me, AppError> {
  let uid = state.user_cache.get_user_uid(user_uuid).await?;
  me_in(&state.pg_pool, ws, uid).await
}

fn require(me: &Me, perm: &str) -> Result<(), AppError> {
  if me.can(perm) || me.can("admin") {
    Ok(())
  } else {
    Err(AppError::NotEnoughPermissions)
  }
}

/// For the editor permission answer: (edit, comment, share, create).
/// `None` when roles could not be read (then nothing extra is restricted).
pub(crate) async fn collab_caps(
  pool: &PgPool,
  ws: Uuid,
  uid: i64,
) -> Option<(bool, bool, bool, bool)> {
  match me_in(pool, ws, uid).await {
    Ok(me) => {
      let admin = me.can("admin");
      Some((
        admin || me.can("edit"),
        admin || me.can("comment") || me.can("edit"),
        admin || me.can("share"),
        admin || me.can("create"),
      ))
    },
    Err(_) => None,
  }
}

/// Used when a page is created: refuses people whose role may not create pages.
pub(crate) async fn require_perm(pool: &PgPool, ws: Uuid, uid: i64, perm: &str) -> Result<(), AppError> {
  match me_in(pool, ws, uid).await {
    Ok(me) => require(&me, perm),
    // Not a member, or the settings could not be read: leave it to the normal checks.
    Err(_) => Ok(()),
  }
}

/// Someone given the Admin role (or a custom role with "admin") in any workspace.
pub(crate) async fn is_assigned_admin(pool: &PgPool, email: &str) -> bool {
  if ensure_tables(pool).await.is_err() {
    return false;
  }
  let rows: Vec<String> =
    match sqlx::query_scalar("SELECT value FROM carehub_setting WHERE key = 'roles'")
      .fetch_all(pool)
      .await
    {
      Ok(rows) => rows,
      Err(_) => return false,
    };
  for raw in rows {
    let cfg: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
    if let Some(id) = cfg
      .get("assign")
      .and_then(|a| a.get(email))
      .and_then(|v| v.as_str())
    {
      if let Some((_, _, perms)) = role_info(&roles_list(&cfg), id) {
        if perms.iter().any(|p| p == "admin") {
          return true;
        }
      }
    }
  }
  false
}

// ---------------------------------------------------------------------------
// me, settings, members
// ---------------------------------------------------------------------------

/// GET /api/carehub/ws/{ws}/me
async fn me_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let ws = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  let owner: i32 = AFRole::Owner.into();
  let guest: i32 = AFRole::Guest.into();
  let cfg = get_setting(&state.pg_pool, ws, "roles").await?;
  ok(json!({
    "role": { "id": me.role_id, "name": me.role_name },
    "perms": me.perms,
    "email": me.email,
    "is_owner": me.af_role == owner,
    "is_guest": me.af_role == guest,
    "env_admin": admin_emails().iter().any(|e| *e == me.email),
    "roles": roles_list(&cfg),
  }))
}

/// GET /api/carehub/ws/{ws}/settings
async fn get_settings_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let ws = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  let pool = &state.pg_pool;
  let roles = get_setting(pool, ws, "roles").await?;
  let retention = get_setting(pool, ws, "retention").await?;
  let (versions_days, analytics_days, trash_days) = retention_of(&retention);
  let templates = get_setting(pool, ws, "templates").await?;
  let mut answer = json!({
    "roles": {
      "default": roles.get("default").cloned().unwrap_or(json!("editor")),
      "assign": roles.get("assign").cloned().unwrap_or(json!({})),
      "custom": roles.get("custom").cloned().unwrap_or(json!([])),
      "list": roles_list(&roles),
    },
    "retention": {
      "versions_days": versions_days,
      "analytics_days": analytics_days,
      "trash_days": trash_days,
    },
    "templates": templates.get("items").cloned().unwrap_or(json!([])),
  });
  if me.can("automations") || me.can("admin") {
    let automations = get_setting(pool, ws, "automations").await?;
    answer["automations"] = automations.get("rules").cloned().unwrap_or(json!([]));
  }
  if me.can("admin") {
    answer["trash_seen"] = get_setting(pool, ws, "trash_seen").await?;
  }
  ok(answer)
}

fn slug_ok(id: &str) -> bool {
  !id.is_empty()
    && id.len() <= 64
    && id
      .chars()
      .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Keeps who made each item (rule/template) and when, by id.
fn stamp_items(new_items: &[Value], old_items: &[Value], uid: i64, max: usize) -> Result<Vec<Value>, AppError> {
  let now = now_ms();
  let mut out = Vec::new();
  for item in new_items.iter().take(max) {
    let mut item = item.clone();
    if !item.is_object() {
      return Err(bad("Each item must be an object").into());
    }
    let mut id = str_of(&item, "id").to_string();
    if !slug_ok(&id) {
      id = Uuid::new_v4().to_string();
      item["id"] = json!(id);
    }
    match old_items.iter().find(|o| str_of(o, "id") == id) {
      Some(old) => {
        item["created_by"] = old.get("created_by").cloned().unwrap_or(json!(uid));
        item["created_ms"] = old.get("created_ms").cloned().unwrap_or(json!(now));
      },
      None => {
        item["created_by"] = json!(uid);
        item["created_ms"] = json!(now);
      },
    }
    item["updated_ms"] = json!(now);
    out.push(item);
  }
  Ok(out)
}

/// PUT /api/carehub/ws/{ws}/settings/{key}
async fn put_setting_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, String)>,
  body: Json<Value>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, key) = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  let pool = &state.pg_pool;
  let body = body.into_inner();
  if serde_json::to_string(&body).map(|s| s.len()).unwrap_or(0) > MAX_SETTING_BYTES {
    return Err(bad("That is too much to save at once").into());
  }
  let saved: Value = match key.as_str() {
    "roles" => {
      require(&me, "admin")?;
      let mut custom = Vec::new();
      if let Some(list) = body.get("custom").and_then(|v| v.as_array()) {
        for r in list.iter().take(30) {
          let id = str_of(r, "id");
          let name = str_of(r, "name").trim();
          if !slug_ok(id) || name.is_empty() || preset_roles().iter().any(|p| str_of(p, "id") == id) {
            continue;
          }
          custom.push(json!({
            "id": id,
            "name": clip(name, 40),
            "perms": clean_perms(r.get("perms")),
            "description": clip(str_of(r, "description"), 200),
          }));
        }
      }
      let draft = json!({ "custom": custom });
      let list = roles_list(&draft);
      let known = |id: &str| list.iter().any(|r| str_of(r, "id") == id);
      let default = str_of(&body, "default");
      let default = if known(default) { default.to_string() } else { "editor".to_string() };
      let mut assign = serde_json::Map::new();
      if let Some(map) = body.get("assign").and_then(|v| v.as_object()) {
        for (email, role) in map.iter().take(2000) {
          let email = email.trim().to_lowercase();
          if let Some(role) = role.as_str() {
            if !email.is_empty() && known(role) {
              assign.insert(email, json!(role));
            }
          }
        }
      }
      let value = json!({ "default": default, "assign": assign, "custom": custom });
      // Nobody can lock themselves out of the admin settings.
      let mine = resolve_role_id(&value, &me.email, me.af_role);
      let still_admin = role_info(&list, &mine)
        .map(|(_, _, perms)| perms.iter().any(|p| p == "admin"))
        .unwrap_or(false);
      if !still_admin {
        return Err(bad("You can't remove your own admin access. Ask another admin to change your role.").into());
      }
      value
    },
    "retention" => {
      require(&me, "admin")?;
      let (versions_days, analytics_days, trash_days) = retention_of(&body);
      json!({ "versions_days": versions_days, "analytics_days": analytics_days, "trash_days": trash_days })
    },
    "trash_seen" => {
      require(&me, "admin")?;
      let mut map = serde_json::Map::new();
      if let Some(obj) = body.as_object() {
        for (k, v) in obj.iter().take(5000) {
          if let Some(ms) = v.as_i64() {
            map.insert(clip(k, 64), json!(ms));
          }
        }
      }
      Value::Object(map)
    },
    "automations" => {
      require(&me, "automations")?;
      let old = get_setting(pool, ws, "automations").await?;
      let old_rules = old.get("rules").and_then(|v| v.as_array()).cloned().unwrap_or_default();
      let new_rules = body.get("rules").and_then(|v| v.as_array()).cloned().unwrap_or_default();
      let rules = stamp_items(&new_rules, &old_rules, me.uid, 100)?;
      json!({ "rules": rules })
    },
    "templates" => {
      require(&me, "templates")?;
      let old = get_setting(pool, ws, "templates").await?;
      let old_items = old.get("items").and_then(|v| v.as_array()).cloned().unwrap_or_default();
      let new_items = body.get("items").and_then(|v| v.as_array()).cloned().unwrap_or_default();
      let items = stamp_items(&new_items, &old_items, me.uid, 200)?;
      json!({ "items": items })
    },
    _ => return Err(bad("Unknown setting").into()),
  };
  put_setting(pool, ws, &key, &saved).await?;
  ok(saved)
}

/// GET /api/carehub/ws/{ws}/members – everyone with their Care Hub role (admins only).
async fn members_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let ws = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  require(&me, "admin")?;
  let cfg = get_setting(&state.pg_pool, ws, "roles").await?;
  let list = roles_list(&cfg);
  let rows: Vec<(i32, Uuid, String, String, String)> = sqlx::query_as(
    "SELECT m.role_id::INT, u.uuid, COALESCE(u.name, ''), LOWER(COALESCE(u.email, '')), \
     COALESCE(u.metadata ->> 'icon_url', '') FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
     WHERE m.workspace_id = $1 AND COALESCE(u.email, '') NOT LIKE '%.removed-%' ORDER BY u.name, u.email",
  )
  .bind(ws)
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  let owner: i32 = AFRole::Owner.into();
  let guest: i32 = AFRole::Guest.into();
  let people: Vec<Value> = rows
    .iter()
    .map(|(af_role, uuid, name, email, avatar)| {
      let wanted = resolve_role_id(&cfg, email.trim(), *af_role);
      let (role_id, role_name, _) = role_info(&list, &wanted)
        .or_else(|| role_info(&list, "editor"))
        .unwrap_or_default();
      json!({
        "uuid": uuid, "name": name, "email": email, "avatar": avatar,
        "workspace_role": if *af_role == owner { "owner" } else if *af_role == guest { "guest" } else { "member" },
        "role": { "id": role_id, "name": role_name },
        "assigned": cfg.get("assign").and_then(|a| a.get(email.as_str())).is_some(),
        "env_admin": admin_emails().iter().any(|e| e == email),
      })
    })
    .collect();
  ok(json!({ "members": people, "roles": list }))
}

// ---------------------------------------------------------------------------
// analytics
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ViewBody {
  page_id: Uuid,
  #[serde(default)]
  title: String,
  #[serde(default)]
  kind: String,
}

/// POST /api/carehub/ws/{ws}/analytics/view – someone opened (or edited) a page.
async fn record_view_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
  body: Json<ViewBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let ws = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  let pool = &state.pg_pool;
  let kind = if body.kind == "edit" { "edit" } else { "view" };
  let now = now_ms();
  let title = clip(body.title.trim(), 200);
  let recent: Option<i64> = sqlx::query_scalar(
    "SELECT id FROM carehub_page_view WHERE page_id = $1 AND uid = $2 AND kind = $3 AND ts_ms > $4 \
     ORDER BY ts_ms DESC LIMIT 1",
  )
  .bind(body.page_id)
  .bind(me.uid)
  .bind(kind)
  .bind(now - VIEW_DEDUPE_MS)
  .fetch_optional(pool)
  .await
  .map_err(AppError::from)?;
  match recent {
    Some(id) => {
      if !title.is_empty() {
        sqlx::query("UPDATE carehub_page_view SET title = $2 WHERE id = $1")
          .bind(id)
          .bind(&title)
          .execute(pool)
          .await
          .map_err(AppError::from)?;
      }
      ok(json!({ "recorded": false }))
    },
    None => {
      sqlx::query(
        "INSERT INTO carehub_page_view (workspace_id, page_id, uid, kind, title, ts_ms) \
         VALUES ($1, $2, $3, $4, $5, $6)",
      )
      .bind(ws)
      .bind(body.page_id)
      .bind(me.uid)
      .bind(kind)
      .bind(&title)
      .bind(now)
      .execute(pool)
      .await
      .map_err(AppError::from)?;
      ok(json!({ "recorded": true }))
    },
  }
}

#[derive(Debug, Deserialize)]
struct RangeQuery {
  days: Option<i64>,
  /// Minutes east of UTC (the browser's time zone).
  tz: Option<i64>,
}

async fn titles_for(pool: &PgPool, ws: Uuid, ids: &[Uuid]) -> Result<Vec<(Uuid, String)>, AppError> {
  let rows: Vec<(Uuid, String)> = sqlx::query_as(
    "SELECT DISTINCT ON (page_id) page_id, title FROM carehub_page_view \
     WHERE workspace_id = $1 AND page_id = ANY($2) AND title <> '' ORDER BY page_id, ts_ms DESC",
  )
  .bind(ws)
  .bind(ids.to_vec())
  .fetch_all(pool)
  .await?;
  Ok(rows)
}

async fn latest_title(pool: &PgPool, page: Uuid) -> String {
  sqlx::query_scalar::<_, String>(
    "SELECT title FROM carehub_page_view WHERE page_id = $1 AND title <> '' ORDER BY ts_ms DESC LIMIT 1",
  )
  .bind(page)
  .fetch_optional(pool)
  .await
  .ok()
  .flatten()
  .unwrap_or_default()
}

fn person(uuid: &Uuid, name: &str, email: &str, avatar: &str) -> Value {
  json!({ "uuid": uuid, "name": name, "email": email, "avatar": avatar })
}

/// GET /api/carehub/ws/{ws}/analytics?days=30&tz=-360
async fn analytics_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
  query: web::Query<RangeQuery>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let ws = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  require(&me, "analytics")?;
  let pool = &state.pg_pool;
  let days = query.days.unwrap_or(30).clamp(1, 3650);
  let tz_ms = query.tz.unwrap_or(0).clamp(-14 * 60, 14 * 60) * 60_000;
  let since = now_ms() - days * DAY_MS;

  let totals: (i64, i64, i64, i64, i64) = sqlx::query_as(
    "SELECT COUNT(*) FILTER (WHERE kind = 'view'), COUNT(*) FILTER (WHERE kind = 'edit'), \
     COUNT(DISTINCT uid), COUNT(DISTINCT page_id), COUNT(DISTINCT uid) FILTER (WHERE kind = 'edit') \
     FROM carehub_page_view WHERE workspace_id = $1 AND ts_ms >= $2",
  )
  .bind(ws)
  .bind(since)
  .fetch_one(pool)
  .await
  .map_err(AppError::from)?;

  let daily: Vec<(i64, i64, i64, i64)> = sqlx::query_as(
    "SELECT (ts_ms + $3) / 86400000 AS d, COUNT(*) FILTER (WHERE kind = 'view'), \
     COUNT(*) FILTER (WHERE kind = 'edit'), COUNT(DISTINCT uid) \
     FROM carehub_page_view WHERE workspace_id = $1 AND ts_ms >= $2 GROUP BY d ORDER BY d",
  )
  .bind(ws)
  .bind(since)
  .bind(tz_ms)
  .fetch_all(pool)
  .await
  .map_err(AppError::from)?;

  let hours: Vec<(i64, i64)> = sqlx::query_as(
    "SELECT ((ts_ms + $3) % 86400000) / 3600000 AS h, COUNT(*) FROM carehub_page_view \
     WHERE workspace_id = $1 AND ts_ms >= $2 GROUP BY h ORDER BY h",
  )
  .bind(ws)
  .bind(since)
  .bind(tz_ms)
  .fetch_all(pool)
  .await
  .map_err(AppError::from)?;

  let weekdays: Vec<(i64, i64)> = sqlx::query_as(
    "SELECT (((ts_ms + $3) / 86400000) + 4) % 7 AS w, COUNT(*) FROM carehub_page_view \
     WHERE workspace_id = $1 AND ts_ms >= $2 GROUP BY w ORDER BY w",
  )
  .bind(ws)
  .bind(since)
  .bind(tz_ms)
  .fetch_all(pool)
  .await
  .map_err(AppError::from)?;

  let pages: Vec<(Uuid, i64, i64, i64, i64)> = sqlx::query_as(
    "SELECT page_id, COUNT(*) FILTER (WHERE kind = 'view') AS v, COUNT(*) FILTER (WHERE kind = 'edit'), \
     COUNT(DISTINCT uid), MAX(ts_ms) FROM carehub_page_view WHERE workspace_id = $1 AND ts_ms >= $2 \
     GROUP BY page_id ORDER BY v DESC, MAX(ts_ms) DESC LIMIT 25",
  )
  .bind(ws)
  .bind(since)
  .fetch_all(pool)
  .await
  .map_err(AppError::from)?;
  let ids: Vec<Uuid> = pages.iter().map(|p| p.0).collect();
  let titles = titles_for(pool, ws, &ids).await?;

  let people: Vec<(Uuid, String, String, String, i64, i64, i64, i64)> = sqlx::query_as(
    "SELECT u.uuid, COALESCE(u.name, ''), COALESCE(u.email, ''), COALESCE(u.metadata ->> 'icon_url', ''), \
     COUNT(*) FILTER (WHERE p.kind = 'view') AS v, COUNT(*) FILTER (WHERE p.kind = 'edit'), \
     COUNT(DISTINCT p.page_id), MAX(p.ts_ms) FROM carehub_page_view p JOIN af_user u ON u.uid = p.uid \
     WHERE p.workspace_id = $1 AND p.ts_ms >= $2 GROUP BY 1, 2, 3, 4 \
     ORDER BY v DESC LIMIT 30",
  )
  .bind(ws)
  .bind(since)
  .fetch_all(pool)
  .await
  .map_err(AppError::from)?;

  let versions: i64 = sqlx::query_scalar(
    "SELECT COUNT(*) FROM carehub_page_version WHERE workspace_id = $1 AND created_ms >= $2",
  )
  .bind(ws)
  .bind(since)
  .fetch_one(pool)
  .await
  .map_err(AppError::from)?;

  let tracking_since: Option<i64> =
    sqlx::query_scalar("SELECT MIN(ts_ms) FROM carehub_page_view WHERE workspace_id = $1")
      .bind(ws)
      .fetch_one(pool)
      .await
      .map_err(AppError::from)?;

  ok(json!({
    "days": days,
    "tracking_since": tracking_since,
    "totals": {
      "views": totals.0, "edits": totals.1, "people": totals.2, "pages": totals.3,
      "editors": totals.4, "versions": versions,
    },
    "daily": daily.iter().map(|d| json!({ "day": d.0, "views": d.1, "edits": d.2, "people": d.3 })).collect::<Vec<_>>(),
    "hours": hours.iter().map(|h| json!({ "hour": h.0, "count": h.1 })).collect::<Vec<_>>(),
    "weekdays": weekdays.iter().map(|w| json!({ "weekday": w.0, "count": w.1 })).collect::<Vec<_>>(),
    "pages": pages.iter().map(|p| json!({
      "page_id": p.0,
      "title": titles.iter().find(|t| t.0 == p.0).map(|t| t.1.clone()).unwrap_or_default(),
      "views": p.1, "edits": p.2, "people": p.3, "last_at": p.4,
    })).collect::<Vec<_>>(),
    "people": people.iter().map(|p| json!({
      "person": person(&p.0, &p.1, &p.2, &p.3),
      "views": p.4, "edits": p.5, "pages": p.6, "last_at": p.7,
    })).collect::<Vec<_>>(),
  }))
}

/// GET /api/carehub/ws/{ws}/analytics/page/{page}
async fn page_analytics_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, Uuid)>,
  query: web::Query<RangeQuery>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, page) = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  require(&me, "analytics")?;
  let pool = &state.pg_pool;
  let days = query.days.unwrap_or(30).clamp(1, 3650);
  let tz_ms = query.tz.unwrap_or(0).clamp(-14 * 60, 14 * 60) * 60_000;
  let since = now_ms() - days * DAY_MS;

  let totals: (i64, i64, i64, Option<i64>, Option<i64>) = sqlx::query_as(
    "SELECT COUNT(*) FILTER (WHERE kind = 'view'), COUNT(*) FILTER (WHERE kind = 'edit'), \
     COUNT(DISTINCT uid), MAX(ts_ms) FILTER (WHERE kind = 'view'), MAX(ts_ms) FILTER (WHERE kind = 'edit') \
     FROM carehub_page_view WHERE workspace_id = $1 AND page_id = $2 AND ts_ms >= $3",
  )
  .bind(ws)
  .bind(page)
  .bind(since)
  .fetch_one(pool)
  .await
  .map_err(AppError::from)?;

  let daily: Vec<(i64, i64, i64)> = sqlx::query_as(
    "SELECT (ts_ms + $4) / 86400000 AS d, COUNT(*) FILTER (WHERE kind = 'view'), \
     COUNT(*) FILTER (WHERE kind = 'edit') FROM carehub_page_view \
     WHERE workspace_id = $1 AND page_id = $2 AND ts_ms >= $3 GROUP BY d ORDER BY d",
  )
  .bind(ws)
  .bind(page)
  .bind(since)
  .bind(tz_ms)
  .fetch_all(pool)
  .await
  .map_err(AppError::from)?;

  let people: Vec<(Uuid, String, String, String, i64, i64, i64)> = sqlx::query_as(
    "SELECT u.uuid, COALESCE(u.name, ''), COALESCE(u.email, ''), COALESCE(u.metadata ->> 'icon_url', ''), \
     COUNT(*) FILTER (WHERE p.kind = 'view') AS v, COUNT(*) FILTER (WHERE p.kind = 'edit'), MAX(p.ts_ms) \
     FROM carehub_page_view p JOIN af_user u ON u.uid = p.uid \
     WHERE p.workspace_id = $1 AND p.page_id = $2 AND p.ts_ms >= $3 \
     GROUP BY 1, 2, 3, 4 ORDER BY MAX(p.ts_ms) DESC LIMIT 50",
  )
  .bind(ws)
  .bind(page)
  .bind(since)
  .fetch_all(pool)
  .await
  .map_err(AppError::from)?;

  let versions: i64 =
    sqlx::query_scalar("SELECT COUNT(*) FROM carehub_page_version WHERE page_id = $1")
      .bind(page)
      .fetch_one(pool)
      .await
      .map_err(AppError::from)?;
  let first_seen: Option<i64> =
    sqlx::query_scalar("SELECT MIN(ts_ms) FROM carehub_page_view WHERE page_id = $1")
      .bind(page)
      .fetch_one(pool)
      .await
      .map_err(AppError::from)?;

  ok(json!({
    "days": days,
    "title": latest_title(pool, page).await,
    "first_seen": first_seen,
    "views": totals.0, "edits": totals.1, "people": totals.2,
    "last_viewed": totals.3, "last_edited": totals.4,
    "versions": versions,
    "daily": daily.iter().map(|d| json!({ "day": d.0, "views": d.1, "edits": d.2 })).collect::<Vec<_>>(),
    "visitors": people.iter().map(|p| json!({
      "person": person(&p.0, &p.1, &p.2, &p.3), "views": p.4, "edits": p.5, "last_at": p.6,
    })).collect::<Vec<_>>(),
  }))
}

// ---------------------------------------------------------------------------
// page versions
// ---------------------------------------------------------------------------

/// GET /api/carehub/ws/{ws}/pages/{page}/versions
async fn list_versions_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, Uuid)>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, page) = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  let rows: Vec<(Uuid, String, bool, i32, i64, i64, Uuid, String, String, String)> = sqlx::query_as(
    "SELECT v.id, v.name, v.auto, v.size, v.created_ms, v.uid, COALESCE(u.uuid, '00000000-0000-0000-0000-000000000000'::uuid), \
     COALESCE(u.name, ''), COALESCE(u.email, ''), COALESCE(u.metadata ->> 'icon_url', '') \
     FROM carehub_page_version v LEFT JOIN af_user u ON u.uid = v.uid \
     WHERE v.workspace_id = $1 AND v.page_id = $2 ORDER BY v.created_ms DESC LIMIT 400",
  )
  .bind(ws)
  .bind(page)
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  let retention = get_setting(&state.pg_pool, ws, "retention").await?;
  let (versions_days, _, _) = retention_of(&retention);
  ok(json!({
    "keep_days": versions_days,
    "can_restore": me.can("edit") || me.can("admin"),
    "versions": rows.iter().map(|r| json!({
      "id": r.0, "name": r.1, "auto": r.2, "size": r.3, "created_at": r.4,
      "mine": r.5 == me.uid,
      "by": person(&r.6, &r.7, &r.8, &r.9),
    })).collect::<Vec<_>>(),
  }))
}

#[derive(Debug, Deserialize)]
struct SaveVersionQuery {
  name: Option<String>,
  auto: Option<bool>,
}

/// POST /api/carehub/ws/{ws}/pages/{page}/versions?auto=true&name=…
/// The body is the page's content as a Yjs update (binary).
async fn save_version_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, Uuid)>,
  query: web::Query<SaveVersionQuery>,
  body: Bytes,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, page) = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  require(&me, "edit")?;
  let pool = &state.pg_pool;
  if body.is_empty() || body.len() > MAX_VERSION_BYTES {
    return Err(bad("This page is too large to save as a version").into());
  }
  let auto = query.auto.unwrap_or(false);
  let name = clip(query.name.as_deref().unwrap_or("").trim(), 120);
  let now = now_ms();
  let latest: Option<(i64, Vec<u8>)> = sqlx::query_as(
    "SELECT created_ms, state FROM carehub_page_version WHERE page_id = $1 ORDER BY created_ms DESC LIMIT 1",
  )
  .bind(page)
  .fetch_optional(pool)
  .await
  .map_err(AppError::from)?;
  if let Some((created, state_bytes)) = &latest {
    if auto && (now - created < AUTO_VERSION_GAP_MS || state_bytes.as_slice() == &body[..]) {
      return ok(json!({ "saved": false }));
    }
  }
  let id = Uuid::new_v4();
  sqlx::query(
    "INSERT INTO carehub_page_version (id, workspace_id, page_id, uid, name, auto, size, state, created_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
  )
  .bind(id)
  .bind(ws)
  .bind(page)
  .bind(me.uid)
  .bind(&name)
  .bind(auto)
  .bind(body.len() as i32)
  .bind(body.to_vec())
  .bind(now)
  .execute(pool)
  .await
  .map_err(AppError::from)?;
  ok(json!({ "saved": true, "id": id, "created_at": now }))
}

async fn version_row(pool: &PgPool, ws: Uuid, id: Uuid) -> Result<(Uuid, i64, Vec<u8>), AppError> {
  let row: Option<(Uuid, i64, Vec<u8>)> = sqlx::query_as(
    "SELECT page_id, uid, state FROM carehub_page_version WHERE id = $1 AND workspace_id = $2",
  )
  .bind(id)
  .bind(ws)
  .fetch_optional(pool)
  .await?;
  row.ok_or_else(|| AppError::RecordNotFound("Version not found".to_string()))
}

/// GET /api/carehub/ws/{ws}/versions/{id} → { page_id, state_b64 }
async fn get_version_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, Uuid)>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, id) = path.into_inner();
  me_for(&state, &user_uuid, ws).await?;
  let (page, _, bytes) = version_row(&state.pg_pool, ws, id).await?;
  ok(json!({
    "id": id,
    "page_id": page,
    "state_b64": base64::engine::general_purpose::STANDARD.encode(&bytes),
  }))
}

#[derive(Debug, Deserialize)]
struct RenameBody {
  name: String,
}

/// PUT /api/carehub/ws/{ws}/versions/{id} {name} – names (and keeps) a version.
async fn rename_version_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, Uuid)>,
  body: Json<RenameBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, id) = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  require(&me, "edit")?;
  version_row(&state.pg_pool, ws, id).await?;
  let name = clip(body.name.trim(), 120);
  sqlx::query("UPDATE carehub_page_version SET name = $2, auto = ($2 = '') WHERE id = $1")
    .bind(id)
    .bind(&name)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  ok(json!({ "id": id, "name": name, "auto": name.is_empty() }))
}

/// DELETE /api/carehub/ws/{ws}/versions/{id} – whoever saved it, or an admin.
async fn delete_version_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, Uuid)>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, id) = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  let (_, owner_uid, _) = version_row(&state.pg_pool, ws, id).await?;
  if owner_uid != me.uid && !me.can("admin") {
    return Err(AppError::NotEnoughPermissions.into());
  }
  sqlx::query("DELETE FROM carehub_page_version WHERE id = $1")
    .bind(id)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  ok(json!({ "deleted": true }))
}

// ---------------------------------------------------------------------------
// reminders
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ReminderBody {
  title: String,
  #[serde(default)]
  note: String,
  remind_at: i64,
  page_id: Option<Uuid>,
}

type ReminderRow = (Uuid, Option<Uuid>, String, String, i64, i64, i64);

fn reminder_json(r: &ReminderRow) -> Value {
  json!({
    "id": r.0, "page_id": r.1, "title": r.2, "note": r.3,
    "remind_at": r.4, "created_at": r.5, "sent_at": if r.6 > 0 { json!(r.6) } else { Value::Null },
  })
}

/// GET /api/carehub/ws/{ws}/reminders – my upcoming reminders and the last few sent.
async fn list_reminders_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let ws = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  let upcoming: Vec<ReminderRow> = sqlx::query_as(
    "SELECT id, page_id, title, note, remind_ms, created_ms, sent_ms FROM carehub_reminder \
     WHERE workspace_id = $1 AND uid = $2 AND sent_ms = 0 ORDER BY remind_ms LIMIT 500",
  )
  .bind(ws)
  .bind(me.uid)
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  let sent: Vec<ReminderRow> = sqlx::query_as(
    "SELECT id, page_id, title, note, remind_ms, created_ms, sent_ms FROM carehub_reminder \
     WHERE workspace_id = $1 AND uid = $2 AND sent_ms > 0 ORDER BY remind_ms DESC LIMIT 20",
  )
  .bind(ws)
  .bind(me.uid)
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  ok(json!({
    "upcoming": upcoming.iter().map(reminder_json).collect::<Vec<_>>(),
    "sent": sent.iter().map(reminder_json).collect::<Vec<_>>(),
  }))
}

/// POST /api/carehub/ws/{ws}/reminders {title, note, remind_at, page_id}
async fn create_reminder_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
  body: Json<ReminderBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let ws = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  let now = now_ms();
  let title = clip(body.title.trim(), 200);
  if title.is_empty() {
    return Err(bad("Give the reminder a title").into());
  }
  if body.remind_at < now - 60_000 {
    return Err(bad("Pick a time in the future").into());
  }
  let pending: i64 = sqlx::query_scalar(
    "SELECT COUNT(*) FROM carehub_reminder WHERE uid = $1 AND sent_ms = 0",
  )
  .bind(me.uid)
  .fetch_one(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  if pending >= 500 {
    return Err(bad("You already have 500 reminders waiting").into());
  }
  let id = Uuid::new_v4();
  let note = clip(body.note.trim(), 2000);
  sqlx::query(
    "INSERT INTO carehub_reminder (id, workspace_id, uid, page_id, title, note, remind_ms, created_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
  )
  .bind(id)
  .bind(ws)
  .bind(me.uid)
  .bind(body.page_id)
  .bind(&title)
  .bind(&note)
  .bind(body.remind_at)
  .bind(now)
  .execute(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  ok(reminder_json(&(id, body.page_id, title, note, body.remind_at, now, 0)))
}

/// DELETE /api/carehub/ws/{ws}/reminders/{id}
async fn delete_reminder_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, Uuid)>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, id) = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  sqlx::query("DELETE FROM carehub_reminder WHERE id = $1 AND uid = $2 AND workspace_id = $3")
    .bind(id)
    .bind(me.uid)
    .bind(ws)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  ok(json!({ "deleted": true }))
}

// ---------------------------------------------------------------------------
// messages from Care Hub (reminders, automations, alerts)
// ---------------------------------------------------------------------------

async fn new_system_chat(pool: &PgPool, name: &str, members: &[i64]) -> Result<Uuid, AppError> {
  super::carehub_messages::ensure_tables(pool).await?;
  let id = Uuid::new_v4();
  let now = now_ms();
  sqlx::query(
    "INSERT INTO carehub_chat (id, name, is_group, created_by, created_ms, last_ms) VALUES ($1, $2, TRUE, 0, $3, $3)",
  )
  .bind(id)
  .bind(name)
  .bind(now)
  .execute(pool)
  .await?;
  add_chat_members(pool, id, members).await?;
  Ok(id)
}

async fn add_chat_members(pool: &PgPool, chat: Uuid, members: &[i64]) -> Result<(), AppError> {
  let now = now_ms();
  for uid in members {
    sqlx::query(
      "INSERT INTO carehub_chat_member (chat_id, uid, read_ms, joined_ms) VALUES ($1, $2, $3, $3) \
       ON CONFLICT (chat_id, uid) DO NOTHING",
    )
    .bind(chat)
    .bind(*uid)
    .bind(now)
    .execute(pool)
    .await?;
  }
  Ok(())
}

/// Posts a message from "Care Hub" (sender 0), so it counts as unread for everyone.
async fn system_post(pool: &PgPool, chat: Uuid, body: &str) -> Result<(), AppError> {
  let last: i64 = sqlx::query_scalar("SELECT last_ms FROM carehub_chat WHERE id = $1")
    .bind(chat)
    .fetch_one(pool)
    .await?;
  let now = now_ms().max(last + 1);
  sqlx::query(
    "INSERT INTO carehub_chat_message (id, chat_id, sender, body, created_ms) VALUES ($1, $2, 0, $3, $4)",
  )
  .bind(Uuid::new_v4())
  .bind(chat)
  .bind(clip(body, 8000))
  .bind(now)
  .execute(pool)
  .await?;
  sqlx::query("UPDATE carehub_chat SET last_ms = GREATEST(last_ms, $2) WHERE id = $1")
    .bind(chat)
    .bind(now)
    .execute(pool)
    .await?;
  Ok(())
}

/// The person's own "🔔 Care Hub" chat, created the first time it is needed.
async fn notify_user(pool: &PgPool, uid: i64, body: &str) -> Result<(), AppError> {
  super::carehub_messages::ensure_tables(pool).await?;
  let found: Option<Uuid> = sqlx::query_scalar(
    "SELECT c.id FROM carehub_chat c JOIN carehub_chat_member m ON m.chat_id = c.id AND m.uid = $1 \
     WHERE c.created_by = 0 AND c.name = $2 ORDER BY c.created_ms LIMIT 1",
  )
  .bind(uid)
  .bind(SYSTEM_CHAT_NAME)
  .fetch_optional(pool)
  .await?;
  let chat = match found {
    Some(id) => id,
    None => new_system_chat(pool, SYSTEM_CHAT_NAME, &[uid]).await?,
  };
  system_post(pool, chat, body).await
}

async fn admin_uids(pool: &PgPool) -> Vec<i64> {
  let emails = admin_emails();
  if emails.is_empty() {
    return Vec::new();
  }
  sqlx::query_scalar("SELECT uid FROM af_user WHERE LOWER(email) = ANY($1)")
    .bind(emails)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
}

async fn notify_admins(pool: &PgPool, text: &str) {
  for uid in admin_uids(pool).await {
    if let Err(err) = notify_user(pool, uid, text).await {
      tracing::warn!("CareHub: could not message an admin: {}", err);
    }
  }
}

// ---------------------------------------------------------------------------
// automations
// ---------------------------------------------------------------------------

struct RunCtx {
  title: String,
  page: Option<Uuid>,
  person: String,
}

fn local_now(tz_min: i64) -> chrono::NaiveDateTime {
  chrono::Utc::now().naive_utc() + ChronoDuration::minutes(tz_min)
}

fn rule_tz(rule: &Value) -> i64 {
  rule.get("tz").and_then(|v| v.as_i64()).unwrap_or(0).clamp(-840, 840)
}

/// The latest time (UTC ms) a schedule should have run, if it was in the last 3 hours.
fn due_occurrence(trigger: &Value, tz: i64) -> Option<i64> {
  use chrono::TimeZone;
  let time = str_of(trigger, "time");
  let mut parts = time.split(':');
  let h: u32 = parts.next()?.trim().parse().ok()?;
  let m: u32 = parts.next().unwrap_or("0").trim().parse().ok()?;
  let t = NaiveTime::from_hms_opt(h, m, 0)?;
  let now = now_ms();
  let local = local_now(tz);
  let every = str_of(trigger, "every");
  let weekday = trigger.get("weekday").and_then(|v| v.as_i64()).unwrap_or(1);
  let day = trigger.get("day").and_then(|v| v.as_i64()).unwrap_or(1);
  for back in 0..2i64 {
    let date = local.date() - ChronoDuration::days(back);
    let wd = date.weekday().num_days_from_sunday() as i64;
    let fits = match every {
      "weekday" => (1..=5).contains(&wd),
      "week" => wd == weekday,
      "month" => date.day() as i64 == day,
      _ => true,
    };
    if !fits {
      continue;
    }
    let utc = date.and_time(t) - ChronoDuration::minutes(tz);
    let occ = chrono::Utc.from_utc_datetime(&utc).timestamp_millis();
    if occ <= now && now - occ < 3 * 3_600_000 {
      return Some(occ);
    }
  }
  None
}

async fn run_rule(pool: &PgPool, ws: Uuid, rule: &Value, ctx: &RunCtx) -> Result<(), AppError> {
  let creator = rule
    .get("created_by")
    .and_then(|v| v.as_i64())
    .ok_or_else(|| bad("This automation has no owner"))?;
  if me_in(pool, ws, creator).await.is_err() {
    return Err(bad("The person who made this automation is no longer in the workspace"));
  }
  super::carehub_messages::ensure_tables(pool).await?;
  let action = rule.get("action").cloned().unwrap_or(Value::Null);
  let tz = rule_tz(rule);
  let name = clip(str_of(rule, "name").trim(), 80);
  let name = if name.is_empty() { "Automation".to_string() } else { name };
  let mut text = clip(str_of(&action, "text"), 4000);
  if text.trim().is_empty() {
    text = format!("⚡ {}", name);
  }
  let title = if ctx.title.trim().is_empty() { "Untitled".to_string() } else { ctx.title.clone() };
  let who = if ctx.person.trim().is_empty() { "Someone".to_string() } else { ctx.person.clone() };
  text = text
    .replace("{date}", &local_now(tz).format("%A, %B %-d, %Y").to_string())
    .replace("{title}", &title)
    .replace("{person}", &who);
  match ctx.page {
    Some(page) => {
      let card = page_card(ws, page, &title);
      if text.contains("{page}") {
        text = text.replace("{page}", &card);
      } else {
        text = format!("{}\n{}", text, card);
      }
    },
    None => text = text.replace("{page}", ""),
  }
  if let Some(linked) = action
    .get("page_id")
    .and_then(|v| v.as_str())
    .and_then(|s| Uuid::parse_str(s).ok())
  {
    let mut linked_title = str_of(&action, "page_title").to_string();
    if linked_title.is_empty() {
      linked_title = latest_title(pool, linked).await;
    }
    text = format!("{}\n{}", text, page_card(ws, linked, &linked_title));
  }
  let rule_id = str_of(rule, "id").to_string();
  let chat = if str_of(&action, "to") == "chat" {
    let chat = action
      .get("chat_id")
      .and_then(|v| v.as_str())
      .and_then(|s| Uuid::parse_str(s).ok())
      .ok_or_else(|| bad("Pick a chat for this automation"))?;
    let member: Option<i64> =
      sqlx::query_scalar("SELECT uid FROM carehub_chat_member WHERE chat_id = $1 AND uid = $2")
        .bind(chat)
        .bind(creator)
        .fetch_optional(pool)
        .await?;
    if member.is_none() {
      return Err(bad("The automation's owner is no longer in that chat"));
    }
    chat
  } else {
    let mut uids = vec![creator];
    let ids: Vec<Uuid> = action
      .get("people")
      .and_then(|v| v.as_array())
      .map(|list| {
        list
          .iter()
          .filter_map(|v| v.as_str())
          .filter_map(|s| Uuid::parse_str(s).ok())
          .take(50)
          .collect()
      })
      .unwrap_or_default();
    if !ids.is_empty() {
      let found: Vec<i64> = sqlx::query_scalar(
        "SELECT u.uid FROM af_user u JOIN af_workspace_member m ON m.uid = u.uid AND m.workspace_id = $1 \
         WHERE u.uuid = ANY($2)",
      )
      .bind(ws)
      .bind(ids)
      .fetch_all(pool)
      .await?;
      for uid in found {
        if !uids.contains(&uid) {
          uids.push(uid);
        }
      }
    }
    let existing: Option<Uuid> = sqlx::query_scalar(
      "SELECT s.chat_id FROM carehub_automation_state s JOIN carehub_chat c ON c.id = s.chat_id \
       WHERE s.workspace_id = $1 AND s.rule_id = $2",
    )
    .bind(ws)
    .bind(&rule_id)
    .fetch_optional(pool)
    .await?;
    match existing {
      Some(chat) => {
        add_chat_members(pool, chat, &uids).await?;
        chat
      },
      None => {
        let chat = new_system_chat(pool, &format!("⚡ {}", name), &uids).await?;
        sqlx::query(
          "INSERT INTO carehub_automation_state (workspace_id, rule_id, chat_id) VALUES ($1, $2, $3) \
           ON CONFLICT (workspace_id, rule_id) DO UPDATE SET chat_id = EXCLUDED.chat_id",
        )
        .bind(ws)
        .bind(&rule_id)
        .bind(chat)
        .execute(pool)
        .await?;
        chat
      },
    }
  };
  system_post(pool, chat, &text).await
}

async fn record_run(pool: &PgPool, ws: Uuid, rule_id: &str, result: &Result<(), AppError>) {
  let error = match result {
    Ok(_) => String::new(),
    Err(err) => clip(&err.to_string(), 300),
  };
  let _ = sqlx::query(
    "INSERT INTO carehub_automation_state (workspace_id, rule_id, last_run_ms, runs, last_error) \
     VALUES ($1, $2, $3, 1, $4) ON CONFLICT (workspace_id, rule_id) DO UPDATE SET \
     runs = carehub_automation_state.runs + 1, last_error = EXCLUDED.last_error, \
     last_run_ms = GREATEST(carehub_automation_state.last_run_ms, EXCLUDED.last_run_ms)",
  )
  .bind(ws)
  .bind(rule_id)
  .bind(now_ms())
  .bind(error)
  .execute(pool)
  .await;
}

fn rules_of(cfg: &Value) -> Vec<Value> {
  cfg
    .get("rules")
    .and_then(|v| v.as_array())
    .cloned()
    .unwrap_or_default()
}

fn enabled(rule: &Value) -> bool {
  rule.get("enabled").and_then(|v| v.as_bool()) != Some(false)
}

async fn run_schedules(pool: &PgPool) -> Result<(), AppError> {
  let rows: Vec<(Uuid, String)> =
    sqlx::query_as("SELECT workspace_id, value FROM carehub_setting WHERE key = 'automations'")
      .fetch_all(pool)
      .await?;
  for (ws, raw) in rows {
    let cfg: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
    for rule in rules_of(&cfg) {
      let trigger = rule.get("trigger").cloned().unwrap_or(Value::Null);
      if !enabled(&rule) || str_of(&trigger, "type") != "schedule" {
        continue;
      }
      let occ = match due_occurrence(&trigger, rule_tz(&rule)) {
        Some(occ) => occ,
        None => continue,
      };
      if occ <= rule.get("created_ms").and_then(|v| v.as_i64()).unwrap_or(0) {
        continue;
      }
      let rule_id = str_of(&rule, "id").to_string();
      let claimed = sqlx::query(
        "INSERT INTO carehub_automation_state (workspace_id, rule_id, last_run_ms) VALUES ($1, $2, $3) \
         ON CONFLICT (workspace_id, rule_id) DO UPDATE SET last_run_ms = EXCLUDED.last_run_ms \
         WHERE carehub_automation_state.last_run_ms < EXCLUDED.last_run_ms",
      )
      .bind(ws)
      .bind(&rule_id)
      .bind(occ)
      .execute(pool)
      .await?
      .rows_affected()
        > 0;
      if !claimed {
        continue;
      }
      let ctx = RunCtx {
        title: String::new(),
        page: None,
        person: String::new(),
      };
      let result = run_rule(pool, ws, &rule, &ctx).await;
      record_run(pool, ws, &rule_id, &result).await;
    }
  }
  Ok(())
}

async fn fire_event(pool: &PgPool, ws: Uuid, kind: &str, subject: Uuid, actor: i64, ctx: RunCtx) {
  if ensure_tables(pool).await.is_err() {
    return;
  }
  let cfg = match get_setting(pool, ws, "automations").await {
    Ok(cfg) => cfg,
    Err(_) => return,
  };
  for rule in rules_of(&cfg) {
    let trigger = rule.get("trigger").cloned().unwrap_or(Value::Null);
    if !enabled(&rule) || str_of(&trigger, "type") != kind {
      continue;
    }
    let only = str_of(&trigger, "page_id");
    if !only.is_empty() && Uuid::parse_str(only).ok() != Some(subject) {
      continue;
    }
    let creator = rule.get("created_by").and_then(|v| v.as_i64()).unwrap_or(0);
    let include_own = trigger.get("include_own").and_then(|v| v.as_bool()).unwrap_or(false);
    if kind == "page_created" && actor == creator && !include_own {
      continue;
    }
    let result = run_rule(pool, ws, &rule, &ctx).await;
    record_run(pool, ws, str_of(&rule, "id"), &result).await;
  }
}

async fn person_name(pool: &PgPool, uid: i64) -> String {
  sqlx::query_scalar::<_, String>(
    "SELECT COALESCE(NULLIF(name, ''), split_part(COALESCE(email, ''), '@', 1)) FROM af_user WHERE uid = $1",
  )
  .bind(uid)
  .fetch_optional(pool)
  .await
  .ok()
  .flatten()
  .unwrap_or_default()
}

/// Called after a form response was saved.
pub(crate) fn on_form_submitted(pool: PgPool, ws: Uuid, view_id: Uuid) {
  tokio::spawn(async move {
    let title = latest_title(&pool, view_id).await;
    let ctx = RunCtx {
      title: if title.is_empty() { "your form".to_string() } else { title },
      page: Some(view_id),
      person: String::new(),
    };
    fire_event(&pool, ws, "form_submitted", view_id, 0, ctx).await;
  });
}

/// Called after someone created a page. Waits two minutes so the page has its name.
pub(crate) fn on_page_created(pool: PgPool, ws: Uuid, uid: i64, page: Option<Uuid>, name: Option<String>) {
  let page = match page {
    Some(page) => page,
    None => return,
  };
  tokio::spawn(async move {
    tokio::time::sleep(std::time::Duration::from_secs(120)).await;
    let mut title = latest_title(&pool, page).await;
    if title.is_empty() {
      title = name.unwrap_or_default();
    }
    let ctx = RunCtx {
      title,
      page: Some(page),
      person: person_name(&pool, uid).await,
    };
    fire_event(&pool, ws, "page_created", page, uid, ctx).await;
  });
}

/// GET /api/carehub/ws/{ws}/automations/status
async fn automation_status_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let ws = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  require(&me, "automations")?;
  let rows: Vec<(String, i64, i64, String)> = sqlx::query_as(
    "SELECT rule_id, last_run_ms, runs, last_error FROM carehub_automation_state WHERE workspace_id = $1",
  )
  .bind(ws)
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  ok(json!(rows
    .iter()
    .map(|r| json!({ "rule_id": r.0, "last_run": r.1, "runs": r.2, "last_error": r.3 }))
    .collect::<Vec<_>>()))
}

/// POST /api/carehub/ws/{ws}/automations/{rule}/run – try a rule now.
async fn run_automation_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<(Uuid, String)>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (ws, rule_id) = path.into_inner();
  let me = me_for(&state, &user_uuid, ws).await?;
  require(&me, "automations")?;
  let pool = &state.pg_pool;
  let cfg = get_setting(pool, ws, "automations").await?;
  let rule = rules_of(&cfg)
    .into_iter()
    .find(|r| str_of(r, "id") == rule_id)
    .ok_or_else(|| AppError::RecordNotFound("Save the automation first".to_string()))?;
  let ctx = RunCtx {
    title: "Test run".to_string(),
    page: None,
    person: person_name(pool, me.uid).await,
  };
  let result = run_rule(pool, ws, &rule, &ctx).await;
  record_run(pool, ws, &rule_id, &result).await;
  result?;
  ok(json!({ "ran": true }))
}

// ---------------------------------------------------------------------------
// status page, backups and the background task
// ---------------------------------------------------------------------------

fn backup_dir() -> String {
  std::env::var("CAREHUB_BACKUP_DIR").unwrap_or_else(|_| "/carehub-backups".to_string())
}

fn backups_info() -> Value {
  let entries = match std::fs::read_dir(backup_dir()) {
    Ok(entries) => entries,
    Err(_) => return json!({ "available": false }),
  };
  let mut files: Vec<(String, u64, i64)> = Vec::new();
  for entry in entries.flatten() {
    let name = entry.file_name().to_string_lossy().to_string();
    if !name.starts_with("carehub-") || name.ends_with(".part") {
      continue;
    }
    if let Ok(meta) = entry.metadata() {
      let modified = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
      files.push((name, meta.len(), modified));
    }
  }
  files.sort_by(|a, b| b.2.cmp(&a.2));
  let total: u64 = files.iter().map(|f| f.1).sum();
  let latest = files
    .iter()
    .find(|f| f.0.starts_with("carehub-db-"))
    .map(|f| f.2);
  json!({
    "available": true,
    "keep_days": std::env::var("CAREHUB_BACKUP_KEEP_DAYS").ok().and_then(|v| v.trim().parse::<i64>().ok()),
    "count": files.len(),
    "total_bytes": total,
    "latest_ms": latest,
    "files": files.iter().take(12).map(|f| json!({ "name": f.0, "size": f.1, "modified": f.2 })).collect::<Vec<_>>(),
  })
}

/// GET /api/carehub/status?tz=-360 – public, so it also works when signing in doesn't.
async fn status_handler(
  state: Data<AppState>,
  query: web::Query<RangeQuery>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let pool = &state.pg_pool;
  let now = now_ms();
  let tz_ms = query.tz.unwrap_or(0).clamp(-14 * 60, 14 * 60) * 60_000;
  let db_ok = sqlx::query_scalar::<_, i32>("SELECT 1")
    .fetch_one(pool)
    .await
    .is_ok();
  let mut days: Vec<Value> = Vec::new();
  let mut incidents: Vec<Value> = Vec::new();
  let mut overall: Option<f64> = None;
  let mut tracking_since: Option<i64> = None;
  if db_ok && ensure_tables(pool).await.is_ok() {
    let today = (now + tz_ms).div_euclid(DAY_MS);
    let since = (today - 29) * DAY_MS - tz_ms;
    let beats: Vec<i64> =
      sqlx::query_scalar("SELECT ts_ms FROM carehub_heartbeat WHERE ts_ms >= $1 ORDER BY ts_ms")
        .bind(since - BUCKET_MS)
        .fetch_all(pool)
        .await
        .unwrap_or_default();
    let first: Option<i64> = sqlx::query_scalar("SELECT MIN(ts_ms) FROM carehub_heartbeat")
      .fetch_one(pool)
      .await
      .unwrap_or(None);
    tracking_since = first;
    let (mut present_sum, mut expected_sum) = (0i64, 0i64);
    for d in (today - 29)..=today {
      let start = d * DAY_MS - tz_ms;
      let end = start + DAY_MS;
      let value = match first {
        Some(first) => {
          let from = first.max(start);
          let to = end.min(now);
          if to > from {
            let expected = ((to - from + BUCKET_MS - 1) / BUCKET_MS).max(1);
            let present = beats.iter().filter(|b| **b >= start && **b < end).count() as i64;
            let present = present.min(expected);
            present_sum += present;
            expected_sum += expected;
            json!(present as f64 / expected as f64)
          } else {
            Value::Null
          }
        },
        None => Value::Null,
      };
      days.push(json!({ "day": d, "uptime": value }));
    }
    if expected_sum > 0 {
      overall = Some(present_sum as f64 / expected_sum as f64);
    }
    for pair in beats.windows(2) {
      if pair[1] - pair[0] > 2 * BUCKET_MS {
        incidents.push(json!({ "start": pair[0] + BUCKET_MS, "end": pair[1] }));
      }
    }
    incidents.reverse();
    incidents.truncate(20);
  }
  ok(json!({
    "ok": db_ok,
    "database": db_ok,
    "now": now,
    "server_started": STARTED_MS.load(Ordering::Relaxed),
    "tracking_since": tracking_since,
    "uptime_30d": overall,
    "days": days,
    "incidents": incidents,
    "backups": backups_info(),
  }))
}

async fn check_downtime(pool: &PgPool) {
  let last: Option<i64> = match sqlx::query_scalar("SELECT MAX(ts_ms) FROM carehub_heartbeat")
    .fetch_one(pool)
    .await
  {
    Ok(last) => last,
    Err(_) => return,
  };
  let last = match last {
    Some(last) => last,
    None => return,
  };
  let gap = now_ms() - (last + BUCKET_MS);
  if gap < 15 * 60_000 {
    return;
  }
  notify_admins(
    pool,
    &format!(
      "⚠️ Care Hub was offline for about {} and is running again. This usually means the computer it runs on was asleep, restarting or without power or internet. Details: Settings → Data & backups, or the status page (/app/status).",
      duration_text(gap)
    ),
  )
  .await;
}

async fn check_backups(pool: &PgPool) {
  let info = backups_info();
  if info.get("available").and_then(|v| v.as_bool()) != Some(true) {
    return;
  }
  let now = now_ms();
  let stale = match info.get("latest_ms").and_then(|v| v.as_i64()) {
    Some(ms) => now - ms > 36 * 3_600_000,
    None => now - STARTED_MS.load(Ordering::Relaxed) > 36 * 3_600_000,
  };
  if !stale || now - LAST_BACKUP_ALERT_MS.load(Ordering::Relaxed) < DAY_MS {
    return;
  }
  LAST_BACKUP_ALERT_MS.store(now, Ordering::Relaxed);
  notify_admins(
    pool,
    "⚠️ Care Hub hasn't made a backup in more than a day and a half. Check that the backup service is running (Settings → Data & backups).",
  )
  .await;
}

async fn deliver_reminders(pool: &PgPool) -> Result<(), AppError> {
  let now = now_ms();
  let due: Vec<(Uuid, Uuid, i64, Option<Uuid>, String, String)> = sqlx::query_as(
    "SELECT id, workspace_id, uid, page_id, title, note FROM carehub_reminder \
     WHERE sent_ms = 0 AND remind_ms <= $1 ORDER BY remind_ms LIMIT 100",
  )
  .bind(now)
  .fetch_all(pool)
  .await?;
  for (id, ws, uid, page, title, note) in due {
    let claimed = sqlx::query("UPDATE carehub_reminder SET sent_ms = $2 WHERE id = $1 AND sent_ms = 0")
      .bind(id)
      .bind(now)
      .execute(pool)
      .await?
      .rows_affected()
      > 0;
    if !claimed {
      continue;
    }
    let mut text = format!("⏰ Reminder: {}", title);
    if !note.trim().is_empty() {
      text.push('\n');
      text.push_str(note.trim());
    }
    if let Some(page) = page {
      let page_title = latest_title(pool, page).await;
      let shown = if page_title.is_empty() { title.clone() } else { page_title };
      text = format!("{}\n{}", text, page_card(ws, page, &shown));
    }
    if let Err(err) = notify_user(pool, uid, &text).await {
      tracing::warn!("CareHub: could not deliver a reminder: {}", err);
    }
  }
  Ok(())
}

async fn apply_retention(pool: &PgPool) -> Result<(), AppError> {
  let now = now_ms();
  let workspaces: Vec<Uuid> = sqlx::query_scalar(
    "SELECT workspace_id FROM carehub_page_version UNION SELECT workspace_id FROM carehub_page_view",
  )
  .fetch_all(pool)
  .await?;
  for ws in workspaces {
    let (versions_days, analytics_days, _) = retention_of(&get_setting(pool, ws, "retention").await?);
    if versions_days > 0 {
      sqlx::query("DELETE FROM carehub_page_version WHERE workspace_id = $1 AND auto AND created_ms < $2")
        .bind(ws)
        .bind(now - versions_days * DAY_MS)
        .execute(pool)
        .await?;
    }
    if analytics_days > 0 {
      sqlx::query("DELETE FROM carehub_page_view WHERE workspace_id = $1 AND ts_ms < $2")
        .bind(ws)
        .bind(now - analytics_days * DAY_MS)
        .execute(pool)
        .await?;
    }
  }
  sqlx::query(
    "DELETE FROM carehub_page_version WHERE id IN (SELECT id FROM (SELECT id, ROW_NUMBER() OVER \
     (PARTITION BY page_id ORDER BY created_ms DESC) AS rn FROM carehub_page_version WHERE auto) t WHERE t.rn > $1)",
  )
  .bind(MAX_AUTO_VERSIONS_PER_PAGE)
  .execute(pool)
  .await?;
  sqlx::query("DELETE FROM carehub_reminder WHERE sent_ms > 0 AND sent_ms < $1")
    .bind(now - 30 * DAY_MS)
    .execute(pool)
    .await?;
  sqlx::query("DELETE FROM carehub_heartbeat WHERE ts_ms < $1")
    .bind(now - 90 * DAY_MS)
    .execute(pool)
    .await?;
  Ok(())
}

async fn background_tick(pool: &PgPool, tick: u64) -> Result<(), AppError> {
  ensure_tables(pool).await?;
  sqlx::query("INSERT INTO carehub_heartbeat (ts_ms) VALUES ($1) ON CONFLICT DO NOTHING")
    .bind(now_ms() / BUCKET_MS * BUCKET_MS)
    .execute(pool)
    .await?;
  if let Err(err) = deliver_reminders(pool).await {
    tracing::warn!("CareHub reminders: {}", err);
  }
  if let Err(err) = run_schedules(pool).await {
    tracing::warn!("CareHub automations: {}", err);
  }
  if tick % 60 == 5 {
    if let Err(err) = apply_retention(pool).await {
      tracing::warn!("CareHub clean-up: {}", err);
    }
  }
  if tick % 30 == 10 {
    check_backups(pool).await;
  }
  Ok(())
}

/// Started once from application.rs.
pub fn start_background(pool: PgPool) {
  STARTED_MS.store(now_ms(), Ordering::Relaxed);
  tokio::spawn(async move {
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    if let Err(err) = ensure_tables(&pool).await {
      tracing::warn!("CareHub extras: could not create tables: {}", err);
    }
    check_downtime(&pool).await;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    let mut tick: u64 = 0;
    loop {
      interval.tick().await;
      if let Err(err) = background_tick(&pool, tick).await {
        tracing::warn!("CareHub background task: {}", err);
      }
      tick = tick.wrapping_add(1);
    }
  });
}
