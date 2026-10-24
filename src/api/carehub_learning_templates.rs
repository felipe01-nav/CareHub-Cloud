//! Care Hub Learning, lane templates: block templates.
//!
//! A builder saves one block, or every block of a lesson, as a named template
//! for the workspace. Anyone who builds courses can then add it to any lesson
//! from the block library. Adding a template copies its blocks into the lesson
//! (the web gives them fresh ids and clears links to lessons or blocks of the
//! course they came from), so renaming or deleting a template never changes a
//! course.
//!
//! Every route needs "Build courses"; learners never see templates. The table
//! is made on first use. Nothing else in Care Hub Learning changes.

use crate::api::carehub_learning::{self, Ctx};
use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Bytes, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use sqlx::{PgConnection, PgPool};
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

static READY: AtomicBool = AtomicBool::new(false);

const TABLE_LOCK_KEY: i64 = 7_316_402_240;
/// With hashtext(workspace id): one lock per workspace while the number of
/// templates and their names are checked, so the limit and the name check are
/// exact.
const TEMPLATE_LOCK_CLASS: i32 = 7_316_420;
const BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_TEMPLATES: i64 = 200;
const MAX_BLOCKS: usize = 50;
const MAX_BLOCKS_BYTES: usize = 512 * 1024;
const MAX_NAME_CHARS: usize = 80;
const MAX_TYPE_CHARS: usize = 40;
const MAX_LISTED: i64 = 500;

const BUILD_MSG: &str =
  "You need the \"Build courses\" permission for this. Ask an admin to update your role.";
const SETUP_MSG: &str = "Learning is still being set up. Please try again in a minute.";
const SERVER_MSG: &str = "Something went wrong on the server. Please try again in a moment.";
const BODY_MSG: &str = "That request couldn't be read. Please reload the page and try again.";
const LINK_MSG: &str = "That template link doesn't look right.";
const MISSING_MSG: &str = "That template couldn't be found. It may have been deleted.";
const NAME_MSG: &str = "Give the template a name.";
const NAME_LONG_MSG: &str = "Keep the template name to 80 characters or fewer.";
const NAME_TAKEN_MSG: &str = "A template with this name already exists.";
const FULL_MSG: &str = "This workspace has 200 templates. Delete one you no longer need first.";
const EMPTY_MSG: &str = "Pick at least one block to save.";
const MANY_MSG: &str = "A template can hold up to 50 blocks.";
const DAMAGED_MSG: &str = "The template content looks damaged. Please reload the page.";
const BIG_MSG: &str =
  "These blocks are too big for a template (over 512 KB). Use links for big images and videos.";
const FORMER_MEMBER: &str = "Former member";

const TABLES: &[&str] = &[
  "CREATE TABLE IF NOT EXISTS carehub_block_template (\
     id UUID PRIMARY KEY, workspace_id UUID NOT NULL, name TEXT NOT NULL, \
     blocks JSONB NOT NULL DEFAULT '[]'::jsonb, block_count INT NOT NULL DEFAULT 0, \
     block_types JSONB NOT NULL DEFAULT '[]'::jsonb, created_by BIGINT NOT NULL DEFAULT 0, \
     created_ms BIGINT NOT NULL DEFAULT 0, updated_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE UNIQUE INDEX IF NOT EXISTS carehub_block_template_name \
     ON carehub_block_template (workspace_id, lower(name))",
];

/// Creates the template table once. A failure is logged and turned into a
/// friendly message; it never stops the server.
async fn ready(pool: &PgPool) -> Result<(), AppError> {
  if READY.load(Ordering::Relaxed) {
    return Ok(());
  }
  match create_tables(pool).await {
    Ok(()) => {
      READY.store(true, Ordering::Relaxed);
      Ok(())
    },
    Err(err) => {
      tracing::warn!("carehub learning templates: could not create tables: {}", err);
      Err(bad(SETUP_MSG))
    },
  }
}

async fn create_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
  let mut tx = pool.begin().await?;
  sqlx::query("SELECT pg_advisory_xact_lock($1)")
    .bind(TABLE_LOCK_KEY)
    .execute(&mut *tx)
    .await?;
  for sql in TABLES {
    sqlx::query(sql).execute(&mut *tx).await?;
  }
  tx.commit().await
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

type Reply = ActixResult<Json<AppResponse<Value>>>;

fn reply(result: Result<Value, AppError>) -> Reply {
  match result {
    Ok(data) => Ok(Json(AppResponse::Ok().with_data(data))),
    Err(err) => Err(tidy(err).into()),
  }
}

/// Keeps our plain-English errors; anything unexpected is logged and replaced
/// with a friendly message (no internal details reach the browser).
fn tidy(err: AppError) -> AppError {
  match err {
    AppError::InvalidRequest(_) | AppError::RecordNotFound(_) | AppError::NotEnoughPermissions => {
      err
    },
    other => {
      tracing::warn!("carehub learning templates: {}", other);
      bad(SERVER_MSG)
    },
  }
}

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn missing() -> AppError {
  AppError::RecordNotFound(MISSING_MSG.to_string())
}

fn now_ms() -> i64 {
  chrono::Utc::now().timestamp_millis()
}

fn parse_uuid(raw: &str, msg: &str) -> Result<Uuid, AppError> {
  Uuid::parse_str(raw.trim()).map_err(|_| bad(msg))
}

fn body_json(body: &[u8]) -> Result<Value, AppError> {
  if body.iter().all(|b| b.is_ascii_whitespace()) {
    return Ok(json!({}));
  }
  match serde_json::from_slice::<Value>(body) {
    Ok(v) if v.is_object() => Ok(v),
    _ => Err(bad(BODY_MSG)),
  }
}

fn txt<'a>(v: &'a Value, key: &str) -> &'a str {
  v.get(key).and_then(|x| x.as_str()).unwrap_or("")
}

fn chars(text: &str) -> usize {
  text.chars().count()
}

/// Removes NUL characters from every string and key in a JSON value
/// (Postgres can't store them).
fn strip_nul(v: &mut Value) {
  match v {
    Value::String(s) => {
      if s.contains('\0') {
        *s = s.replace('\0', "");
      }
    },
    Value::Array(items) => {
      for x in items.iter_mut() {
        strip_nul(x);
      }
    },
    Value::Object(o) => {
      if o.keys().any(|k| k.contains('\0')) {
        let old = std::mem::take(o);
        for (k, mut x) in old {
          strip_nul(&mut x);
          o.insert(k.replace('\0', ""), x);
        }
      } else {
        for (_, x) in o.iter_mut() {
          strip_nul(x);
        }
      }
    },
    _ => {},
  }
}

fn need_build(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_build") {
    Ok(())
  } else {
    Err(bad(BUILD_MSG))
  }
}

/// One lock per workspace while the number of templates and their names are
/// checked and saved.
async fn lock_templates(conn: &mut PgConnection, ws: Uuid) -> Result<(), AppError> {
  sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
    .bind(TEMPLATE_LOCK_CLASS)
    .bind(ws.to_string())
    .execute(&mut *conn)
    .await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

/// A name on one line: runs of spaces, tabs and line breaks become one space;
/// other control characters and text-direction overrides are removed; trimmed.
fn template_name(raw: &str) -> Result<String, AppError> {
  let mut name = String::with_capacity(raw.len());
  let mut gap = false;
  for ch in raw.chars() {
    if ch.is_whitespace() {
      gap = true;
      continue;
    }
    if ch.is_control() || matches!(ch, '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}') {
      continue;
    }
    if gap && !name.is_empty() {
      name.push(' ');
    }
    gap = false;
    name.push(ch);
  }
  if name.is_empty() {
    Err(bad(NAME_MSG))
  } else if chars(&name) > MAX_NAME_CHARS {
    Err(bad(NAME_LONG_MSG))
  } else {
    Ok(name)
  }
}

/// The blocks as saved: 1–50 objects, each with a short non-empty string
/// "type"; NUL characters removed; at most 512 KB as compact JSON. No publish
/// checks (the course's own checks still run after the blocks are added).
/// Returns the JSON text, the number of blocks and the kinds in first-seen
/// order.
fn template_blocks(v: Value) -> Result<(String, i32, Vec<String>), AppError> {
  let mut items = match v {
    Value::Null => return Err(bad(EMPTY_MSG)),
    Value::Array(items) => items,
    _ => return Err(bad(DAMAGED_MSG)),
  };
  if items.is_empty() {
    return Err(bad(EMPTY_MSG));
  }
  if items.len() > MAX_BLOCKS {
    return Err(bad(MANY_MSG));
  }
  let mut kinds: Vec<String> = Vec::new();
  for b in items.iter_mut() {
    strip_nul(b);
    let kind = match b.get("type").and_then(|t| t.as_str()) {
      Some(t) if !t.trim().is_empty() && chars(t) <= MAX_TYPE_CHARS => t.to_string(),
      _ => return Err(bad(DAMAGED_MSG)),
    };
    if !kinds.contains(&kind) {
      kinds.push(kind);
    }
  }
  let count = items.len() as i32;
  let text = Value::Array(items).to_string();
  if text.len() > MAX_BLOCKS_BYTES {
    return Err(bad(BIG_MSG));
  }
  Ok((text, count, kinds))
}

/// Another template in the workspace already has this name (ignoring case).
async fn name_taken(
  conn: &mut PgConnection,
  ws: Uuid,
  name: &str,
  except: Uuid,
) -> Result<bool, AppError> {
  let (taken,): (bool,) = sqlx::query_as(
    "SELECT EXISTS (SELECT 1 FROM carehub_block_template \
       WHERE workspace_id = $1 AND lower(name) = lower($2) AND id <> $3)",
  )
  .bind(ws)
  .bind(name)
  .bind(except)
  .fetch_one(&mut *conn)
  .await?;
  Ok(taken)
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

/// id, name, block_count, block_types (JSON text), created_ms, updated_ms,
/// saver's name.
type InfoRow = (Uuid, String, i32, String, i64, i64, String);

fn info_json(r: InfoRow) -> Value {
  let (id, name, count, kinds, saved_ms, updated_ms, by) = r;
  let kinds = serde_json::from_str::<Value>(&kinds)
    .ok()
    .filter(|v| v.is_array())
    .unwrap_or_else(|| json!([]));
  let by = if by.is_empty() { FORMER_MEMBER.to_string() } else { by };
  json!({
    "id": id.to_string(),
    "name": name,
    "block_count": count,
    "block_types": kinds,
    "saved_by_name": by,
    "saved_ms": saved_ms,
    "updated_ms": updated_ms,
  })
}

async fn fetch_info(conn: &mut PgConnection, ws: Uuid, id: Uuid) -> Result<Value, AppError> {
  let row: Option<(Uuid, String, i32, String, i64, i64, String)> = sqlx::query_as(
    "SELECT t.id, t.name, t.block_count, t.block_types::text, t.created_ms, t.updated_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
     FROM carehub_block_template t LEFT JOIN af_user u ON u.uid = t.created_by \
     WHERE t.id = $1 AND t.workspace_id = $2",
  )
  .bind(id)
  .bind(ws)
  .fetch_optional(&mut *conn)
  .await?;
  row.map(info_json).ok_or_else(missing)
}

/// GET /templates (no blocks)
async fn list_templates(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_build(&c)?;
  ready(&state.pg_pool).await?;
  let rows: Vec<(Uuid, String, i32, String, i64, i64, String)> = sqlx::query_as(
    "SELECT t.id, t.name, t.block_count, t.block_types::text, t.created_ms, t.updated_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
     FROM carehub_block_template t LEFT JOIN af_user u ON u.uid = t.created_by \
     WHERE t.workspace_id = $1 ORDER BY lower(t.name), t.id LIMIT $2",
  )
  .bind(c.ws)
  .bind(MAX_LISTED)
  .fetch_all(&state.pg_pool)
  .await?;
  let templates: Vec<Value> = rows.into_iter().map(info_json).collect();
  Ok(json!({ "templates": templates }))
}

/// POST /templates {name, blocks}
async fn create_template(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_build(&c)?;
  let mut body = body_json(body)?;
  let name = template_name(txt(&body, "name"))?;
  let blocks = body.get_mut("blocks").map(std::mem::take).unwrap_or(Value::Null);
  let (blocks, count, kinds) = template_blocks(blocks)?;
  let kinds = Value::from(kinds).to_string();
  ready(&state.pg_pool).await?;
  let id = Uuid::new_v4();
  let now = now_ms();
  let mut tx = state.pg_pool.begin().await?;
  lock_templates(&mut tx, c.ws).await?;
  let (saved,): (i64,) =
    sqlx::query_as("SELECT COUNT(*) FROM carehub_block_template WHERE workspace_id = $1")
      .bind(c.ws)
      .fetch_one(&mut *tx)
      .await?;
  if saved >= MAX_TEMPLATES {
    return Err(bad(FULL_MSG));
  }
  if name_taken(&mut tx, c.ws, &name, id).await? {
    return Err(bad(NAME_TAKEN_MSG));
  }
  sqlx::query(
    "INSERT INTO carehub_block_template \
       (id, workspace_id, name, blocks, block_count, block_types, created_by, created_ms, updated_ms) \
     VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb, $7, $8, $8)",
  )
  .bind(id)
  .bind(c.ws)
  .bind(&name)
  .bind(&blocks)
  .bind(count)
  .bind(&kinds)
  .bind(c.uid())
  .bind(now)
  .execute(&mut *tx)
  .await?;
  let info = fetch_info(&mut tx, c.ws, id).await?;
  tx.commit().await?;
  Ok(json!({ "template": info }))
}

/// GET /templates/{id} (with its blocks, as saved)
async fn get_template(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_build(&c)?;
  let template = parse_uuid(id, LINK_MSG)?;
  ready(&state.pg_pool).await?;
  let row: Option<(Uuid, String, i32, String, i64, i64, String, String)> = sqlx::query_as(
    "SELECT t.id, t.name, t.block_count, t.block_types::text, t.created_ms, t.updated_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), \
       t.blocks::text \
     FROM carehub_block_template t LEFT JOIN af_user u ON u.uid = t.created_by \
     WHERE t.id = $1 AND t.workspace_id = $2",
  )
  .bind(template)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (tid, name, count, kinds, saved_ms, updated_ms, by, blocks) = row.ok_or_else(missing)?;
  let mut info = info_json((tid, name, count, kinds, saved_ms, updated_ms, by));
  let blocks = serde_json::from_str::<Value>(&blocks)
    .ok()
    .filter(|v| v.is_array())
    .unwrap_or_else(|| json!([]));
  if let Some(o) = info.as_object_mut() {
    o.insert("blocks".to_string(), blocks);
  }
  Ok(json!({ "template": info }))
}

/// PUT /templates/{id} {name} (rename only; other keys are ignored)
async fn rename_template(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_build(&c)?;
  let template = parse_uuid(id, LINK_MSG)?;
  let body = body_json(body)?;
  let name = template_name(txt(&body, "name"))?;
  ready(&state.pg_pool).await?;
  let mut tx = state.pg_pool.begin().await?;
  lock_templates(&mut tx, c.ws).await?;
  let row: Option<(String, i64)> = sqlx::query_as(
    "SELECT name, updated_ms FROM carehub_block_template \
     WHERE id = $1 AND workspace_id = $2 FOR UPDATE",
  )
  .bind(template)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let (old_name, old_ms) = row.ok_or_else(missing)?;
  if name != old_name {
    if name_taken(&mut tx, c.ws, &name, template).await? {
      return Err(bad(NAME_TAKEN_MSG));
    }
    let updated_ms = now_ms().max(old_ms.saturating_add(1));
    sqlx::query(
      "UPDATE carehub_block_template SET name = $3, updated_ms = $4 \
       WHERE id = $1 AND workspace_id = $2",
    )
    .bind(template)
    .bind(c.ws)
    .bind(&name)
    .bind(updated_ms)
    .execute(&mut *tx)
    .await?;
  }
  let info = fetch_info(&mut tx, c.ws, template).await?;
  tx.commit().await?;
  Ok(json!({ "template": info }))
}

/// DELETE /templates/{id} (courses that used it keep their blocks; already
/// gone is fine)
async fn delete_template(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_build(&c)?;
  let template = parse_uuid(id, LINK_MSG)?;
  ready(&state.pg_pool).await?;
  sqlx::query("DELETE FROM carehub_block_template WHERE id = $1 AND workspace_id = $2")
    .bind(template)
    .bind(c.ws)
    .execute(&state.pg_pool)
    .await?;
  Ok(json!({ "ok": true }))
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

async fn templates_list_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
) -> Reply {
  let ws = path.into_inner();
  reply(list_templates(&state, &user, &ws).await)
}

async fn templates_create_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
  body: Bytes,
) -> Reply {
  let ws = path.into_inner();
  reply(create_template(&state, &user, &ws, &body).await)
}

async fn template_get_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(get_template(&state, &user, &ws, &id).await)
}

async fn template_rename_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(rename_template(&state, &user, &ws, &id, &body).await)
}

async fn template_delete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(delete_template(&state, &user, &ws, &id).await)
}

/// Called once from the end of carehub_learning::configure.
pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/templates")
        .app_data(web::PayloadConfig::new(BODY_BYTES))
        .route(web::get().to(templates_list_handler))
        .route(web::post().to(templates_create_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/templates/{id}")
        .app_data(web::PayloadConfig::new(BODY_BYTES))
        .route(web::get().to(template_get_handler))
        .route(web::put().to(template_rename_handler))
        .route(web::delete().to(template_delete_handler)),
    );
}
