//! Care Hub Learning, lane drafts: share a course draft for review by link,
//! and comments on a draft (on a block or on the whole course).
//!
//! Additive: two tables made on first use and twelve routes, registered by one
//! line at the end of `carehub_learning::configure`. A review link works only
//! for signed-in, non-guest members of the course's workspace and shows the
//! current draft, read-only. Learner routes never see links or comments.

use crate::api::carehub_learning;
use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Bytes, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use base64::Engine;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use sqlx::{PgConnection, PgPool};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

static READY: AtomicBool = AtomicBool::new(false);

/// Lock ids of our own: learning 219, assign 220, bank 221 and the lock
/// classes 7_316_403 / 7_316_404 belong to the other learning modules.
const TABLE_LOCK_KEY: i64 = 7_316_402_230;
const COMMENT_LOCK_CLASS: i32 = 7_316_410;

const MAX_COMMENT_CHARS: usize = 2000;
const MAX_COMMENTS: i64 = 2000;
const MAX_ID_CHARS: usize = 64;
const TOKEN_BYTES: usize = 32;
const TOKEN_CHARS: usize = 43;

const BUILD_MSG: &str =
  "You need the \"Build courses\" permission for this. Ask an admin to update your role.";
const SETUP_MSG: &str = "Learning is still being set up. Please try again in a minute.";
const SERVER_MSG: &str = "Something went wrong on the server. Please try again in a moment.";
const BODY_MSG: &str = "That request couldn't be read. Please reload the page and try again.";
const COURSE_LINK_MSG: &str = "That course link doesn't look right.";
const MISSING_MSG: &str = "That course couldn't be found. It may have been deleted.";
const LINK_OFF_MSG: &str =
  "This review link was turned off or doesn't exist. Ask the author for a new link.";
const EMPTY_MSG: &str = "Write a comment first.";
const LONG_MSG: &str = "A comment can be at most 2,000 characters.";
const MANY_MSG: &str = "This draft has too many comments. Delete some old ones first.";
const BLOCK_GONE_MSG: &str =
  "That block isn't in the draft any more. Reload to see the latest version.";
const COMMENT_GONE_MSG: &str = "That comment was deleted. Reload to see the latest comments.";
const DONE_PERM_MSG: &str = "Only people who build courses can mark comments done.";
const DELETE_PERM_MSG: &str =
  "Only the person who wrote a comment, or someone who builds courses, can delete it.";
const UNTITLED: &str = "Untitled course";

const TABLES: &[&str] = &[
  "CREATE TABLE IF NOT EXISTS carehub_course_review_link (\
     course_id UUID PRIMARY KEY REFERENCES carehub_course (id) ON DELETE CASCADE, \
     workspace_id UUID NOT NULL, token TEXT NOT NULL UNIQUE, \
     created_by BIGINT NOT NULL DEFAULT 0, created_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE TABLE IF NOT EXISTS carehub_course_comment (\
     id UUID PRIMARY KEY, \
     course_id UUID NOT NULL REFERENCES carehub_course (id) ON DELETE CASCADE, \
     workspace_id UUID NOT NULL, parent_id UUID, \
     lesson_id TEXT NOT NULL DEFAULT '', block_id TEXT NOT NULL DEFAULT '', \
     uid BIGINT NOT NULL, body TEXT NOT NULL, done BOOLEAN NOT NULL DEFAULT FALSE, \
     done_by BIGINT NOT NULL DEFAULT 0, done_ms BIGINT NOT NULL DEFAULT 0, \
     created_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE INDEX IF NOT EXISTS carehub_course_comment_course \
     ON carehub_course_comment (course_id, created_ms)",
];

/// Creates the drafts tables once (after the learning tables they point at).
/// A failure is logged and turned into a friendly message; it never stops the
/// server.
async fn ready(pool: &PgPool) -> Result<(), AppError> {
  if READY.load(Ordering::Relaxed) {
    return Ok(());
  }
  carehub_learning::ready(pool).await?;
  match create_tables(pool).await {
    Ok(()) => {
      READY.store(true, Ordering::Relaxed);
      Ok(())
    },
    Err(err) => {
      tracing::warn!("carehub learning drafts: could not create tables: {}", err);
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
      tracing::warn!("carehub learning drafts: {}", other);
      bad(SERVER_MSG)
    },
  }
}

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn link_off() -> AppError {
  AppError::RecordNotFound(LINK_OFF_MSG.to_string())
}

fn comment_gone() -> AppError {
  AppError::RecordNotFound(COMMENT_GONE_MSG.to_string())
}

fn course_missing() -> AppError {
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

/// 32 random bytes from the OS (Uuid::new_v4 reads getrandom; its fixed
/// version and variant bytes are skipped), as base64url without padding.
fn new_token() -> String {
  let mut bytes: Vec<u8> = Vec::with_capacity(TOKEN_BYTES + 14);
  while bytes.len() < TOKEN_BYTES {
    let id = Uuid::new_v4();
    for (i, b) in id.as_bytes().iter().enumerate() {
      if i != 6 && i != 8 {
        bytes.push(*b);
      }
    }
  }
  bytes.truncate(TOKEN_BYTES);
  base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
}

fn token_ok(token: &str) -> bool {
  token.len() == TOKEN_CHARS
    && token
      .bytes()
      .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Line breaks become "\n" and tabs spaces; other control characters and
/// text-direction overrides are dropped; then the text is trimmed.
fn tidy_text(raw: &str) -> String {
  let mut out = String::with_capacity(raw.len());
  let mut it = raw.chars().peekable();
  while let Some(ch) = it.next() {
    match ch {
      '\r' => {
        if it.peek() == Some(&'\n') {
          it.next();
        }
        out.push('\n');
      },
      '\n' => out.push('\n'),
      '\t' => out.push(' '),
      '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' => {},
      c if c.is_control() => {},
      c => out.push(c),
    }
  }
  out.trim().to_string()
}

// ---------------------------------------------------------------------------
// Who may open which draft
// ---------------------------------------------------------------------------

/// The course a request is about and what the caller may do there.
struct Draft {
  ws: Uuid,
  course: Uuid,
  uid: i64,
  can_build: bool,
}

/// Comments come in through Studio (by course id, builders) or through a
/// review link (by token, any member who has a working link).
#[derive(Clone, Copy)]
enum Base<'a> {
  Studio(&'a str),
  Review(&'a str),
}

async fn studio_draft(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Draft, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  if !c.has("learning_build") {
    return Err(bad(BUILD_MSG));
  }
  ready(&state.pg_pool).await?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let row: Option<(Uuid,)> =
    sqlx::query_as("SELECT id FROM carehub_course WHERE id = $1 AND workspace_id = $2")
      .bind(course)
      .bind(c.ws)
      .fetch_optional(&state.pg_pool)
      .await?;
  if row.is_none() {
    return Err(course_missing());
  }
  Ok(Draft {
    ws: c.ws,
    course,
    uid: c.uid(),
    can_build: true,
  })
}

async fn review_draft(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  token: &str,
) -> Result<Draft, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  ready(&state.pg_pool).await?;
  let token = token.trim();
  if !token_ok(token) {
    return Err(link_off());
  }
  let row: Option<(Uuid,)> = sqlx::query_as(
    "SELECT l.course_id FROM carehub_course_review_link l \
     JOIN carehub_course c ON c.id = l.course_id \
     WHERE l.token = $1 AND l.workspace_id = $2 AND c.workspace_id = $2",
  )
  .bind(token)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let course = row.map(|r| r.0).ok_or_else(link_off)?;
  Ok(Draft {
    ws: c.ws,
    course,
    uid: c.uid(),
    can_build: c.has("learning_build"),
  })
}

async fn draft_for(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  base: Base<'_>,
) -> Result<Draft, AppError> {
  match base {
    Base::Studio(id) => studio_draft(state, user, ws, id).await,
    Base::Review(token) => review_draft(state, user, ws, token).await,
  }
}

// ---------------------------------------------------------------------------
// Review links (Studio, learning_build)
// ---------------------------------------------------------------------------

async fn load_link(pool: &PgPool, d: &Draft) -> Result<Value, AppError> {
  let row: Option<(String, i64, String)> = sqlx::query_as(
    "SELECT l.token, l.created_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
     FROM carehub_course_review_link l LEFT JOIN af_user u ON u.uid = l.created_by \
     WHERE l.course_id = $1 AND l.workspace_id = $2",
  )
  .bind(d.course)
  .bind(d.ws)
  .fetch_optional(pool)
  .await?;
  Ok(match row {
    Some((token, created_ms, name)) => {
      json!({ "token": token, "created_ms": created_ms, "created_by_name": name })
    },
    None => Value::Null,
  })
}

async fn link_get(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let d = studio_draft(state, user, ws, id).await?;
  let link = load_link(&state.pg_pool, &d).await?;
  Ok(json!({ "link": link }))
}

/// Gives the link that's on, or makes a new one (a new token every time the
/// link was turned off before).
async fn link_make(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let d = studio_draft(state, user, ws, id).await?;
  sqlx::query(
    "INSERT INTO carehub_course_review_link \
       (course_id, workspace_id, token, created_by, created_ms) \
     VALUES ($1, $2, $3, $4, $5) ON CONFLICT (course_id) DO NOTHING",
  )
  .bind(d.course)
  .bind(d.ws)
  .bind(new_token())
  .bind(d.uid)
  .bind(now_ms())
  .execute(&state.pg_pool)
  .await?;
  let link = load_link(&state.pg_pool, &d).await?;
  if link.is_null() {
    return Err(course_missing());
  }
  Ok(json!({ "link": link }))
}

async fn link_turn_off(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let d = studio_draft(state, user, ws, id).await?;
  sqlx::query("DELETE FROM carehub_course_review_link WHERE course_id = $1 AND workspace_id = $2")
    .bind(d.course)
    .bind(d.ws)
    .execute(&state.pg_pool)
    .await?;
  Ok(json!({ "ok": true }))
}

// ---------------------------------------------------------------------------
// The review page (any non-guest member with a working link)
// ---------------------------------------------------------------------------

fn pick_title(draft: &Value, column: &str) -> String {
  let t = txt(draft, "title").trim();
  if !t.is_empty() {
    return t.to_string();
  }
  let t = column.trim();
  if !t.is_empty() {
    return t.to_string();
  }
  UNTITLED.to_string()
}

/// The current saved draft (not a copy), exactly as Studio loads it.
async fn review_get(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  token: &str,
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  ready(&state.pg_pool).await?;
  let token = token.trim();
  if !token_ok(token) {
    return Err(link_off());
  }
  let row: Option<(Uuid, String, String, String, i64, String)> = sqlx::query_as(
    "SELECT c.id, c.title, c.status, c.draft::text, c.draft_updated_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
     FROM carehub_course_review_link l \
     JOIN carehub_course c ON c.id = l.course_id \
     LEFT JOIN af_user u ON u.uid = l.created_by \
     WHERE l.token = $1 AND l.workspace_id = $2 AND c.workspace_id = $2",
  )
  .bind(token)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (course, title_col, status, draft_text, updated_ms, shared_by) =
    row.ok_or_else(link_off)?;
  let draft: Value = serde_json::from_str(&draft_text).unwrap_or_else(|_| json!({}));
  let title = pick_title(&draft, &title_col);
  Ok(json!({
    "course_id": course.to_string(),
    "title": title,
    "status": status,
    "draft": draft,
    "updated_ms": updated_ms,
    "shared_by_name": shared_by,
    "can_build": c.has("learning_build"),
  }))
}

// ---------------------------------------------------------------------------
// Comments (same rules through Studio and through a review link)
// ---------------------------------------------------------------------------

/// (id, parent_id, lesson_id, block_id, body, uid, done, done_ms, created_ms,
/// writer name, done-by name)
type CommentRow = (Uuid, Option<Uuid>, String, String, String, i64, bool, i64, i64, String, String);

fn comment_json(r: &CommentRow, me: i64) -> Value {
  let thread = r.1.is_none();
  let done = thread && r.6;
  let parent_id = r.1.map(|p| p.to_string()).unwrap_or_default();
  let done_ms = if done { json!(r.7) } else { Value::Null };
  let done_by = if done { r.10.clone() } else { String::new() };
  let mut v = json!({
    "id": r.0.to_string(),
    "parent_id": parent_id,
    "lesson_id": r.2,
    "block_id": r.3,
    "text": r.4,
    "name": r.9,
    "created_ms": r.8,
    "mine": r.5 == me,
    "done": done,
    "done_ms": done_ms,
    "done_by_name": done_by,
  });
  if thread {
    if let Some(obj) = v.as_object_mut() {
      obj.insert("replies".to_string(), json!([]));
    }
  }
  v
}

/// Threads oldest first, each with its replies oldest first.
fn threads(rows: &[CommentRow], me: i64) -> Vec<Value> {
  let mut out: Vec<Value> = Vec::new();
  let mut at: HashMap<Uuid, usize> = HashMap::new();
  for r in rows.iter().filter(|r| r.1.is_none()) {
    at.insert(r.0, out.len());
    out.push(comment_json(r, me));
  }
  for r in rows.iter() {
    let parent = match r.1 {
      Some(p) => p,
      None => continue,
    };
    let slot = match at.get(&parent) {
      Some(i) => *i,
      None => continue,
    };
    if let Some(Value::Array(list)) = out.get_mut(slot).and_then(|t| t.get_mut("replies")) {
      list.push(comment_json(r, me));
    }
  }
  out
}

async fn list_comments(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  base: Base<'_>,
) -> Result<Value, AppError> {
  let d = draft_for(state, user, ws, base).await?;
  let rows: Vec<CommentRow> = sqlx::query_as(
    "SELECT m.id, m.parent_id, m.lesson_id, m.block_id, m.body, m.uid, m.done, m.done_ms, \
       m.created_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), \
       COALESCE(NULLIF(b.name, ''), split_part(COALESCE(b.email, ''), '@', 1), '') \
     FROM carehub_course_comment m \
     LEFT JOIN af_user u ON u.uid = m.uid \
     LEFT JOIN af_user b ON b.uid = m.done_by AND m.done \
     WHERE m.course_id = $1 AND m.workspace_id = $2 \
     ORDER BY m.created_ms, m.id LIMIT 5000",
  )
  .bind(d.course)
  .bind(d.ws)
  .fetch_all(&state.pg_pool)
  .await?;
  Ok(json!({ "comments": threads(&rows, d.uid), "can_build": d.can_build }))
}

/// One writer at a time per course, so the 2000 cap is exact.
async fn lock_course(conn: &mut PgConnection, course: Uuid) -> Result<(), AppError> {
  sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
    .bind(COMMENT_LOCK_CLASS)
    .bind(course.to_string())
    .execute(&mut *conn)
    .await?;
  Ok(())
}

/// A new thread sits on a block of that lesson in the current draft, or on
/// the whole course (both ids empty).
async fn check_place(
  conn: &mut PgConnection,
  d: &Draft,
  lesson: &str,
  block: &str,
) -> Result<(), AppError> {
  if lesson.is_empty() && block.is_empty() {
    return Ok(());
  }
  if lesson.is_empty()
    || block.is_empty()
    || lesson.chars().count() > MAX_ID_CHARS
    || block.chars().count() > MAX_ID_CHARS
  {
    return Err(bad(BLOCK_GONE_MSG));
  }
  let row: Option<(bool,)> = sqlx::query_as(
    "SELECT EXISTS (SELECT 1 \
       FROM jsonb_array_elements(CASE WHEN jsonb_typeof(c.draft->'lessons') = 'array' \
         THEN c.draft->'lessons' ELSE '[]'::jsonb END) AS l(v), \
       jsonb_array_elements(CASE WHEN jsonb_typeof(l.v->'blocks') = 'array' \
         THEN l.v->'blocks' ELSE '[]'::jsonb END) AS b(v) \
       WHERE l.v->>'id' = $3 AND b.v->>'id' = $4) \
     FROM carehub_course c WHERE c.id = $1 AND c.workspace_id = $2",
  )
  .bind(d.course)
  .bind(d.ws)
  .bind(lesson)
  .bind(block)
  .fetch_optional(&mut *conn)
  .await?;
  match row {
    Some((true,)) => Ok(()),
    Some(_) => Err(bad(BLOCK_GONE_MSG)),
    None => Err(course_missing()),
  }
}

/// New thread or reply. A reply joins the thread of the comment it answers
/// (one level) and takes the thread's place. A reply from someone who can't
/// build courses reopens a done thread (the reviewer says it isn't fixed).
async fn add_comment(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  base: Base<'_>,
  body: &[u8],
) -> Result<Value, AppError> {
  let d = draft_for(state, user, ws, base).await?;
  let b = body_json(body)?;
  let text = tidy_text(txt(&b, "text"));
  if text.is_empty() {
    return Err(bad(EMPTY_MSG));
  }
  if text.chars().count() > MAX_COMMENT_CHARS {
    return Err(bad(LONG_MSG));
  }
  let parent_raw = txt(&b, "parent_id").trim();
  let lesson_raw = txt(&b, "lesson_id").trim();
  let block_raw = txt(&b, "block_id").trim();
  let pool = &state.pg_pool;
  let mut tx = pool.begin().await?;
  lock_course(&mut *tx, d.course).await?;
  let (count,): (i64,) = sqlx::query_as(
    "SELECT COUNT(*) FROM carehub_course_comment WHERE course_id = $1 AND workspace_id = $2",
  )
  .bind(d.course)
  .bind(d.ws)
  .fetch_one(&mut *tx)
  .await?;
  if count >= MAX_COMMENTS {
    return Err(bad(MANY_MSG));
  }
  let (parent, lesson_id, block_id, thread_done) = if parent_raw.is_empty() {
    check_place(&mut *tx, &d, lesson_raw, block_raw).await?;
    (None, lesson_raw.to_string(), block_raw.to_string(), false)
  } else {
    let pid = Uuid::parse_str(parent_raw).map_err(|_| comment_gone())?;
    let row: Option<(Uuid, String, String, bool)> = sqlx::query_as(
      "SELECT t.id, t.lesson_id, t.block_id, t.done FROM carehub_course_comment p \
       JOIN carehub_course_comment t ON t.id = COALESCE(p.parent_id, p.id) \
       WHERE p.id = $1 AND p.course_id = $2 AND p.workspace_id = $3 \
         AND t.course_id = $2 AND t.parent_id IS NULL",
    )
    .bind(pid)
    .bind(d.course)
    .bind(d.ws)
    .fetch_optional(&mut *tx)
    .await?;
    let (thread, lesson, block, done) = row.ok_or_else(comment_gone)?;
    (Some(thread), lesson, block, done)
  };
  let id = Uuid::new_v4();
  let now = now_ms();
  sqlx::query(
    "INSERT INTO carehub_course_comment \
       (id, course_id, workspace_id, parent_id, lesson_id, block_id, uid, body, created_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
  )
  .bind(id)
  .bind(d.course)
  .bind(d.ws)
  .bind(parent)
  .bind(lesson_id.as_str())
  .bind(block_id.as_str())
  .bind(d.uid)
  .bind(text.as_str())
  .bind(now)
  .execute(&mut *tx)
  .await?;
  if let Some(thread) = parent {
    if thread_done && !d.can_build {
      sqlx::query(
        "UPDATE carehub_course_comment SET done = FALSE, done_by = 0, done_ms = 0 \
         WHERE id = $1 AND course_id = $2",
      )
      .bind(thread)
      .bind(d.course)
      .execute(&mut *tx)
      .await?;
    }
  }
  tx.commit().await?;
  let name: Option<(String,)> = sqlx::query_as(
    "SELECT COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
     FROM af_user u WHERE u.uid = $1",
  )
  .bind(d.uid)
  .fetch_optional(pool)
  .await?;
  let name = name.map(|n| n.0).unwrap_or_default();
  let row: CommentRow = (id, parent, lesson_id, block_id, text, d.uid, false, 0, now, name, String::new());
  Ok(json!({ "comment": comment_json(&row, d.uid) }))
}

/// Builders only. A reply's id acts on its thread. Marking a done thread done
/// again keeps who did it first.
async fn set_done(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  base: Base<'_>,
  cid: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let d = draft_for(state, user, ws, base).await?;
  if !d.can_build {
    return Err(bad(DONE_PERM_MSG));
  }
  let b = body_json(body)?;
  let done = match b.get("done") {
    Some(Value::Bool(x)) => *x,
    _ => return Err(bad(BODY_MSG)),
  };
  let cid = Uuid::parse_str(cid.trim()).map_err(|_| comment_gone())?;
  let res = sqlx::query(
    "UPDATE carehub_course_comment t SET \
       done_by = CASE WHEN NOT $4 THEN 0 WHEN t.done THEN t.done_by ELSE $5 END, \
       done_ms = CASE WHEN NOT $4 THEN 0 WHEN t.done THEN t.done_ms ELSE $6 END, \
       done = $4 \
     FROM carehub_course_comment p \
     WHERE p.id = $1 AND p.course_id = $2 AND p.workspace_id = $3 \
       AND t.id = COALESCE(p.parent_id, p.id) AND t.course_id = $2 AND t.parent_id IS NULL",
  )
  .bind(cid)
  .bind(d.course)
  .bind(d.ws)
  .bind(done)
  .bind(d.uid)
  .bind(now_ms())
  .execute(&state.pg_pool)
  .await?;
  if res.rows_affected() == 0 {
    return Err(comment_gone());
  }
  Ok(json!({ "ok": true }))
}

/// The writer or a builder. Deleting a thread deletes its replies too.
async fn delete_comment(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  base: Base<'_>,
  cid: &str,
) -> Result<Value, AppError> {
  let d = draft_for(state, user, ws, base).await?;
  let cid = Uuid::parse_str(cid.trim()).map_err(|_| comment_gone())?;
  let mut tx = state.pg_pool.begin().await?;
  lock_course(&mut *tx, d.course).await?;
  let row: Option<(i64,)> = sqlx::query_as(
    "SELECT uid FROM carehub_course_comment WHERE id = $1 AND course_id = $2 AND workspace_id = $3",
  )
  .bind(cid)
  .bind(d.course)
  .bind(d.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let (writer,) = row.ok_or_else(comment_gone)?;
  if writer != d.uid && !d.can_build {
    return Err(bad(DELETE_PERM_MSG));
  }
  let res = sqlx::query(
    "DELETE FROM carehub_course_comment \
     WHERE course_id = $1 AND workspace_id = $2 AND (id = $3 OR parent_id = $3)",
  )
  .bind(d.course)
  .bind(d.ws)
  .bind(cid)
  .execute(&mut *tx)
  .await?;
  tx.commit().await?;
  Ok(json!({ "ok": true, "deleted": res.rows_affected() }))
}

// ---------------------------------------------------------------------------
// HTTP handlers and routes
// ---------------------------------------------------------------------------

async fn link_get_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(link_get(&state, &user, &ws, &id).await)
}

async fn link_make_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(link_make(&state, &user, &ws, &id).await)
}

async fn link_off_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(link_turn_off(&state, &user, &ws, &id).await)
}

async fn review_get_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, token) = path.into_inner();
  reply(review_get(&state, &user, &ws, &token).await)
}

async fn studio_comments_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(list_comments(&state, &user, &ws, Base::Studio(id.as_str())).await)
}

async fn review_comments_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, token) = path.into_inner();
  reply(list_comments(&state, &user, &ws, Base::Review(token.as_str())).await)
}

async fn studio_comment_add_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(add_comment(&state, &user, &ws, Base::Studio(id.as_str()), &body).await)
}

async fn review_comment_add_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, token) = path.into_inner();
  reply(add_comment(&state, &user, &ws, Base::Review(token.as_str()), &body).await)
}

async fn studio_comment_done_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id, cid) = path.into_inner();
  reply(set_done(&state, &user, &ws, Base::Studio(id.as_str()), &cid, &body).await)
}

async fn review_comment_done_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, token, cid) = path.into_inner();
  reply(set_done(&state, &user, &ws, Base::Review(token.as_str()), &cid, &body).await)
}

async fn studio_comment_delete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String, String)>,
) -> Reply {
  let (ws, id, cid) = path.into_inner();
  reply(delete_comment(&state, &user, &ws, Base::Studio(id.as_str()), &cid).await)
}

async fn review_comment_delete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String, String)>,
) -> Reply {
  let (ws, token, cid) = path.into_inner();
  reply(delete_comment(&state, &user, &ws, Base::Review(token.as_str()), &cid).await)
}

/// Called once from the end of `carehub_learning::configure`.
pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/review-link")
        .route(web::get().to(link_get_handler))
        .route(web::post().to(link_make_handler))
        .route(web::delete().to(link_off_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/comments")
        .route(web::get().to(studio_comments_handler))
        .route(web::post().to(studio_comment_add_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/comments/{cid}")
        .route(web::delete().to(studio_comment_delete_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/comments/{cid}/done")
        .route(web::post().to(studio_comment_done_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/review/{token}")
        .route(web::get().to(review_get_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/review/{token}/comments")
        .route(web::get().to(review_comments_handler))
        .route(web::post().to(review_comment_add_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/review/{token}/comments/{cid}")
        .route(web::delete().to(review_comment_delete_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/review/{token}/comments/{cid}/done")
        .route(web::post().to(review_comment_done_handler)),
    );
}
