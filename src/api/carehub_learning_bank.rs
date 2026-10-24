//! Care Hub Learning, lane qbank3: shared question banks (copied into a course
//! version on publish, drawn per attempt), surveys (answers and results),
//! feedback for each answer, and the publish checks for the newer block kinds
//! (sorting, timeline, gallery, table, buttons, survey, video captions and
//! transcripts, links to lessons and blocks).
//!
//! The hooks in carehub_learning.rs are marked `// qbank3:`. Content without
//! the new fields plays, grades, publishes and reports exactly as before.

use crate::api::carehub_learning::{self, Ctx};
use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Bytes, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use serde_json::{json, Map, Value};
use shared_entity::response::AppResponse;
use sqlx::{PgConnection, PgPool};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

static READY: AtomicBool = AtomicBool::new(false);

const TABLE_LOCK_KEY: i64 = 7_316_402_221;
const BANK_LOCK_CLASS: i32 = 7_316_404;
const BANK_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_BANK_BYTES: usize = 1024 * 1024;
const MAX_BANK_QUESTIONS: usize = 200;
const MAX_BANK_TITLE_CHARS: usize = 80;
const MAX_BANK_DESCRIPTION_CHARS: usize = 500;
const MAX_DRAWS: usize = 10;
const MAX_FEEDBACK_CHARS: usize = 300;
const MAX_SURVEY_TEXT_CHARS: usize = 1000;
const MAX_TEXT_ANSWERS: usize = 500;
const MAX_LABEL_SHOWN: usize = 40;
/// Kept in a version's quiz when bank questions were copied into it: the
/// checksum of the draft it came from, so "unpublished changes" still compares
/// against the draft.
const DRAFT_MD5: &str = "draft_md5";

const BUILD_MSG: &str =
  "You need the \"Build courses\" permission for this. Ask an admin to update your role.";
const REPORTS_MSG: &str =
  "You need the \"See course reports\" permission for this. Ask an admin to update your role.";
const SETUP_MSG: &str = "Learning is still being set up. Please try again in a minute.";
const SERVER_MSG: &str = "Something went wrong on the server. Please try again in a moment.";
const BODY_MSG: &str = "That request couldn't be read. Please reload the page and try again.";
const COURSE_LINK_MSG: &str = "That course link doesn't look right.";
const MISSING_MSG: &str = "That course couldn't be found. It may have been deleted.";
const UNKNOWN_BLOCK_MSG: &str = "That part of the lesson isn't in this course any more.";
const BANK_LINK_MSG: &str = "That question bank link doesn't look right.";
const BANK_MISSING_MSG: &str = "That question bank couldn't be found. It may have been deleted.";
const BANK_NAME_MSG: &str = "Give the question bank a name.";
const BANK_NAME_LONG_MSG: &str = "Keep the question bank name to 80 characters or fewer.";
const BANK_DESCRIPTION_MSG: &str = "Keep the description to 500 characters or fewer.";
const BANK_MANY_MSG: &str = "A question bank can hold up to 200 questions.";
const BANK_BIG_MSG: &str =
  "This question bank is too big to save (over 1 MB). Use links for big pictures.";
const BANK_DAMAGED_MSG: &str = "The question bank content looks damaged. Please reload the page.";
const BANK_STALE_MSG: &str =
  "Someone else saved this question bank a moment ago. Reload to see their changes.";
const BANK_GONE_MSG: &str =
  "The quiz uses a question bank that has been deleted. Remove it in the quiz settings.";
const DRAWS_MANY_MSG: &str = "The quiz can take questions from up to 10 question banks.";
const SURVEY_EMPTY_MSG: &str = "Answer at least one question before sending.";
const SURVEY_LONG_MSG: &str = "One of your answers is too long (1,000 characters at most).";
const SURVEY_CHANGED_MSG: &str =
  "This survey has changed. Please reload the page and answer it again.";
const FORMER_MEMBER: &str = "Former member";
const LINK_RULE: &str = "must start with https:// (or be a Care Hub link starting with /)";

const TABLES: &[&str] = &[
  "CREATE TABLE IF NOT EXISTS carehub_question_bank (\
     id UUID PRIMARY KEY, workspace_id UUID NOT NULL, title TEXT NOT NULL, \
     description TEXT NOT NULL DEFAULT '', questions JSONB NOT NULL DEFAULT '[]'::jsonb, \
     created_by BIGINT NOT NULL DEFAULT 0, created_ms BIGINT NOT NULL DEFAULT 0, \
     updated_by BIGINT NOT NULL DEFAULT 0, updated_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE INDEX IF NOT EXISTS carehub_question_bank_ws ON carehub_question_bank (workspace_id)",
  "CREATE TABLE IF NOT EXISTS carehub_survey_answer (\
     course_id UUID NOT NULL, block_id TEXT NOT NULL, uid BIGINT NOT NULL, workspace_id UUID NOT NULL, \
     version INT NOT NULL DEFAULT 0, lesson_id TEXT NOT NULL DEFAULT '', \
     anonymous BOOLEAN NOT NULL DEFAULT FALSE, answers JSONB NOT NULL DEFAULT '{}'::jsonb, \
     created_ms BIGINT NOT NULL DEFAULT 0, updated_ms BIGINT NOT NULL DEFAULT 0, \
     PRIMARY KEY (course_id, block_id, uid))",
  "CREATE INDEX IF NOT EXISTS carehub_survey_answer_course \
     ON carehub_survey_answer (course_id, updated_ms)",
];

/// Creates the qbank3 tables once (after the learning tables they point at).
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
      tracing::warn!("carehub learning bank: could not create tables: {}", err);
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
      tracing::warn!("carehub learning bank: {}", other);
      bad(SERVER_MSG)
    },
  }
}

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn bank_missing() -> AppError {
  AppError::RecordNotFound(BANK_MISSING_MSG.to_string())
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

fn list<'a>(v: &'a Value, key: &str) -> &'a [Value] {
  match v.get(key).and_then(|x| x.as_array()) {
    Some(a) => a.as_slice(),
    None => &[],
  }
}

fn flag(v: &Value, key: &str, default: bool) -> bool {
  v.get(key).and_then(|x| x.as_bool()).unwrap_or(default)
}

fn num(v: &Value, key: &str, default: i64) -> i64 {
  match v.get(key) {
    Some(x) => x
      .as_i64()
      .or_else(|| x.as_f64().map(|f| f as i64))
      .unwrap_or(default),
    None => default,
  }
}

fn chars(text: &str) -> usize {
  text.chars().count()
}

/// Text without NUL characters (Postgres can't store them), trimmed.
fn clean_line(raw: &str) -> String {
  raw.chars().filter(|ch| *ch != '\0').collect::<String>().trim().to_string()
}

/// Removes NUL characters from every string and key in a JSON value.
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

fn push_once(problems: &mut Vec<String>, problem: String) {
  if !problems.contains(&problem) {
    problems.push(problem);
  }
}

/// Ids that are empty or appear twice.
fn bad_ids<'a>(ids: impl Iterator<Item = &'a str>) -> bool {
  let mut seen: HashSet<&str> = HashSet::new();
  let mut broken = false;
  for id in ids {
    let id = id.trim();
    if id.is_empty() || !seen.insert(id) {
      broken = true;
    }
  }
  broken
}

fn need_build(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_build") {
    Ok(())
  } else {
    Err(bad(BUILD_MSG))
  }
}

fn need_reports(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_build") || c.has("learning_analytics") {
    Ok(())
  } else {
    Err(bad(REPORTS_MSG))
  }
}

// ---------------------------------------------------------------------------
// Question banks
// ---------------------------------------------------------------------------

fn bank_title(raw: &str) -> Result<String, AppError> {
  let title = clean_line(raw);
  if title.is_empty() {
    Err(bad(BANK_NAME_MSG))
  } else if chars(&title) > MAX_BANK_TITLE_CHARS {
    Err(bad(BANK_NAME_LONG_MSG))
  } else {
    Ok(title)
  }
}

fn bank_description(raw: &str) -> Result<String, AppError> {
  let description = clean_line(raw);
  if chars(&description) > MAX_BANK_DESCRIPTION_CHARS {
    Err(bad(BANK_DESCRIPTION_MSG))
  } else {
    Ok(description)
  }
}

/// The questions as saved: shape and limits only (full checks run when a
/// course using the bank is published). A `bank_id` key is removed.
fn bank_questions(v: &Value) -> Result<String, AppError> {
  let items = match v {
    Value::Null => return Ok("[]".to_string()),
    Value::Array(items) => items,
    _ => return Err(bad(BANK_DAMAGED_MSG)),
  };
  if items.len() > MAX_BANK_QUESTIONS {
    return Err(bad(BANK_MANY_MSG));
  }
  let mut out: Vec<Value> = Vec::with_capacity(items.len());
  for q in items {
    let mut o = match q {
      Value::Object(o) => o.clone(),
      _ => return Err(bad(BANK_DAMAGED_MSG)),
    };
    o.remove("bank_id");
    let mut q = Value::Object(o);
    strip_nul(&mut q);
    out.push(q);
  }
  let text = Value::Array(out).to_string();
  if text.len() > MAX_BANK_BYTES {
    return Err(bad(BANK_BIG_MSG));
  }
  Ok(text)
}

fn present<'a>(body: &'a Value, key: &str) -> Option<&'a Value> {
  body.get(key).filter(|v| !v.is_null())
}

/// One lock per workspace while a bank name is checked and saved, so two
/// banks can't get the same name at the same moment.
async fn lock_banks(conn: &mut PgConnection, ws: Uuid) -> Result<(), AppError> {
  sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
    .bind(BANK_LOCK_CLASS)
    .bind(ws.to_string())
    .execute(&mut *conn)
    .await?;
  Ok(())
}

async fn title_taken(
  conn: &mut PgConnection,
  ws: Uuid,
  title: &str,
  except: Uuid,
) -> Result<bool, AppError> {
  let (taken,): (bool,) = sqlx::query_as(
    "SELECT EXISTS (SELECT 1 FROM carehub_question_bank \
       WHERE workspace_id = $1 AND lower(title) = lower($2) AND id <> $3)",
  )
  .bind(ws)
  .bind(title)
  .bind(except)
  .fetch_one(&mut *conn)
  .await?;
  Ok(taken)
}

fn taken_msg(title: &str) -> AppError {
  bad(&format!("There's already a question bank called “{}”.", title))
}

/// Courses whose draft takes questions from each bank: `{course_id, title}`
/// A-Z by title.
async fn used_by(pool: &PgPool, ws: Uuid) -> Result<HashMap<Uuid, Vec<Value>>, AppError> {
  let rows: Vec<(String, Uuid, String)> = sqlx::query_as(
    "SELECT COALESCE(e.draw->>'bank_id', ''), c.id, c.title \
     FROM carehub_course c \
     CROSS JOIN LATERAL jsonb_array_elements(CASE \
       WHEN jsonb_typeof(c.draft->'quiz'->'draws') = 'array' THEN c.draft->'quiz'->'draws' \
       ELSE '[]'::jsonb END) AS e(draw) \
     WHERE c.workspace_id = $1 AND jsonb_typeof(e.draw) = 'object' \
     LIMIT 20000",
  )
  .bind(ws)
  .fetch_all(pool)
  .await?;
  let mut found: HashMap<Uuid, Vec<(String, Uuid)>> = HashMap::new();
  for (bank, course, title) in rows {
    let bank = match Uuid::parse_str(bank.trim()) {
      Ok(b) => b,
      Err(_) => continue,
    };
    let courses = found.entry(bank).or_default();
    if !courses.iter().any(|(_, id)| *id == course) {
      courses.push((title, course));
    }
  }
  let mut out: HashMap<Uuid, Vec<Value>> = HashMap::new();
  for (bank, mut courses) in found {
    courses.sort_by_key(|(title, id)| (title.to_lowercase(), *id));
    let items: Vec<Value> = courses
      .into_iter()
      .map(|(title, id)| json!({ "course_id": id.to_string(), "title": title }))
      .collect();
    out.insert(bank, items);
  }
  Ok(out)
}

/// GET /banks
async fn list_banks(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_reports(&c)?;
  ready(&state.pg_pool).await?;
  let rows: Vec<(Uuid, String, String, i64, i64, String)> = sqlx::query_as(
    "SELECT b.id, b.title, b.description, \
       (CASE WHEN jsonb_typeof(b.questions) = 'array' \
         THEN jsonb_array_length(b.questions) ELSE 0 END)::BIGINT, \
       b.updated_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
     FROM carehub_question_bank b LEFT JOIN af_user u ON u.uid = b.updated_by \
     WHERE b.workspace_id = $1 ORDER BY lower(b.title), b.id LIMIT 1000",
  )
  .bind(c.ws)
  .fetch_all(&state.pg_pool)
  .await?;
  let mut used = used_by(&state.pg_pool, c.ws).await?;
  let banks: Vec<Value> = rows
    .into_iter()
    .map(|(id, title, description, count, updated_ms, by_name)| {
      json!({
        "id": id.to_string(),
        "title": title,
        "description": description,
        "question_count": count,
        "updated_ms": updated_ms,
        "updated_by_name": by_name,
        "used_by": used.remove(&id).unwrap_or_default(),
      })
    })
    .collect();
  Ok(json!({ "banks": banks }))
}

/// GET /banks/{id}
async fn get_bank(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_reports(&c)?;
  let bank = parse_uuid(id, BANK_LINK_MSG)?;
  ready(&state.pg_pool).await?;
  let row: Option<(String, String, String, i64, i64, String)> = sqlx::query_as(
    "SELECT b.title, b.description, b.questions::text, b.created_ms, b.updated_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
     FROM carehub_question_bank b LEFT JOIN af_user u ON u.uid = b.updated_by \
     WHERE b.id = $1 AND b.workspace_id = $2",
  )
  .bind(bank)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (title, description, text, created_ms, updated_ms, by_name) =
    row.ok_or_else(bank_missing)?;
  let questions: Value = match serde_json::from_str::<Value>(&text) {
    Ok(v) if v.is_array() => v,
    _ => json!([]),
  };
  let used = used_by(&state.pg_pool, c.ws)
    .await?
    .remove(&bank)
    .unwrap_or_default();
  Ok(json!({
    "bank": {
      "id": bank.to_string(),
      "title": title,
      "description": description,
      "questions": questions,
      "created_ms": created_ms,
      "updated_ms": updated_ms,
      "updated_by_name": by_name,
      "used_by": used,
    }
  }))
}

/// POST /banks
async fn create_bank(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_build(&c)?;
  let body = body_json(body)?;
  let title = bank_title(txt(&body, "title"))?;
  let description = bank_description(txt(&body, "description"))?;
  let questions = bank_questions(body.get("questions").unwrap_or(&Value::Null))?;
  ready(&state.pg_pool).await?;
  let id = Uuid::new_v4();
  let now = now_ms();
  let mut tx = state.pg_pool.begin().await?;
  lock_banks(&mut tx, c.ws).await?;
  if title_taken(&mut tx, c.ws, &title, id).await? {
    return Err(taken_msg(&title));
  }
  sqlx::query(
    "INSERT INTO carehub_question_bank \
       (id, workspace_id, title, description, questions, created_by, created_ms, updated_by, updated_ms) \
     VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $6, $7)",
  )
  .bind(id)
  .bind(c.ws)
  .bind(&title)
  .bind(&description)
  .bind(&questions)
  .bind(c.uid())
  .bind(now)
  .execute(&mut *tx)
  .await?;
  tx.commit().await?;
  Ok(json!({ "id": id.to_string(), "updated_ms": now }))
}

/// PUT /banks/{id} (autosave; a missing field stays as it is)
async fn save_bank(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_build(&c)?;
  let bank = parse_uuid(id, BANK_LINK_MSG)?;
  let body = body_json(body)?;
  let new_title = match present(&body, "title") {
    Some(v) => Some(bank_title(v.as_str().unwrap_or(""))?),
    None => None,
  };
  let new_description = match present(&body, "description") {
    Some(v) => Some(bank_description(v.as_str().unwrap_or(""))?),
    None => None,
  };
  let new_questions = match present(&body, "questions") {
    Some(v) => Some(bank_questions(v)?),
    None => None,
  };
  let base = present(&body, "base_updated_ms").and_then(|x| {
    x.as_i64().or_else(|| x.as_f64().map(|f| f as i64))
  });
  ready(&state.pg_pool).await?;
  let mut tx = state.pg_pool.begin().await?;
  lock_banks(&mut tx, c.ws).await?;
  let row: Option<(String, String, String, i64)> = sqlx::query_as(
    "SELECT title, description, questions::text, updated_ms FROM carehub_question_bank \
     WHERE id = $1 AND workspace_id = $2 FOR UPDATE",
  )
  .bind(bank)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let (old_title, old_description, old_questions, old_ms) = row.ok_or_else(bank_missing)?;
  if base.is_some_and(|b| b != old_ms) {
    return Err(bad(BANK_STALE_MSG));
  }
  let title = match new_title {
    Some(t) => t,
    None => old_title.clone(),
  };
  if title.to_lowercase() != old_title.to_lowercase()
    && title_taken(&mut tx, c.ws, &title, bank).await?
  {
    return Err(taken_msg(&title));
  }
  let description = new_description.unwrap_or(old_description);
  let questions = new_questions.unwrap_or(old_questions);
  let updated_ms = now_ms().max(old_ms.saturating_add(1));
  sqlx::query(
    "UPDATE carehub_question_bank SET title = $3, description = $4, questions = $5::jsonb, \
       updated_by = $6, updated_ms = $7 WHERE id = $1 AND workspace_id = $2",
  )
  .bind(bank)
  .bind(c.ws)
  .bind(&title)
  .bind(&description)
  .bind(&questions)
  .bind(c.uid())
  .bind(updated_ms)
  .execute(&mut *tx)
  .await?;
  tx.commit().await?;
  Ok(json!({ "updated_ms": updated_ms }))
}

/// DELETE /banks/{id} (published versions keep their copy)
async fn delete_bank(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_build(&c)?;
  let bank = parse_uuid(id, BANK_LINK_MSG)?;
  ready(&state.pg_pool).await?;
  let done = sqlx::query("DELETE FROM carehub_question_bank WHERE id = $1 AND workspace_id = $2")
    .bind(bank)
    .bind(c.ws)
    .execute(&state.pg_pool)
    .await?;
  if done.rows_affected() == 0 {
    return Err(bank_missing());
  }
  Ok(json!({ "ok": true }))
}

// ---------------------------------------------------------------------------
// Bank questions in a course: freeze on publish, draw per attempt
// ---------------------------------------------------------------------------

/// A quiz question copied from a question bank (tagged with its bank's id).
pub(crate) fn from_bank(q: &Value) -> bool {
  matches!(q.get("bank_id"), Some(Value::String(s)) if !s.is_empty())
}

/// Indices into `all` (the version's quiz questions) of this attempt's bank
/// questions: per draw row in order, `count` of that bank's copies (a seeded
/// pick, kept in bank order). Empty when the quiz takes nothing from banks.
pub(crate) fn drawn(
  all: &[Value],
  content: &Value,
  course: Uuid,
  uid: i64,
  attempt: i32,
) -> Vec<usize> {
  let quiz = match content.get("quiz") {
    Some(q) => q,
    None => return Vec::new(),
  };
  let mut out: Vec<usize> = Vec::new();
  let mut seen: HashSet<&str> = HashSet::new();
  for d in list(quiz, "draws") {
    let bank = txt(d, "bank_id");
    if bank.is_empty() || !seen.insert(bank) {
      continue;
    }
    let mut idx: Vec<usize> = all
      .iter()
      .enumerate()
      .filter(|(_, q)| txt(q, "bank_id") == bank)
      .map(|(i, _)| i)
      .collect();
    let count = usize::try_from(num(d, "count", 0).max(0)).unwrap_or(0);
    if count < idx.len() {
      let seed = format!("{}:{}:{}:draw:{}", course, uid, attempt, bank);
      carehub_learning::shuffle(&mut idx, &seed);
      idx.truncate(count);
      idx.sort_unstable();
    }
    out.extend(idx);
  }
  out
}

/// Publish-check labels for a quiz's questions: own ones "Quiz question {n}",
/// copied ones "Question {k} in “{bank}”".
pub(crate) fn question_labels(quiz: &Value) -> Vec<String> {
  let titles: HashMap<&str, &str> = list(quiz, "draws")
    .iter()
    .map(|d| (txt(d, "bank_id"), txt(d, "title").trim()))
    .collect();
  let mut own = 0_usize;
  let mut per_bank: HashMap<&str, usize> = HashMap::new();
  let mut out: Vec<String> = Vec::new();
  for q in list(quiz, "questions") {
    let bank = txt(q, "bank_id");
    if bank.is_empty() {
      own += 1;
      out.push(format!("Quiz question {}", own));
    } else {
      let k = per_bank.entry(bank).or_insert(0);
      *k += 1;
      let title = match titles.get(bank) {
        Some(t) if !t.is_empty() => *t,
        _ => "a question bank",
      };
      out.push(format!("Question {} in “{}”", k, title));
    }
  }
  out
}

/// What publishing stores instead of the draft (when `content` is set) and
/// the problems found while copying the banks' questions.
pub(crate) struct Frozen {
  pub(crate) content: Option<Value>,
  pub(crate) text: String,
  pub(crate) problems: Vec<String>,
}

/// For an enabled quiz with draws: the draft with each drawn bank's current
/// questions appended to `quiz.questions` (tagged `bank_id`) and `quiz.draws`
/// as `[{bank_id, count, title, updated_ms}]`. Copies already in the draft
/// (e.g. after Restore) are dropped first. `content` is None when nothing
/// changes, so the draft publishes exactly as before.
pub(crate) async fn freeze_draws(
  conn: &mut PgConnection,
  pool: &PgPool,
  ws: Uuid,
  draft: &Value,
) -> Result<Frozen, AppError> {
  let mut frozen = Frozen {
    content: None,
    text: String::new(),
    problems: Vec::new(),
  };
  let quiz = match draft.get("quiz") {
    Some(q) if q.is_object() => q,
    _ => return Ok(frozen),
  };
  let questions = list(quiz, "questions");
  let draws = list(quiz, "draws");
  let use_draws = flag(quiz, "enabled", false) && !draws.is_empty();
  if !use_draws && !questions.iter().any(from_bank) && quiz.get(DRAFT_MD5).is_none() {
    return Ok(frozen);
  }
  let mut out_questions: Vec<Value> = questions
    .iter()
    .filter(|q| !from_bank(q))
    .cloned()
    .collect();
  let mut out_draws: Vec<Value> = Vec::new();
  if use_draws {
    if ready(pool).await.is_err() {
      frozen.problems.push(SETUP_MSG.to_string());
    } else {
      if draws.len() > MAX_DRAWS {
        frozen.problems.push(DRAWS_MANY_MSG.to_string());
      }
      let wanted: Vec<(Option<Uuid>, i64)> = draws
        .iter()
        .take(MAX_DRAWS)
        .map(|d| (Uuid::parse_str(txt(d, "bank_id").trim()).ok(), num(d, "count", 0)))
        .collect();
      let ids: Vec<Uuid> = wanted.iter().filter_map(|w| w.0).collect();
      let rows: Vec<(Uuid, String, String, i64)> = sqlx::query_as(
        "SELECT id, title, questions::text, updated_ms FROM carehub_question_bank \
         WHERE workspace_id = $1 AND id = ANY($2)",
      )
      .bind(ws)
      .bind(ids)
      .fetch_all(&mut *conn)
      .await?;
      let banks: HashMap<Uuid, (String, Vec<Value>, i64)> = rows
        .into_iter()
        .map(|(id, title, text, ms)| {
          let items = match serde_json::from_str::<Value>(&text) {
            Ok(Value::Array(items)) => items,
            _ => Vec::new(),
          };
          (id, (title, items, ms))
        })
        .collect();
      let mut seen: HashSet<Uuid> = HashSet::new();
      let mut gone = false;
      for (wanted_id, count) in wanted {
        let found = wanted_id.and_then(|id| banks.get(&id).map(|b| (id, b)));
        let (id, (title, items, ms)) = match found {
          Some(f) => f,
          None => {
            if !gone {
              gone = true;
              frozen.problems.push(BANK_GONE_MSG.to_string());
            }
            continue;
          },
        };
        if !seen.insert(id) {
          frozen.problems.push(format!(
            "The quiz takes questions from “{}” twice. Use one row for it.",
            title
          ));
          continue;
        }
        if count < 1 {
          frozen.problems.push(format!(
            "Choose how many questions the quiz takes from “{}” (at least 1).",
            title
          ));
        } else if count > items.len() as i64 {
          frozen.problems.push(format!(
            "The quiz wants {} questions from “{}”, but it only has {}.",
            count,
            title,
            items.len()
          ));
        }
        for q in items {
          let mut copy = match q {
            Value::Object(o) => o.clone(),
            _ => Map::new(),
          };
          copy.insert("bank_id".to_string(), json!(id.to_string()));
          out_questions.push(Value::Object(copy));
        }
        out_draws.push(json!({
          "bank_id": id.to_string(),
          "count": count,
          "title": title,
          "updated_ms": ms,
        }));
      }
    }
  }
  let mut out_quiz = quiz.clone();
  if let Some(o) = out_quiz.as_object_mut() {
    o.remove(DRAFT_MD5);
    o.insert("questions".to_string(), Value::Array(out_questions));
    if use_draws {
      o.insert("draws".to_string(), Value::Array(out_draws));
    }
  }
  let mut content = draft.clone();
  if let Some(o) = content.as_object_mut() {
    o.insert("quiz".to_string(), out_quiz);
  }
  frozen.text = content.to_string();
  frozen.content = Some(content);
  Ok(frozen)
}

/// Stores a version made by freeze_draws, with the draft's checksum in
/// `quiz.draft_md5` (the studio's "unpublished changes" compares that).
pub(crate) async fn insert_frozen(
  conn: &mut PgConnection,
  course: Uuid,
  version: i32,
  note: &str,
  by: i64,
  now: i64,
  text: &str,
) -> Result<(), AppError> {
  sqlx::query(
    "INSERT INTO carehub_course_version (course_id, version, content, note, published_by, published_ms) \
     SELECT id, $2, jsonb_set($6::jsonb, '{quiz,draft_md5}', to_jsonb(md5(draft::text))), $3, $4, $5 \
     FROM carehub_course WHERE id = $1",
  )
  .bind(course)
  .bind(version)
  .bind(note)
  .bind(by)
  .bind(now)
  .bind(text)
  .execute(&mut *conn)
  .await?;
  Ok(())
}

// ---------------------------------------------------------------------------
// Feedback for each answer
// ---------------------------------------------------------------------------

/// The picked options' own feedback `[{option_id, text}]` in option order
/// (empty ones left out); None for kinds without options.
pub(crate) fn option_feedback(q: &Value, answer: &Value) -> Option<Value> {
  let picked: Vec<&str> = match txt(q, "kind") {
    "matching" | "fill_blank" | "ordering" | "hotspot" => return None,
    "multiple" => match answer.as_array() {
      Some(items) => items.iter().filter_map(|x| x.as_str()).collect(),
      None => Vec::new(),
    },
    _ => answer.as_str().into_iter().collect(),
  };
  let out: Vec<Value> = list(q, "options")
    .iter()
    .filter(|o| {
      let id = txt(o, "id");
      !id.is_empty() && picked.contains(&id)
    })
    .filter_map(|o| {
      let text = txt(o, "feedback").trim();
      if text.is_empty() {
        None
      } else {
        Some(json!({ "option_id": txt(o, "id"), "text": text }))
      }
    })
    .collect();
  Some(Value::Array(out))
}

/// Extra checks for a quiz, bank or knowledge-check question.
pub(crate) fn check_question_extra(q: &Value, label: &str, problems: &mut Vec<String>) {
  if !matches!(txt(q, "kind"), "single" | "multiple" | "true_false") {
    return;
  }
  if list(q, "options")
    .iter()
    .any(|o| chars(txt(o, "feedback")) > MAX_FEEDBACK_CHARS)
  {
    problems.push(format!(
      "{} has answer feedback that is too long (300 characters at most).",
      label
    ));
  }
}

// ---------------------------------------------------------------------------
// Publish checks for the newer block kinds
// ---------------------------------------------------------------------------

/// Extra checks for one lesson block ({at} = `block 2 of Lesson 1 ("Welcome")`).
pub(crate) fn check_block_extra(b: &Value, at: &str, problems: &mut Vec<String>) {
  match txt(b, "type") {
    "video" => {
      let captions = txt(b, "captions_url");
      if !captions.trim().is_empty() && !carehub_learning::url_ok(captions) {
        problems.push(format!("The video in {} has a captions link that {}.", at, LINK_RULE));
      }
      if chars(txt(b, "captions_label")) > 40 {
        problems.push(format!(
          "The video in {} has a captions label that is too long (40 characters at most).",
          at
        ));
      }
      if chars(txt(b, "transcript")) > 20_000 {
        problems.push(format!(
          "The video in {} has a transcript that is too long (20,000 characters at most).",
          at
        ));
      }
    },
    "audio" => {
      if chars(txt(b, "transcript")) > 20_000 {
        problems.push(format!(
          "The audio in {} has a transcript that is too long (20,000 characters at most).",
          at
        ));
      }
    },
    "sorting" => check_sorting(b, at, problems),
    "timeline" => check_timeline(b, at, problems),
    "gallery" => check_gallery(b, at, problems),
    "table" => check_table(b, at, problems),
    "buttons" => check_buttons(b, at, problems),
    "survey" => check_survey(b, at, problems),
    _ => {},
  }
}

fn check_sorting(b: &Value, at: &str, problems: &mut Vec<String>) {
  let who = format!("The sorting activity in {}", at);
  let piles = list(b, "piles");
  let cards = list(b, "cards");
  if !(2..=4).contains(&piles.len()) {
    problems.push(format!("{} needs two to four groups.", who));
  }
  if piles.iter().any(|p| txt(p, "title").trim().is_empty()) {
    problems.push(format!("{} has a group without a name.", who));
  }
  if cards.len() < 2 {
    problems.push(format!("{} needs at least two cards.", who));
  } else if cards.len() > 20 {
    problems.push(format!("{} can have at most 20 cards.", who));
  }
  if cards.iter().any(|c| txt(c, "text").trim().is_empty()) {
    problems.push(format!("{} has an empty card.", who));
  }
  let pile_ids: HashSet<&str> = piles
    .iter()
    .map(|p| txt(p, "id").trim())
    .filter(|id| !id.is_empty())
    .collect();
  if cards
    .iter()
    .any(|c| !pile_ids.contains(txt(c, "pile_id").trim()))
  {
    problems.push(format!("{} has a card that isn't in a group.", who));
  }
  if piles.iter().any(|p| chars(txt(p, "title")) > 60)
    || cards.iter().any(|c| chars(txt(c, "text")) > 200)
  {
    problems.push(format!("{} has a group name or card that is too long.", who));
  }
  if bad_ids(piles.iter().map(|p| txt(p, "id"))) || bad_ids(cards.iter().map(|c| txt(c, "id"))) {
    problems.push(format!(
      "{} has groups or cards with missing or repeated ids. Try deleting and re-adding them.",
      who
    ));
  }
}

fn check_timeline(b: &Value, at: &str, problems: &mut Vec<String>) {
  let who = format!("The timeline in {}", at);
  let items = list(b, "items");
  if items.len() < 2 {
    problems.push(format!("{} needs at least two entries.", who));
  } else if items.len() > 20 {
    problems.push(format!("{} can have at most 20 entries.", who));
  }
  if items.iter().any(|it| txt(it, "title").trim().is_empty()) {
    problems.push(format!("{} has an entry without a title.", who));
  }
  if items
    .iter()
    .any(|it| chars(txt(it, "date")) > 100 || chars(txt(it, "title")) > 100)
  {
    problems.push(format!("{} has a date or title that is too long.", who));
  }
  let pictures: Vec<&Value> = items
    .iter()
    .filter(|it| !txt(it, "image_url").trim().is_empty())
    .collect();
  if pictures
    .iter()
    .any(|it| !carehub_learning::url_ok(txt(it, "image_url")))
  {
    problems.push(format!("{} has a picture link that {}.", who, LINK_RULE));
  }
  if pictures.iter().any(|it| txt(it, "image_alt").trim().is_empty()) {
    problems.push(format!("{} has a picture without a description.", who));
  }
}

fn check_gallery(b: &Value, at: &str, problems: &mut Vec<String>) {
  let who = format!("The gallery in {}", at);
  let images = list(b, "images");
  if images.len() < 2 {
    problems.push(format!("{} needs at least two pictures.", who));
  } else if images.len() > 12 {
    problems.push(format!("{} can have at most 12 pictures.", who));
  }
  if images.iter().any(|im| txt(im, "url").trim().is_empty()) {
    problems.push(format!("{} has a picture without a link.", who));
  }
  if images.iter().any(|im| {
    let url = txt(im, "url");
    !url.trim().is_empty() && !carehub_learning::url_ok(url)
  }) {
    problems.push(format!("{} has a picture link that {}.", who, LINK_RULE));
  }
  if images.iter().any(|im| txt(im, "alt").trim().is_empty()) {
    problems.push(format!("{} has a picture without a description.", who));
  }
}

fn check_table(b: &Value, at: &str, problems: &mut Vec<String>) {
  let who = format!("The table in {}", at);
  let rows = list(b, "rows");
  if rows.is_empty() {
    problems.push(format!("{} needs at least one row.", who));
    return;
  }
  let widths: Vec<usize> = rows
    .iter()
    .map(|r| r.as_array().map(|cells| cells.len()).unwrap_or(0))
    .collect();
  if rows.len() > 30 || widths.iter().any(|w| *w > 6) {
    problems.push(format!("{} can have at most 30 rows and 6 columns.", who));
  }
  let first = widths.first().copied().unwrap_or(0);
  if widths.iter().any(|w| *w != first) {
    problems.push(format!("{} has rows of different lengths.", who));
  }
  let cells: Vec<&str> = rows
    .iter()
    .filter_map(|r| r.as_array())
    .flat_map(|cells| cells.iter().map(|x| x.as_str().unwrap_or("")))
    .collect();
  if cells.iter().any(|t| chars(t) > 300) {
    problems.push(format!(
      "{} has a cell that is too long (300 characters at most).",
      who
    ));
  }
  if cells.iter().all(|t| t.trim().is_empty()) {
    problems.push(format!("{} is empty.", who));
  }
}

fn check_buttons(b: &Value, at: &str, problems: &mut Vec<String>) {
  let items = list(b, "items");
  if items.is_empty() {
    problems.push(format!("The buttons block in {} needs at least one button.", at));
  } else if items.len() > 4 {
    problems.push(format!("The buttons block in {} can have at most 4 buttons.", at));
  }
  for item in items {
    let label = txt(item, "label").trim();
    if label.is_empty() {
      push_once(problems, format!("A button in {} needs a label.", at));
    } else if chars(label) > 60 {
      push_once(
        problems,
        format!("A button label in {} is too long (60 characters at most).", at),
      );
    }
    let who = if label.is_empty() {
      format!("A button in {}", at)
    } else {
      let shown: String = label.chars().take(MAX_LABEL_SHOWN).collect();
      let dots = if chars(label) > MAX_LABEL_SHOWN { "…" } else { "" };
      format!("The button “{}{}” in {}", shown, dots, at)
    };
    let none = Value::Null;
    let target = item.get("target").unwrap_or(&none);
    let problem = match txt(target, "kind") {
      "link" => {
        let url = txt(target, "url");
        if url.trim().is_empty() || !carehub_learning::url_ok(url) {
          Some(format!(
            "{} needs a link that starts with https:// (or a Care Hub link starting with /).",
            who
          ))
        } else {
          None
        }
      },
      "file" => {
        let url = txt(target, "url");
        if url.trim().is_empty() || !carehub_learning::url_ok(url) {
          Some(format!("{} needs a file.", who))
        } else {
          None
        }
      },
      "page" => {
        if txt(target, "view_id").trim().is_empty() {
          Some(format!("{} needs a Care Hub page.", who))
        } else {
          None
        }
      },
      // A lesson that isn't in the course any more is a link problem (below).
      "lesson" if !txt(target, "lesson_id").trim().is_empty() => None,
      _ => Some(format!("{} doesn't go anywhere yet.", who)),
    };
    if let Some(p) = problem {
      push_once(problems, p);
    }
  }
}

fn check_survey(b: &Value, at: &str, problems: &mut Vec<String>) {
  let who = format!("The survey in {}", at);
  let questions = list(b, "questions");
  if chars(txt(b, "title")) > 120 {
    problems.push(format!(
      "{} has a title that is too long (120 characters at most).",
      who
    ));
  }
  if questions.is_empty() {
    problems.push(format!("{} needs at least one question.", who));
  } else if questions.len() > 10 {
    problems.push(format!("{} can have at most 10 questions.", who));
  }
  let mut broken_ids = bad_ids(questions.iter().map(|q| txt(q, "id")));
  for q in questions {
    let prompt = txt(q, "prompt");
    if prompt.trim().is_empty() {
      push_once(problems, format!("{} has an empty question.", who));
    } else if chars(prompt) > 300 {
      push_once(
        problems,
        format!("{} has a question that is too long (300 characters at most).", who),
      );
    }
    match txt(q, "kind") {
      "rating" | "text" => {},
      "choice" => {
        let options = list(q, "options");
        if !(2..=10).contains(&options.len()) {
          push_once(
            problems,
            format!("{} has a choice question that needs two to ten choices.", who),
          );
        }
        if options.iter().any(|o| txt(o, "text").trim().is_empty()) {
          push_once(problems, format!("{} has an empty choice.", who));
        }
        if options.iter().any(|o| chars(txt(o, "text")) > 200) {
          push_once(
            problems,
            format!("{} has a choice that is too long (200 characters at most).", who),
          );
        }
        if bad_ids(options.iter().map(|o| txt(o, "id"))) {
          broken_ids = true;
        }
      },
      _ => push_once(problems, format!("{} has a question of an unknown type.", who)),
    }
  }
  if broken_ids {
    problems.push(format!(
      "{} has questions or choices with missing or repeated ids. Try deleting and re-adding them.",
      who
    ));
  }
}

/// `](#lesson-<id>)` / `](#block-<id>)` links in a text (ids [A-Za-z0-9_-]+).
fn text_links(text: &str, out: &mut Vec<(bool, String)>) {
  for (marker, is_lesson) in [("](#lesson-", true), ("](#block-", false)] {
    let mut rest = text;
    while let Some(pos) = rest.find(marker) {
      rest = rest.get(pos + marker.len()..).unwrap_or("");
      let id: String = rest
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '-')
        .collect();
      let closed = rest.get(id.len()..).is_some_and(|r| r.starts_with(')'));
      if !id.is_empty() && closed {
        out.push((is_lesson, id));
      }
    }
  }
}

fn collect_links(v: &Value, out: &mut Vec<(bool, String)>) {
  match v {
    Value::String(s) => text_links(s, out),
    Value::Array(items) => {
      for x in items {
        collect_links(x, out);
      }
    },
    Value::Object(o) => {
      for x in o.values() {
        collect_links(x, out);
      }
    },
    _ => {},
  }
}

/// Buttons with a lesson target and text links to a lesson or block must point
/// at something in this course (one sentence per lesson).
pub(crate) fn link_problems(c: &Value, problems: &mut Vec<String>) {
  let lessons = list(c, "lessons");
  let lesson_ids: HashSet<&str> = lessons
    .iter()
    .map(|l| txt(l, "id"))
    .filter(|id| !id.is_empty())
    .collect();
  let block_ids: HashSet<&str> = lessons
    .iter()
    .flat_map(|l| list(l, "blocks").iter().map(|b| txt(b, "id")))
    .filter(|id| !id.is_empty())
    .collect();
  for (li, lesson) in lessons.iter().enumerate() {
    let mut broken = false;
    for b in list(lesson, "blocks") {
      if txt(b, "type") == "buttons" {
        for item in list(b, "items") {
          let none = Value::Null;
          let target = item.get("target").unwrap_or(&none);
          let lid = txt(target, "lesson_id").trim();
          if txt(target, "kind") != "lesson" || lid.is_empty() {
            continue;
          }
          let bid = txt(target, "block_id").trim();
          match lessons.iter().find(|l| txt(l, "id") == lid) {
            Some(l) => {
              if !bid.is_empty() && !list(l, "blocks").iter().any(|x| txt(x, "id") == bid) {
                broken = true;
              }
            },
            None => broken = true,
          }
        }
      }
      let mut links: Vec<(bool, String)> = Vec::new();
      collect_links(b, &mut links);
      if links.iter().any(|(is_lesson, id)| {
        if *is_lesson {
          !lesson_ids.contains(id.as_str())
        } else {
          !block_ids.contains(id.as_str())
        }
      }) {
        broken = true;
      }
    }
    if broken {
      problems.push(format!(
        "A link in {} goes to a lesson or block that's no longer in this course.",
        carehub_learning::lesson_name(li, lesson)
      ));
    }
  }
}

// ---------------------------------------------------------------------------
// Surveys
// ---------------------------------------------------------------------------

fn scale_of(q: &Value) -> i64 {
  if num(q, "scale", 5) == 10 { 10 } else { 5 }
}

/// The answers worth keeping, or the reason the set can't be saved.
fn clean_answers(
  block: &Value,
  given: &Map<String, Value>,
) -> Result<Map<String, Value>, &'static str> {
  let mut out = Map::new();
  for q in list(block, "questions") {
    let qid = txt(q, "id");
    let answer = match given.get(qid) {
      Some(a) if !qid.is_empty() && !a.is_null() && a.as_str() != Some("") => a,
      _ => continue,
    };
    match txt(q, "kind") {
      "rating" => {
        let n = answer
          .as_i64()
          .or_else(|| answer.as_f64().filter(|f| f.fract() == 0.0).map(|f| f as i64));
        match n {
          Some(n) if (1..=scale_of(q)).contains(&n) => {
            out.insert(qid.to_string(), json!(n));
          },
          _ => return Err(SURVEY_CHANGED_MSG),
        }
      },
      "choice" => {
        let pick = answer.as_str().unwrap_or("");
        if pick.is_empty() || !list(q, "options").iter().any(|o| txt(o, "id") == pick) {
          return Err(SURVEY_CHANGED_MSG);
        }
        out.insert(qid.to_string(), json!(pick));
      },
      "text" => {
        let text = match answer.as_str() {
          Some(s) => clean_line(s),
          None => return Err(SURVEY_CHANGED_MSG),
        };
        if text.is_empty() {
          continue;
        }
        if chars(&text) > MAX_SURVEY_TEXT_CHARS {
          return Err(SURVEY_LONG_MSG);
        }
        out.insert(qid.to_string(), json!(text));
      },
      _ => {},
    }
  }
  Ok(out)
}

/// A `survey_answer` progress event: saves the person's answers to a survey
/// block of `lesson` (replacing earlier ones). Ok(Some(reason)) = rejected.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn save_survey(
  conn: &mut PgConnection,
  pool: &PgPool,
  ws: Uuid,
  course: Uuid,
  uid: i64,
  version: i32,
  lesson: &Value,
  block_id: &str,
  data: Option<&Value>,
  now: i64,
) -> Result<Option<String>, AppError> {
  let block = list(lesson, "blocks")
    .iter()
    .find(|b| !block_id.is_empty() && txt(b, "id") == block_id && txt(b, "type") == "survey");
  let block = match block {
    Some(b) => b,
    None => return Ok(Some(UNKNOWN_BLOCK_MSG.to_string())),
  };
  let given = match data.and_then(|d| d.get("answers")).and_then(|a| a.as_object()) {
    Some(a) => a,
    None => return Ok(Some(SURVEY_EMPTY_MSG.to_string())),
  };
  let answers = match clean_answers(block, given) {
    Ok(a) if a.is_empty() => return Ok(Some(SURVEY_EMPTY_MSG.to_string())),
    Ok(a) => a,
    Err(reason) => return Ok(Some(reason.to_string())),
  };
  if ready(pool).await.is_err() {
    return Ok(Some(SETUP_MSG.to_string()));
  }
  sqlx::query(
    "INSERT INTO carehub_survey_answer \
       (course_id, block_id, uid, workspace_id, version, lesson_id, anonymous, answers, \
        created_ms, updated_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $9) \
     ON CONFLICT (course_id, block_id, uid) DO UPDATE SET \
       workspace_id = EXCLUDED.workspace_id, version = EXCLUDED.version, \
       lesson_id = EXCLUDED.lesson_id, anonymous = EXCLUDED.anonymous, \
       answers = EXCLUDED.answers, updated_ms = EXCLUDED.updated_ms",
  )
  .bind(course)
  .bind(block_id)
  .bind(uid)
  .bind(ws)
  .bind(version)
  .bind(txt(lesson, "id"))
  .bind(flag(block, "anonymous", false))
  .bind(Value::Object(answers).to_string())
  .bind(now)
  .execute(&mut *conn)
  .await?;
  Ok(None)
}

/// The caller's own saved survey answers for play: `{block_id: {answers,
/// updated_ms}}` (`{}` if none, or if they can't be read).
pub(crate) async fn my_surveys(pool: &PgPool, course: Uuid, uid: i64) -> Value {
  let mut out = Map::new();
  if ready(pool).await.is_err() {
    return Value::Object(out);
  }
  let rows: Result<Vec<(String, String, i64)>, sqlx::Error> = sqlx::query_as(
    "SELECT block_id, answers::text, updated_ms FROM carehub_survey_answer \
     WHERE course_id = $1 AND uid = $2 LIMIT 1000",
  )
  .bind(course)
  .bind(uid)
  .fetch_all(pool)
  .await;
  match rows {
    Ok(rows) => {
      for (block_id, text, updated_ms) in rows {
        let answers: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({}));
        out.insert(block_id, json!({ "answers": answers, "updated_ms": updated_ms }));
      }
    },
    Err(err) => tracing::warn!("carehub learning bank: could not read survey answers: {}", err),
  }
  Value::Object(out)
}

/// One saved set of answers, as the report reads it.
struct SurveySet {
  uid: i64,
  anonymous: bool,
  answers: Value,
  ms: i64,
  name: String,
}

fn rating_result(q: &Value, sets: &[SurveySet]) -> Value {
  let qid = txt(q, "id");
  let scale = scale_of(q);
  let mut counts: Vec<i64> = vec![0; scale as usize];
  let mut total: i64 = 0;
  for s in sets {
    if let Some(n) = s.answers.get(qid).and_then(|x| x.as_i64()) {
      if (1..=scale).contains(&n) {
        if let Some(slot) = counts.get_mut((n - 1) as usize) {
          *slot += 1;
          total += n;
        }
      }
    }
  }
  let answered: i64 = counts.iter().sum();
  let average = if answered > 0 {
    json!((total as f64 / answered as f64 * 10.0).round() / 10.0)
  } else {
    Value::Null
  };
  json!({
    "id": qid,
    "kind": "rating",
    "prompt": txt(q, "prompt"),
    "scale": scale,
    "answered": answered,
    "average": average,
    "counts": counts,
  })
}

fn choice_result(q: &Value, sets: &[SurveySet]) -> Value {
  let qid = txt(q, "id");
  let options = list(q, "options");
  let mut picked: Vec<i64> = vec![0; options.len()];
  for s in sets {
    let pick = s.answers.get(qid).and_then(|x| x.as_str()).unwrap_or("");
    if pick.is_empty() {
      continue;
    }
    if let Some(pos) = options.iter().position(|o| txt(o, "id") == pick) {
      if let Some(slot) = picked.get_mut(pos) {
        *slot += 1;
      }
    }
  }
  let answered: i64 = picked.iter().sum();
  let options: Vec<Value> = options
    .iter()
    .zip(picked.iter())
    .map(|(o, n)| json!({ "id": txt(o, "id"), "text": txt(o, "text"), "picked": n }))
    .collect();
  json!({
    "id": qid,
    "kind": "choice",
    "prompt": txt(q, "prompt"),
    "answered": answered,
    "options": options,
  })
}

/// Text answers: named ones newest first, then anonymous ones (no uid, name
/// or date) A-Z so their order can't be matched to activity times.
fn text_result(q: &Value, sets: &[SurveySet], anonymous_now: bool) -> Value {
  let qid = txt(q, "id");
  let mut named: Vec<Value> = Vec::new();
  let mut hidden: Vec<String> = Vec::new();
  for s in sets {
    let text = s.answers.get(qid).and_then(|x| x.as_str()).unwrap_or("").trim();
    if text.is_empty() {
      continue;
    }
    if anonymous_now || s.anonymous {
      hidden.push(text.to_string());
    } else {
      named.push(json!({
        "text": text,
        "uid": s.uid.to_string(),
        "name": s.name,
        "ms": s.ms,
      }));
    }
  }
  hidden.sort_by_key(|t| (t.to_lowercase(), t.clone()));
  let answered = named.len() + hidden.len();
  let mut answers = named;
  answers.extend(hidden.into_iter().map(|t| json!({ "text": t })));
  answers.truncate(MAX_TEXT_ANSWERS);
  json!({
    "id": qid,
    "kind": "text",
    "prompt": txt(q, "prompt"),
    "answered": answered,
    "answers": answers,
  })
}

/// GET /reports/courses/{id}/surveys
async fn survey_report(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  need_reports(&c)?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let head: Option<(Option<String>,)> = sqlx::query_as(
    "SELECT (v.content->'lessons')::text FROM carehub_course c \
     LEFT JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.id = $1 AND c.workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (lessons_text,) = head.ok_or_else(|| AppError::RecordNotFound(MISSING_MSG.to_string()))?;
  let lessons: Value = lessons_text
    .and_then(|t| serde_json::from_str::<Value>(&t).ok())
    .filter(|v| v.is_array())
    .unwrap_or_else(|| json!([]));
  let mut found: Vec<(&Value, &Value)> = Vec::new();
  for lesson in lessons.as_array().map(|a| a.as_slice()).unwrap_or(&[]) {
    for b in list(lesson, "blocks") {
      if txt(b, "type") == "survey" && !txt(b, "id").is_empty() {
        found.push((lesson, b));
      }
    }
  }
  if found.is_empty() {
    return Ok(json!({ "surveys": [] }));
  }
  ready(&state.pg_pool).await?;
  let ids: Vec<String> = found.iter().map(|(_, b)| txt(b, "id").to_string()).collect();
  let rows: Vec<(String, i64, bool, String, i64, Option<String>)> = sqlx::query_as(
    "SELECT a.block_id, a.uid, a.anonymous, a.answers::text, a.updated_ms, \
       (SELECT COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
        FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
        WHERE m.workspace_id = $3 AND m.uid = a.uid \
          AND COALESCE(u.email, '') NOT LIKE '%.removed-%' LIMIT 1) \
     FROM carehub_survey_answer a WHERE a.course_id = $1 AND a.block_id = ANY($2) \
     ORDER BY a.updated_ms DESC LIMIT 20000",
  )
  .bind(course)
  .bind(ids)
  .bind(c.ws)
  .fetch_all(&state.pg_pool)
  .await?;
  let mut by_block: HashMap<String, Vec<SurveySet>> = HashMap::new();
  for (block_id, uid, anonymous, text, ms, name) in rows {
    let name = name
      .filter(|n| !n.trim().is_empty())
      .unwrap_or_else(|| FORMER_MEMBER.to_string());
    by_block.entry(block_id).or_default().push(SurveySet {
      uid,
      anonymous,
      answers: serde_json::from_str(&text).unwrap_or_else(|_| json!({})),
      ms,
      name,
    });
  }
  let none: Vec<SurveySet> = Vec::new();
  let surveys: Vec<Value> = found
    .iter()
    .map(|(lesson, b)| {
      let sets = by_block.get(txt(b, "id")).unwrap_or(&none);
      let anonymous_now = flag(b, "anonymous", false);
      let questions: Vec<Value> = list(b, "questions")
        .iter()
        .filter_map(|q| match txt(q, "kind") {
          "rating" => Some(rating_result(q, sets)),
          "choice" => Some(choice_result(q, sets)),
          "text" => Some(text_result(q, sets, anonymous_now)),
          _ => None,
        })
        .collect();
      json!({
        "block_id": txt(b, "id"),
        "lesson_id": txt(lesson, "id"),
        "lesson_title": txt(lesson, "title"),
        "title": txt(b, "title"),
        "anonymous": anonymous_now,
        "answered": sets.len(),
        "questions": questions,
      })
    })
    .collect();
  Ok(json!({ "surveys": surveys }))
}

// ---------------------------------------------------------------------------
// HTTP handlers and routes
// ---------------------------------------------------------------------------

async fn banks_list_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
) -> Reply {
  let ws = path.into_inner();
  reply(list_banks(&state, &user, &ws).await)
}

async fn banks_create_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
  body: Bytes,
) -> Reply {
  let ws = path.into_inner();
  reply(create_bank(&state, &user, &ws, &body).await)
}

async fn bank_get_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(get_bank(&state, &user, &ws, &id).await)
}

async fn bank_save_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(save_bank(&state, &user, &ws, &id, &body).await)
}

async fn bank_delete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(delete_bank(&state, &user, &ws, &id).await)
}

async fn survey_report_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(survey_report(&state, &user, &ws, &id).await)
}

/// Called once from the end of `carehub_learning::configure`.
pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/banks")
        .app_data(web::PayloadConfig::new(BANK_BODY_BYTES))
        .route(web::get().to(banks_list_handler))
        .route(web::post().to(banks_create_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/banks/{id}")
        .app_data(web::PayloadConfig::new(BANK_BODY_BYTES))
        .route(web::get().to(bank_get_handler))
        .route(web::put().to(bank_save_handler))
        .route(web::delete().to(bank_delete_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/courses/{id}/surveys")
        .route(web::get().to(survey_report_handler)),
    );
}
