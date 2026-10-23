//! Care Hub Learning, Phase 2: assignments, groups, reminders, due-date
//! calendar entries, certificates and the insights / teams reports.
//!
//! Routes sit next to Phase 1 under `/api/carehub/ws/{ws}/learning/...` (see
//! carehub_learning.rs); everything here is additive and Phase 1 behaves
//! exactly as before. Tables are created on first use; a failure is logged and
//! turned into a friendly message, the server keeps running.
//!
//! A background task (every minute) re-syncs role / everyone / group targets,
//! sends reminders to each person's "🔔 Care Hub" chat (claim before send, so
//! nothing is sent twice), keeps due-date calendar entries in step and issues
//! certificates. Request handlers run the same reminder / calendar pass for
//! the courses they touched, right after they commit.

use crate::api::carehub_calendar;
use crate::api::carehub_extras;
use crate::api::carehub_learning::{self, CardRow, Ctx, MyRow};
use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Bytes, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use database_entity::dto::AFRole;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use sqlx::{PgConnection, PgPool};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Mutex;
use uuid::Uuid;

static READY: AtomicBool = AtomicBool::new(false);
static LAST_WARN_MS: AtomicI64 = AtomicI64::new(0);
/// settings.certificate per published version (versions never change).
static CERT_FLAGS: Mutex<BTreeMap<(Uuid, i32), bool>> = Mutex::new(BTreeMap::new());

const TABLE_LOCK_KEY: i64 = 7_316_402_220;
const WS_LOCK_CLASS: i32 = 7_316_403;
const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;
/// Reminders go out at 09:00 in the assignment's time zone.
const REMIND_HOUR: i64 = 9;
const MAX_TARGETS: usize = 200;
const MAX_NOTE_CHARS: usize = 500;
const MAX_GROUP_NAME_CHARS: usize = 80;
const MAX_GROUP_DESCRIPTION_CHARS: usize = 500;
const MAX_GROUP_PEOPLE: usize = 1000;
/// 2200-01-01; anything later is a typo.
const MAX_DUE_MS: i64 = 7_258_118_400_000;
const TICK_PAIRS: i64 = 500;
const CERT_BATCH: i64 = 500;
const CALENDAR_BATCH: usize = 1000;
const QUIZ_ATTEMPTS_CAP: i64 = 5000;
/// No 0/O/1/I/L, so codes can be read out and typed without mistakes.
const CODE_ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";

const SETUP_MSG: &str = "Learning is still being set up. Please try again in a minute.";
const SERVER_MSG: &str = "Something went wrong on the server. Please try again in a moment.";
const BODY_MSG: &str = "That request couldn't be read. Please reload the page and try again.";
const COURSE_LINK_MSG: &str = "That course link doesn't look right.";
const MISSING_MSG: &str = "That course couldn't be found. It may have been deleted.";
const ASSIGN_MSG: &str =
  "You need the \"Assign courses\" permission for this. Ask an admin to update your role.";
const READ_MSG: &str = "You need the \"Assign courses\", \"Build courses\" or \"See course reports\" permission for this. Ask an admin to update your role.";
const REPORTS_MSG: &str =
  "You need the \"See course reports\" permission for this. Ask an admin to update your role.";
const GROUP_NAME_MSG: &str = "Give the group a name (up to 80 characters).";
const GROUP_LINK_MSG: &str = "That group link doesn't look right.";
const GROUP_MISSING_MSG: &str = "That group couldn't be found. It may have been deleted.";
const GROUP_TOO_BIG_MSG: &str = "A group can have up to 1000 people.";
const ASSIGNMENT_LINK_MSG: &str = "That assignment link doesn't look right.";
const ASSIGNMENT_MISSING_MSG: &str =
  "That assignment couldn't be found. It may have been removed.";
const NOT_PUBLISHED_MSG: &str = "Publish this course before assigning it.";
const ARCHIVED_MSG: &str = "This course is archived. Unarchive it before assigning it.";
const PAST_DUE_MSG: &str = "Pick a due date in the future.";
const BAD_DUE_MSG: &str = "That due date doesn't look right.";
const NOTE_MSG: &str = "Keep the note to 500 characters or fewer.";
const NO_TARGETS_MSG: &str = "Pick at least one person, role or group.";
const TOO_MANY_TARGETS_MSG: &str = "You can assign to up to 200 people, roles or groups at a time.";
const UNKNOWN_TARGETS_MSG: &str =
  "Those people, roles or groups couldn't be found. Please reload the page and try again.";
const CERT_MISSING_MSG: &str = "We couldn't find a certificate with that code.";

const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS: [&str; 12] = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

const TABLES: &[&str] = &[
  "CREATE TABLE IF NOT EXISTS carehub_learning_group (\
     id UUID PRIMARY KEY, workspace_id UUID NOT NULL, name TEXT NOT NULL, \
     description TEXT NOT NULL DEFAULT '', created_by BIGINT NOT NULL DEFAULT 0, \
     created_ms BIGINT NOT NULL DEFAULT 0, updated_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE UNIQUE INDEX IF NOT EXISTS carehub_learning_group_name \
     ON carehub_learning_group (workspace_id, LOWER(name))",
  "CREATE TABLE IF NOT EXISTS carehub_learning_group_member (\
     group_id UUID NOT NULL REFERENCES carehub_learning_group (id) ON DELETE CASCADE, \
     uid BIGINT NOT NULL, added_ms BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (group_id, uid))",
  "CREATE INDEX IF NOT EXISTS carehub_learning_group_member_uid \
     ON carehub_learning_group_member (uid)",
  "CREATE TABLE IF NOT EXISTS carehub_learning_assignment (\
     id UUID PRIMARY KEY, workspace_id UUID NOT NULL, \
     course_id UUID NOT NULL REFERENCES carehub_course (id) ON DELETE CASCADE, \
     target_kind TEXT NOT NULL, target_id TEXT NOT NULL DEFAULT '', due_ms BIGINT, \
     tz_minutes INT NOT NULL DEFAULT 0, required BOOLEAN NOT NULL DEFAULT FALSE, \
     note TEXT NOT NULL DEFAULT '', calendar BOOLEAN NOT NULL DEFAULT TRUE, \
     notify BOOLEAN NOT NULL DEFAULT TRUE, created_by BIGINT NOT NULL DEFAULT 0, \
     created_ms BIGINT NOT NULL DEFAULT 0, updated_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE UNIQUE INDEX IF NOT EXISTS carehub_learning_assignment_target \
     ON carehub_learning_assignment (course_id, target_kind, target_id)",
  "CREATE INDEX IF NOT EXISTS carehub_learning_assignment_ws \
     ON carehub_learning_assignment (workspace_id, created_ms)",
  "CREATE TABLE IF NOT EXISTS carehub_learning_assignee (\
     assignment_id UUID NOT NULL REFERENCES carehub_learning_assignment (id) ON DELETE CASCADE, \
     uid BIGINT NOT NULL, course_id UUID NOT NULL, workspace_id UUID NOT NULL, \
     assigned_ms BIGINT NOT NULL DEFAULT 0, sent_assigned_ms BIGINT NOT NULL DEFAULT 0, \
     sent_due_ms BIGINT NOT NULL DEFAULT 0, sent_overdue_ms BIGINT NOT NULL DEFAULT 0, \
     PRIMARY KEY (assignment_id, uid))",
  "CREATE INDEX IF NOT EXISTS carehub_learning_assignee_person \
     ON carehub_learning_assignee (uid, course_id)",
  "CREATE INDEX IF NOT EXISTS carehub_learning_assignee_course \
     ON carehub_learning_assignee (course_id, uid)",
  "CREATE TABLE IF NOT EXISTS carehub_learning_certificate (\
     code TEXT PRIMARY KEY, workspace_id UUID NOT NULL, course_id UUID NOT NULL, \
     uid BIGINT NOT NULL, version INT NOT NULL DEFAULT 0, title TEXT NOT NULL DEFAULT '', \
     person_name TEXT NOT NULL DEFAULT '', score INT, issued_ms BIGINT NOT NULL)",
  "CREATE UNIQUE INDEX IF NOT EXISTS carehub_learning_certificate_finish \
     ON carehub_learning_certificate (course_id, uid, issued_ms)",
  "CREATE INDEX IF NOT EXISTS carehub_learning_certificate_person \
     ON carehub_learning_certificate (workspace_id, uid, issued_ms)",
  "CREATE TABLE IF NOT EXISTS carehub_learning_calendar (\
     course_id UUID NOT NULL, uid BIGINT NOT NULL, workspace_id UUID NOT NULL, \
     event_id UUID NOT NULL, start_ms BIGINT NOT NULL, updated_ms BIGINT NOT NULL DEFAULT 0, \
     PRIMARY KEY (course_id, uid))",
];

/// Creates the Phase 2 tables once (after the Phase 1 ones they point at).
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
      tracing::warn!("carehub learning assign: could not create tables: {}", err);
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
      tracing::warn!("carehub learning assign: {}", other);
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

fn group_missing() -> AppError {
  AppError::RecordNotFound(GROUP_MISSING_MSG.to_string())
}

fn assignment_missing() -> AppError {
  AppError::RecordNotFound(ASSIGNMENT_MISSING_MSG.to_string())
}

fn cert_missing() -> AppError {
  AppError::RecordNotFound(CERT_MISSING_MSG.to_string())
}

/// Background problems are logged at most every ten minutes.
fn warn_rarely(what: &str) {
  let now = now_ms();
  let last = LAST_WARN_MS.load(Ordering::Relaxed);
  if now - last >= 600_000 {
    LAST_WARN_MS.store(now, Ordering::Relaxed);
    tracing::warn!("carehub learning assign: {}", what);
  }
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

fn clip(s: &str, max: usize) -> String {
  s.chars().take(max).collect()
}

fn guest_role() -> i32 {
  AFRole::Guest.into()
}

fn uid_of(v: Option<&Value>) -> Option<i64> {
  match v {
    Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
    Some(other) => other.as_i64(),
    None => None,
  }
}

fn finished(status: &str) -> bool {
  status == "completed" || status == "passed"
}

fn pct(part: i64, whole: i64) -> i64 {
  if whole <= 0 {
    0
  } else {
    ((part as f64) * 100.0 / (whole as f64)).round() as i64
  }
}

fn kind_of(q: &Value) -> &'static str {
  match txt(q, "kind") {
    "multiple" => "multiple",
    "true_false" => "true_false",
    "matching" => "matching",
    _ => "single",
  }
}

/// Serialises writes to assignments / groups of one workspace (requests and
/// the background re-sync), for the rest of the transaction.
async fn lock_ws(conn: &mut PgConnection, ws: Uuid) -> Result<(), AppError> {
  sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
    .bind(WS_LOCK_CLASS)
    .bind(ws.to_string())
    .execute(&mut *conn)
    .await?;
  Ok(())
}

fn course_path(course: Uuid) -> String {
  format!("/app/learning/course/{}", course)
}

/// Chat messages get a full link (Messages turns plain https links into
/// links); without a public address the path still tells people where to go.
fn course_link(course: Uuid) -> String {
  let base = std::env::var("APPFLOWY_WEB_URL").unwrap_or_default();
  let base = base.trim().trim_end_matches('/');
  if base.starts_with("https://") || base.starts_with("http://") {
    format!("{}{}", base, course_path(course))
  } else {
    course_path(course)
  }
}

/// Day number (days since 1970-01-01) of a moment in a time zone given in
/// minutes east of UTC.
fn local_day(ms: i64, tz: i32) -> i64 {
  (ms + i64::from(tz) * 60_000).div_euclid(DAY_MS)
}

/// UTC ms of local midnight that starts day `day`.
fn day_start(day: i64, tz: i32) -> i64 {
  day * DAY_MS - i64::from(tz) * 60_000
}

/// (year, month 1-12, day 1-31) for a day number.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
  let z = days + 719_468;
  let era = z.div_euclid(146_097);
  let doe = z.rem_euclid(146_097);
  let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
  let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  let mp = (5 * doy + 2) / 153;
  let d = doy - (153 * mp + 2) / 5 + 1;
  let m = if mp < 10 { mp + 3 } else { mp - 9 };
  let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
  (y, m, d)
}

/// "Fri 30 Oct".
fn day_label(day: i64) -> String {
  let weekday = usize::try_from((day + 4).rem_euclid(7))
    .ok()
    .and_then(|i| WEEKDAYS.get(i).copied())
    .unwrap_or("");
  let (_, month, d) = civil_from_days(day);
  let month = usize::try_from(month - 1)
    .ok()
    .and_then(|i| MONTHS.get(i).copied())
    .unwrap_or("");
  format!("{} {} {}", weekday, d, month)
}

// ---------------------------------------------------------------------------
// Who is calling
// ---------------------------------------------------------------------------

/// Phase 1 checks (signed-in, non-guest member, tables ready) plus our tables.
async fn ctx(state: &AppState, user: &UserUuid, ws: &str) -> Result<Ctx, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  ready(&state.pg_pool).await?;
  Ok(c)
}

fn need_assign(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_assign") {
    Ok(())
  } else {
    Err(bad(ASSIGN_MSG))
  }
}

fn need_read(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_assign") || c.has("learning_build") || c.has("learning_analytics") {
    Ok(())
  } else {
    Err(bad(READ_MSG))
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
// Members, roles, groups, courses, assignments
// ---------------------------------------------------------------------------

struct Person {
  uid: i64,
  name: String,
  email: String,
  role_id: String,
  role_name: String,
}

/// Current non-guest members with their Care Hub role, plus the role list.
struct People {
  list: Vec<Person>,
  index: HashMap<i64, usize>,
  roles: Vec<Value>,
}

impl People {
  fn has(&self, uid: i64) -> bool {
    self.index.contains_key(&uid)
  }

  fn get(&self, uid: i64) -> Option<&Person> {
    self.index.get(&uid).and_then(|i| self.list.get(*i))
  }
}

async fn people_of(pool: &PgPool, ws: Uuid) -> Result<People, AppError> {
  carehub_extras::ensure_tables(pool).await?;
  let rows: Vec<(i64, String, String, i32)> = sqlx::query_as(
    "SELECT m.uid, COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), \
       COALESCE(u.email, ''), m.role_id::INT \
     FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
     WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
       AND COALESCE(u.email, '') NOT LIKE '%.removed-%' \
     ORDER BY 2, 1",
  )
  .bind(ws)
  .bind(guest_role())
  .fetch_all(pool)
  .await?;
  let cfg = carehub_extras::get_setting(pool, ws, "roles").await?;
  let roles = carehub_extras::roles_list(&cfg);
  let mut list = Vec::with_capacity(rows.len());
  for (uid, name, email, af_role) in rows {
    let email = email.trim().to_string();
    let wanted = carehub_extras::resolve_role_id(&cfg, &email.to_lowercase(), af_role);
    let (role_id, role_name, _) = carehub_extras::role_info(&roles, &wanted)
      .or_else(|| carehub_extras::role_info(&roles, "editor"))
      .unwrap_or_else(|| ("editor".to_string(), "Editor".to_string(), Vec::new()));
    list.push(Person {
      uid,
      name,
      email,
      role_id,
      role_name,
    });
  }
  let index = list.iter().enumerate().map(|(i, p)| (p.uid, i)).collect();
  Ok(People { list, index, roles })
}

struct Group {
  id: Uuid,
  name: String,
  description: String,
  updated_ms: i64,
  members: Vec<i64>,
}

async fn groups_of(conn: &mut PgConnection, ws: Uuid) -> Result<Vec<Group>, AppError> {
  let rows: Vec<(Uuid, String, String, i64)> = sqlx::query_as(
    "SELECT id, name, description, updated_ms FROM carehub_learning_group \
     WHERE workspace_id = $1 ORDER BY LOWER(name), id",
  )
  .bind(ws)
  .fetch_all(&mut *conn)
  .await?;
  let links: Vec<(Uuid, i64)> = sqlx::query_as(
    "SELECT gm.group_id, gm.uid FROM carehub_learning_group_member gm \
     JOIN carehub_learning_group g ON g.id = gm.group_id \
     WHERE g.workspace_id = $1 ORDER BY gm.added_ms, gm.uid",
  )
  .bind(ws)
  .fetch_all(&mut *conn)
  .await?;
  let mut by_group: HashMap<Uuid, Vec<i64>> = HashMap::new();
  for (group, uid) in links {
    by_group.entry(group).or_default().push(uid);
  }
  Ok(
    rows
      .into_iter()
      .map(|(id, name, description, updated_ms)| Group {
        id,
        name,
        description,
        updated_ms,
        members: by_group.remove(&id).unwrap_or_default(),
      })
      .collect(),
  )
}

struct Course {
  title: String,
  status: String,
  version: i32,
  catalog: bool,
}

/// Courses published at least once (title and settings of the live version).
async fn courses_of(
  pool: &PgPool,
  ws: Uuid,
  only: Option<Uuid>,
) -> Result<BTreeMap<Uuid, Course>, AppError> {
  let rows: Vec<(Uuid, String, String, i32, bool)> = sqlx::query_as(
    "SELECT c.id, \
       COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), NULLIF(c.title, ''), 'Untitled course'), \
       c.status, v.version, COALESCE(v.content->'settings'->>'catalog', 'true') <> 'false' \
     FROM carehub_course c \
     JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.workspace_id = $1 AND c.published_version IS NOT NULL \
       AND ($2::UUID IS NULL OR c.id = $2)",
  )
  .bind(ws)
  .bind(only)
  .fetch_all(pool)
  .await?;
  Ok(
    rows
      .into_iter()
      .map(|(id, title, status, version, catalog)| {
        (
          id,
          Course {
            title,
            status,
            version,
            catalog,
          },
        )
      })
      .collect(),
  )
}

/// Live titles, fetched only when a message or calendar entry needs them.
async fn titles_of(pool: &PgPool, ids: &[Uuid]) -> Result<HashMap<Uuid, String>, AppError> {
  if ids.is_empty() {
    return Ok(HashMap::new());
  }
  let rows: Vec<(Uuid, String)> = sqlx::query_as(
    "SELECT c.id, \
       COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), NULLIF(c.title, ''), 'Untitled course') \
     FROM carehub_course c \
     LEFT JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.id = ANY($1)",
  )
  .bind(ids.to_vec())
  .fetch_all(pool)
  .await?;
  Ok(rows.into_iter().collect())
}

async fn names_of(pool: &PgPool, uids: Vec<i64>) -> Result<HashMap<i64, String>, AppError> {
  if uids.is_empty() {
    return Ok(HashMap::new());
  }
  let rows: Vec<(i64, String)> = sqlx::query_as(
    "SELECT uid, COALESCE(NULLIF(name, ''), split_part(COALESCE(email, ''), '@', 1), '') \
     FROM af_user WHERE uid = ANY($1)",
  )
  .bind(uids)
  .fetch_all(pool)
  .await?;
  Ok(rows.into_iter().collect())
}

struct Asg {
  id: Uuid,
  course: Uuid,
  kind: String,
  target: String,
  due_ms: Option<i64>,
  tz: i32,
  required: bool,
  note: String,
  calendar: bool,
  created_by: i64,
  created_ms: i64,
  updated_ms: i64,
}

type AsgRow = (
  Uuid,
  Uuid,
  String,
  String,
  Option<i64>,
  i32,
  bool,
  String,
  bool,
  i64,
  i64,
  i64,
);

async fn assignments_of(pool: &PgPool, ws: Uuid, only: Option<Uuid>) -> Result<Vec<Asg>, AppError> {
  let rows: Vec<AsgRow> = sqlx::query_as(
    "SELECT id, course_id, target_kind, target_id, due_ms, tz_minutes, required, note, calendar, \
       created_by, created_ms, updated_ms \
     FROM carehub_learning_assignment \
     WHERE workspace_id = $1 AND ($2::UUID IS NULL OR course_id = $2)",
  )
  .bind(ws)
  .bind(only)
  .fetch_all(pool)
  .await?;
  Ok(
    rows
      .into_iter()
      .map(|r| Asg {
        id: r.0,
        course: r.1,
        kind: r.2,
        target: r.3,
        due_ms: r.4,
        tz: r.5,
        required: r.6,
        note: r.7,
        calendar: r.8,
        created_by: r.9,
        created_ms: r.10,
        updated_ms: r.11,
      })
      .collect(),
  )
}

/// A person's effective assignment for one course when several cover them:
/// earliest due, required if any is, earliest assigned, newest note.
struct Eff {
  due_ms: Option<i64>,
  required: bool,
  assigned_ms: i64,
  note: String,
  note_ms: i64,
}

impl Eff {
  fn new() -> Eff {
    Eff {
      due_ms: None,
      required: false,
      assigned_ms: i64::MAX,
      note: String::new(),
      note_ms: i64::MIN,
    }
  }

  fn add(&mut self, due_ms: Option<i64>, required: bool, note: &str, updated_ms: i64, assigned_ms: i64) {
    if let Some(d) = due_ms {
      self.due_ms = Some(self.due_ms.map_or(d, |x| x.min(d)));
    }
    self.required = self.required || required;
    self.assigned_ms = self.assigned_ms.min(assigned_ms);
    if !note.trim().is_empty() && updated_ms >= self.note_ms {
      self.note = note.to_string();
      self.note_ms = updated_ms;
    }
  }

  fn overdue(&self, status: Option<&str>, now: i64) -> bool {
    !status.map(finished).unwrap_or(false) && self.due_ms.map(|d| d < now).unwrap_or(false)
  }

  fn info(&self, status: Option<&str>, now: i64) -> Value {
    json!({
      "due_ms": self.due_ms,
      "required": self.required,
      "overdue": self.overdue(status, now),
      "assigned_ms": self.assigned_ms,
      "note": self.note,
    })
  }
}

#[derive(Default, Clone, Copy)]
struct Tally {
  assigned: i64,
  finished: i64,
  in_progress: i64,
  not_started: i64,
  overdue: i64,
}

impl Tally {
  fn add(&mut self, status: Option<&str>, due: Option<i64>, now: i64) {
    self.assigned += 1;
    let done = status.map(finished).unwrap_or(false);
    match status {
      None => self.not_started += 1,
      Some(_) if done => self.finished += 1,
      Some(_) => self.in_progress += 1,
    }
    if !done && due.map(|d| d < now).unwrap_or(false) {
      self.overdue += 1;
    }
  }
}

fn with_counts(mut v: Value, t: &Tally, total_key: &str) -> Value {
  if let Value::Object(m) = &mut v {
    m.insert(total_key.to_string(), json!(t.assigned));
    m.insert("finished".to_string(), json!(t.finished));
    m.insert("in_progress".to_string(), json!(t.in_progress));
    m.insert("not_started".to_string(), json!(t.not_started));
    m.insert("overdue".to_string(), json!(t.overdue));
  }
  v
}

struct Enr {
  status: String,
  percent: i32,
  completed_ms: Option<i64>,
}

/// Everything the assignment lists and reports need, loaded once per request.
/// Only current non-guest members are counted.
struct Board {
  now: i64,
  people: People,
  courses: BTreeMap<Uuid, Course>,
  asgs: Vec<Asg>,
  rows: HashMap<Uuid, Vec<i64>>,
  eff: HashMap<(Uuid, i64), Eff>,
  enr: HashMap<(Uuid, i64), Enr>,
}

impl Board {
  async fn load(pool: &PgPool, ws: Uuid, only: Option<Uuid>) -> Result<Board, AppError> {
    let people = people_of(pool, ws).await?;
    let courses = courses_of(pool, ws, only).await?;
    let asgs = assignments_of(pool, ws, only).await?;
    let raw: Vec<(Uuid, i64, i64)> = sqlx::query_as(
      "SELECT s.assignment_id, s.uid, s.assigned_ms FROM carehub_learning_assignee s \
       JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
       WHERE a.workspace_id = $1 AND ($2::UUID IS NULL OR a.course_id = $2)",
    )
    .bind(ws)
    .bind(only)
    .fetch_all(pool)
    .await?;
    let enr_rows: Vec<(Uuid, i64, String, i32, Option<i64>)> = sqlx::query_as(
      "SELECT e.course_id, e.uid, e.status, e.percent, e.completed_ms FROM carehub_enrollment e \
       JOIN carehub_course c ON c.id = e.course_id \
       WHERE c.workspace_id = $1 AND ($2::UUID IS NULL OR e.course_id = $2)",
    )
    .bind(ws)
    .bind(only)
    .fetch_all(pool)
    .await?;
    let by_id: HashMap<Uuid, usize> = asgs.iter().enumerate().map(|(i, a)| (a.id, i)).collect();
    let mut rows: HashMap<Uuid, Vec<i64>> = HashMap::new();
    let mut eff: HashMap<(Uuid, i64), Eff> = HashMap::new();
    for (asg_id, uid, assigned_ms) in raw {
      if !people.has(uid) {
        continue;
      }
      let a = match by_id.get(&asg_id).and_then(|i| asgs.get(*i)) {
        Some(a) => a,
        None => continue,
      };
      rows.entry(asg_id).or_default().push(uid);
      eff
        .entry((a.course, uid))
        .or_insert_with(Eff::new)
        .add(a.due_ms, a.required, &a.note, a.updated_ms, assigned_ms);
    }
    let enr = enr_rows
      .into_iter()
      .map(|(course, uid, status, percent, completed_ms)| {
        (
          (course, uid),
          Enr {
            status,
            percent,
            completed_ms,
          },
        )
      })
      .collect();
    Ok(Board {
      now: now_ms(),
      people,
      courses,
      asgs,
      rows,
      eff,
      enr,
    })
  }

  fn status(&self, course: Uuid, uid: i64) -> Option<&str> {
    self.enr.get(&(course, uid)).map(|e| e.status.as_str())
  }

  fn due(&self, course: Uuid, uid: i64) -> Option<i64> {
    self.eff.get(&(course, uid)).and_then(|e| e.due_ms)
  }

  fn target_name(&self, a: &Asg, groups: &[Group]) -> String {
    match a.kind.as_str() {
      "person" => a
        .target
        .parse::<i64>()
        .ok()
        .and_then(|uid| self.people.get(uid))
        .map(|p| p.name.clone())
        .unwrap_or_else(|| "Former member".to_string()),
      "group" => Uuid::parse_str(&a.target)
        .ok()
        .and_then(|id| groups.iter().find(|g| g.id == id))
        .map(|g| g.name.clone())
        .unwrap_or_else(|| "Deleted group".to_string()),
      "role" => carehub_extras::role_info(&self.people.roles, &a.target)
        .map(|r| r.1)
        .unwrap_or_else(|| a.target.clone()),
      _ => "Everyone".to_string(),
    }
  }

  fn assignment_json(&self, a: &Asg, groups: &[Group], names: &HashMap<i64, String>) -> Value {
    let mut t = Tally::default();
    if let Some(uids) = self.rows.get(&a.id) {
      for uid in uids {
        t.add(self.status(a.course, *uid), self.due(a.course, *uid), self.now);
      }
    }
    let title = self
      .courses
      .get(&a.course)
      .map(|c| c.title.clone())
      .unwrap_or_else(|| "Untitled course".to_string());
    let v = json!({
      "id": a.id.to_string(),
      "course_id": a.course.to_string(),
      "course_title": title,
      "target": { "kind": a.kind, "id": a.target, "name": self.target_name(a, groups) },
      "due_ms": a.due_ms,
      "tz_minutes": a.tz,
      "required": a.required,
      "note": a.note,
      "calendar": a.calendar,
      "created_ms": a.created_ms,
      "created_by_name": names.get(&a.created_by).cloned().unwrap_or_default(),
      "updated_ms": a.updated_ms,
    });
    with_counts(v, &t, "people")
  }

  fn people_json(&self, a: &Asg) -> Vec<Value> {
    let mut out: Vec<(String, Value)> = Vec::new();
    if let Some(uids) = self.rows.get(&a.id) {
      for uid in uids {
        let p = match self.people.get(*uid) {
          Some(p) => p,
          None => continue,
        };
        let e = self.enr.get(&(a.course, *uid));
        let eff = self.eff.get(&(a.course, *uid));
        let status = e.map(|x| x.status.as_str());
        out.push((
          p.name.to_lowercase(),
          json!({
            "uid": uid.to_string(),
            "name": p.name,
            "email": p.email,
            "status": status.unwrap_or("not_started"),
            "percent": e.map(|x| x.percent).unwrap_or(0),
            "due_ms": eff.and_then(|x| x.due_ms),
            "overdue": eff.map(|x| x.overdue(status, self.now)).unwrap_or(false),
            "assigned_ms": eff.map(|x| x.assigned_ms).unwrap_or(0),
            "completed_ms": e.and_then(|x| x.completed_ms),
          }),
        ));
      }
    }
    out.sort_by(|x, y| x.0.cmp(&y.0));
    out.into_iter().map(|x| x.1).collect()
  }

  /// Per course, over distinct assigned people (effective assignment).
  fn course_tallies(&self) -> HashMap<Uuid, Tally> {
    let mut out: HashMap<Uuid, Tally> = HashMap::new();
    for ((course, uid), e) in &self.eff {
      out
        .entry(*course)
        .or_default()
        .add(self.status(*course, *uid), e.due_ms, self.now);
    }
    out
  }
}

/// Who a target covers right now (current non-guest members only).
fn want_for(kind: &str, id: &str, people: &People, groups: &[Group]) -> Vec<i64> {
  match kind {
    "person" => id
      .parse::<i64>()
      .ok()
      .filter(|uid| people.has(*uid))
      .into_iter()
      .collect(),
    "role" => people
      .list
      .iter()
      .filter(|p| p.role_id == id)
      .map(|p| p.uid)
      .collect(),
    "everyone" => people.list.iter().map(|p| p.uid).collect(),
    "group" => match Uuid::parse_str(id).ok().and_then(|g| groups.iter().find(|x| x.id == g)) {
      Some(g) => g.members.iter().copied().filter(|uid| people.has(*uid)).collect(),
      None => Vec::new(),
    },
    _ => Vec::new(),
  }
}

/// Makes the assignee rows of one assignment match `want`: adds missing people
/// and removes people no longer covered unless they finished (progress is never
/// touched).
async fn sync_assignment(
  conn: &mut PgConnection,
  asg: Uuid,
  course: Uuid,
  ws: Uuid,
  notify: bool,
  want: &[i64],
  now: i64,
) -> Result<(), AppError> {
  let pending: i64 = if notify { 0 } else { -1 };
  sqlx::query(
    "INSERT INTO carehub_learning_assignee \
       (assignment_id, uid, course_id, workspace_id, assigned_ms, sent_assigned_ms) \
     SELECT $1, t.x, $2, $3, $4, $5 FROM UNNEST($6::BIGINT[]) AS t(x) \
     ON CONFLICT (assignment_id, uid) DO NOTHING",
  )
  .bind(asg)
  .bind(course)
  .bind(ws)
  .bind(now)
  .bind(pending)
  .bind(want.to_vec())
  .execute(&mut *conn)
  .await?;
  sqlx::query(
    "DELETE FROM carehub_learning_assignee s WHERE s.assignment_id = $1 \
       AND NOT (s.uid = ANY($2::BIGINT[])) \
       AND NOT EXISTS (SELECT 1 FROM carehub_enrollment e WHERE e.course_id = s.course_id \
         AND e.uid = s.uid AND e.status IN ('completed', 'passed'))",
  )
  .bind(asg)
  .bind(want.to_vec())
  .execute(&mut *conn)
  .await?;
  Ok(())
}

/// A new due date: due / overdue reminders start again from scratch.
async fn rearm(conn: &mut PgConnection, asg: Uuid) -> Result<(), AppError> {
  sqlx::query(
    "UPDATE carehub_learning_assignee SET sent_due_ms = 0, sent_overdue_ms = 0 \
     WHERE assignment_id = $1",
  )
  .bind(asg)
  .execute(&mut *conn)
  .await?;
  Ok(())
}

/// After a change commits: reminders and calendar entries for those courses,
/// in the background so the request returns straight away.
fn after_change(pool: &PgPool, mut courses: Vec<Uuid>) {
  courses.sort();
  courses.dedup();
  if courses.is_empty() {
    return;
  }
  let pool = pool.clone();
  tokio::spawn(async move {
    if let Err(err) = deliver(&pool, Some(courses.clone())).await {
      warn_rarely(&format!("reminders: {}", err));
    }
    if let Err(err) = sync_calendar(&pool, Some(courses)).await {
      warn_rarely(&format!("calendar entries: {}", err));
    }
  });
}

// ---------------------------------------------------------------------------
// Members and groups
// ---------------------------------------------------------------------------

/// GET /members
async fn members(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_read(&c)?;
  let people = people_of(&state.pg_pool, c.ws).await?;
  let mut counts: HashMap<&str, i64> = HashMap::new();
  for p in &people.list {
    *counts.entry(p.role_id.as_str()).or_insert(0) += 1;
  }
  let members: Vec<Value> = people
    .list
    .iter()
    .map(|p| {
      json!({
        "uid": p.uid.to_string(),
        "name": p.name,
        "email": p.email,
        "role_id": p.role_id,
        "role_name": p.role_name,
      })
    })
    .collect();
  let roles: Vec<Value> = people
    .roles
    .iter()
    .map(|r| {
      let id = txt(r, "id");
      json!({ "id": id, "name": txt(r, "name"), "members": counts.get(id).copied().unwrap_or(0) })
    })
    .collect();
  Ok(json!({ "members": members, "roles": roles }))
}

/// GET /groups
async fn groups(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_read(&c)?;
  let pool = &state.pg_pool;
  let people = people_of(pool, c.ws).await?;
  let mut conn = pool.acquire().await?;
  let groups = groups_of(&mut conn, c.ws).await?;
  let used: Vec<(String, i64)> = sqlx::query_as(
    "SELECT a.target_id, COUNT(*)::BIGINT FROM carehub_learning_assignment a \
     JOIN carehub_course c ON c.id = a.course_id \
     WHERE a.workspace_id = $1 AND a.target_kind = 'group' AND c.status = 'published' \
     GROUP BY a.target_id",
  )
  .bind(c.ws)
  .fetch_all(&mut *conn)
  .await?;
  let used: HashMap<String, i64> = used.into_iter().collect();
  let list: Vec<Value> = groups
    .iter()
    .map(|g| {
      let uids: Vec<String> = g
        .members
        .iter()
        .filter(|uid| people.has(**uid))
        .map(|uid| uid.to_string())
        .collect();
      json!({
        "id": g.id.to_string(),
        "name": g.name,
        "description": g.description,
        "members": uids.len(),
        "member_uids": uids,
        "courses": used.get(&g.id.to_string()).copied().unwrap_or(0),
        "updated_ms": g.updated_ms,
      })
    })
    .collect();
  Ok(json!({ "groups": list }))
}

fn group_name(v: &Value) -> Result<String, AppError> {
  let name = txt(v, "name").trim().to_string();
  if name.is_empty() || name.chars().count() > MAX_GROUP_NAME_CHARS {
    return Err(bad(GROUP_NAME_MSG));
  }
  Ok(name)
}

/// member_uids from a body: None when absent; unknown and guest uids dropped.
fn wanted_uids(v: &Value, people: &People) -> Result<Option<Vec<i64>>, AppError> {
  let raw = match v.get("member_uids") {
    None | Some(Value::Null) => return Ok(None),
    Some(Value::Array(a)) => a,
    Some(_) => return Err(bad(BODY_MSG)),
  };
  if raw.len() > MAX_GROUP_PEOPLE * 2 {
    return Err(bad(GROUP_TOO_BIG_MSG));
  }
  let mut seen: HashSet<i64> = HashSet::new();
  let mut out: Vec<i64> = Vec::new();
  for x in raw {
    if let Some(uid) = uid_of(Some(x)) {
      if people.has(uid) && seen.insert(uid) {
        out.push(uid);
      }
    }
  }
  if out.len() > MAX_GROUP_PEOPLE {
    return Err(bad(GROUP_TOO_BIG_MSG));
  }
  Ok(Some(out))
}

async fn name_taken(
  conn: &mut PgConnection,
  ws: Uuid,
  name: &str,
  except: Option<Uuid>,
) -> Result<bool, AppError> {
  let found: Option<Uuid> = sqlx::query_scalar(
    "SELECT id FROM carehub_learning_group WHERE workspace_id = $1 AND LOWER(name) = LOWER($2) \
       AND ($3::UUID IS NULL OR id <> $3) LIMIT 1",
  )
  .bind(ws)
  .bind(name)
  .bind(except)
  .fetch_optional(&mut *conn)
  .await?;
  Ok(found.is_some())
}

fn name_clash(name: &str) -> AppError {
  bad(&format!("There's already a group called “{}”.", name))
}

/// POST /groups
async fn create_group(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_assign(&c)?;
  let v = body_json(body)?;
  let name = group_name(&v)?;
  let description = clip(txt(&v, "description").trim(), MAX_GROUP_DESCRIPTION_CHARS);
  let pool = &state.pg_pool;
  let people = people_of(pool, c.ws).await?;
  let uids = wanted_uids(&v, &people)?.unwrap_or_default();
  let id = Uuid::new_v4();
  let now = now_ms();
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  if name_taken(&mut tx, c.ws, &name, None).await? {
    return Err(name_clash(&name));
  }
  sqlx::query(
    "INSERT INTO carehub_learning_group \
       (id, workspace_id, name, description, created_by, created_ms, updated_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $6)",
  )
  .bind(id)
  .bind(c.ws)
  .bind(&name)
  .bind(&description)
  .bind(c.uid())
  .bind(now)
  .execute(&mut *tx)
  .await?;
  sqlx::query(
    "INSERT INTO carehub_learning_group_member (group_id, uid, added_ms) \
     SELECT $1, t.x, $2 FROM UNNEST($3::BIGINT[]) AS t(x) ON CONFLICT DO NOTHING",
  )
  .bind(id)
  .bind(now)
  .bind(uids)
  .execute(&mut *tx)
  .await?;
  tx.commit().await?;
  Ok(json!({ "id": id.to_string() }))
}

/// PUT /groups/{id}
async fn update_group(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_assign(&c)?;
  let group = parse_uuid(id, GROUP_LINK_MSG)?;
  let v = body_json(body)?;
  let name = match v.get("name") {
    None | Some(Value::Null) => None,
    Some(_) => Some(group_name(&v)?),
  };
  let description = v
    .get("description")
    .and_then(|x| x.as_str())
    .map(|d| clip(d.trim(), MAX_GROUP_DESCRIPTION_CHARS));
  let pool = &state.pg_pool;
  let people = people_of(pool, c.ws).await?;
  let wanted = wanted_uids(&v, &people)?;
  let now = now_ms();
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  let found: Option<Uuid> = sqlx::query_scalar(
    "SELECT id FROM carehub_learning_group WHERE id = $1 AND workspace_id = $2 FOR UPDATE",
  )
  .bind(group)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  if found.is_none() {
    return Err(group_missing());
  }
  if let Some(name) = &name {
    if name_taken(&mut tx, c.ws, name, Some(group)).await? {
      return Err(name_clash(name));
    }
  }
  sqlx::query(
    "UPDATE carehub_learning_group SET name = COALESCE($2, name), \
       description = COALESCE($3, description), updated_ms = $4 WHERE id = $1",
  )
  .bind(group)
  .bind(name)
  .bind(description)
  .bind(now)
  .execute(&mut *tx)
  .await?;
  let mut added: usize = 0;
  let mut removed: usize = 0;
  let mut touched: Vec<Uuid> = Vec::new();
  if let Some(want) = wanted {
    let old: Vec<i64> =
      sqlx::query_scalar("SELECT uid FROM carehub_learning_group_member WHERE group_id = $1")
        .bind(group)
        .fetch_all(&mut *tx)
        .await?;
    let old_set: HashSet<i64> = old.iter().copied().collect();
    let new_set: HashSet<i64> = want.iter().copied().collect();
    let gone: Vec<i64> = old.iter().copied().filter(|u| !new_set.contains(u)).collect();
    let fresh: Vec<i64> = want.iter().copied().filter(|u| !old_set.contains(u)).collect();
    added = fresh.len();
    removed = gone.iter().filter(|u| people.has(**u)).count();
    if !gone.is_empty() {
      sqlx::query("DELETE FROM carehub_learning_group_member WHERE group_id = $1 AND uid = ANY($2)")
        .bind(group)
        .bind(gone)
        .execute(&mut *tx)
        .await?;
    }
    if !fresh.is_empty() {
      sqlx::query(
        "INSERT INTO carehub_learning_group_member (group_id, uid, added_ms) \
         SELECT $1, t.x, $2 FROM UNNEST($3::BIGINT[]) AS t(x) ON CONFLICT DO NOTHING",
      )
      .bind(group)
      .bind(now)
      .bind(fresh)
      .execute(&mut *tx)
      .await?;
    }
    // Assignments that target the group follow its members straight away.
    let targets: Vec<(Uuid, Uuid, bool)> = sqlx::query_as(
      "SELECT a.id, a.course_id, a.notify FROM carehub_learning_assignment a \
       JOIN carehub_course c ON c.id = a.course_id \
       WHERE a.workspace_id = $1 AND a.target_kind = 'group' AND a.target_id = $2 \
         AND c.status = 'published'",
    )
    .bind(c.ws)
    .bind(group.to_string())
    .fetch_all(&mut *tx)
    .await?;
    for (asg, course, notify) in targets {
      sync_assignment(&mut tx, asg, course, c.ws, notify, &want, now).await?;
      touched.push(course);
    }
  }
  tx.commit().await?;
  after_change(pool, touched);
  Ok(json!({ "ok": true, "added": added, "removed": removed }))
}

/// DELETE /groups/{id}
async fn delete_group(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_assign(&c)?;
  let group = parse_uuid(id, GROUP_LINK_MSG)?;
  let pool = &state.pg_pool;
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  let found: Option<Uuid> = sqlx::query_scalar(
    "SELECT id FROM carehub_learning_group WHERE id = $1 AND workspace_id = $2 FOR UPDATE",
  )
  .bind(group)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  if found.is_none() {
    return Err(group_missing());
  }
  let courses: Vec<Uuid> = sqlx::query_scalar(
    "DELETE FROM carehub_learning_assignment \
     WHERE workspace_id = $1 AND target_kind = 'group' AND target_id = $2 RETURNING course_id",
  )
  .bind(c.ws)
  .bind(group.to_string())
  .fetch_all(&mut *tx)
  .await?;
  sqlx::query("DELETE FROM carehub_learning_group WHERE id = $1")
    .bind(group)
    .execute(&mut *tx)
    .await?;
  tx.commit().await?;
  let removed = courses.len();
  after_change(pool, courses);
  Ok(json!({ "ok": true, "assignments_removed": removed }))
}

// ---------------------------------------------------------------------------
// Assignments
// ---------------------------------------------------------------------------

fn parse_due(v: Option<&Value>) -> Result<Option<i64>, AppError> {
  match v {
    None | Some(Value::Null) => Ok(None),
    Some(x) => {
      let ms = x
        .as_i64()
        .or_else(|| x.as_f64().filter(|f| f.is_finite()).map(|f| f as i64))
        .ok_or_else(|| bad(BAD_DUE_MSG))?;
      if ms <= 0 || ms > MAX_DUE_MS {
        return Err(bad(BAD_DUE_MSG));
      }
      Ok(Some(ms))
    },
  }
}

/// None = leave as it is; Some("") clears.
fn parse_note(v: &Value) -> Result<Option<String>, AppError> {
  match v.get("note") {
    None | Some(Value::Null) => Ok(None),
    Some(Value::String(s)) => {
      let t = s.trim();
      if t.chars().count() > MAX_NOTE_CHARS {
        Err(bad(NOTE_MSG))
      } else {
        Ok(Some(t.to_string()))
      }
    },
    Some(_) => Err(bad(BODY_MSG)),
  }
}

fn parse_tz(v: &Value) -> Option<i32> {
  v.get("tz_minutes")
    .and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64)))
    .map(|m| m.clamp(-840, 840) as i32)
}

/// A target from the browser in its stored form, if it still exists.
fn normalise_target(
  kind: &str,
  id: &str,
  people: &People,
  groups: &[Group],
) -> Option<(String, String)> {
  match kind {
    "person" => {
      let uid = id.trim().parse::<i64>().ok()?;
      if people.has(uid) {
        Some(("person".to_string(), uid.to_string()))
      } else {
        None
      }
    },
    "group" => {
      let g = Uuid::parse_str(id.trim()).ok()?;
      if groups.iter().any(|x| x.id == g) {
        Some(("group".to_string(), g.to_string()))
      } else {
        None
      }
    },
    "role" => {
      let r = id.trim();
      if carehub_extras::role_info(&people.roles, r).is_some() {
        Some(("role".to_string(), r.to_string()))
      } else {
        None
      }
    },
    "everyone" => Some(("everyone".to_string(), String::new())),
    _ => None,
  }
}

/// POST /assignments
async fn create_assignments(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_assign(&c)?;
  let v = body_json(body)?;
  let course = parse_uuid(txt(&v, "course_id"), COURSE_LINK_MSG)?;
  let pool = &state.pg_pool;
  let head: Option<(String, Option<i32>)> = sqlx::query_as(
    "SELECT status, published_version FROM carehub_course WHERE id = $1 AND workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(pool)
  .await?;
  let (status, published) = head.ok_or_else(missing)?;
  if status == "archived" {
    return Err(bad(ARCHIVED_MSG));
  }
  if status != "published" || published.is_none() {
    return Err(bad(NOT_PUBLISHED_MSG));
  }
  let raw = list(&v, "targets");
  if raw.is_empty() {
    return Err(bad(NO_TARGETS_MSG));
  }
  if raw.len() > MAX_TARGETS {
    return Err(bad(TOO_MANY_TARGETS_MSG));
  }
  let now = now_ms();
  let due = parse_due(v.get("due_ms"))?;
  if due.map(|d| d <= now).unwrap_or(false) {
    return Err(bad(PAST_DUE_MSG));
  }
  let tz = parse_tz(&v).unwrap_or(0);
  let required = flag(&v, "required", false);
  let note = parse_note(&v)?.unwrap_or_default();
  let calendar = flag(&v, "calendar", true);
  let notify = flag(&v, "notify", true);
  let people = people_of(pool, c.ws).await?;
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  let groups = groups_of(&mut tx, c.ws).await?;
  let mut seen: HashSet<(String, String)> = HashSet::new();
  let mut targets: Vec<(String, String, Vec<i64>)> = Vec::new();
  for t in raw {
    let id = match t.get("id") {
      Some(Value::String(s)) => s.trim().to_string(),
      Some(Value::Number(n)) => n.to_string(),
      _ => String::new(),
    };
    let (kind, id) = match normalise_target(txt(t, "kind").trim(), &id, &people, &groups) {
      Some(x) => x,
      None => continue,
    };
    if !seen.insert((kind.clone(), id.clone())) {
      continue;
    }
    let want = want_for(&kind, &id, &people, &groups);
    targets.push((kind, id, want));
  }
  if targets.is_empty() {
    return Err(bad(UNKNOWN_TARGETS_MSG));
  }
  let mut created: i64 = 0;
  let mut updated: i64 = 0;
  let mut covered: HashSet<i64> = HashSet::new();
  for (kind, id, want) in &targets {
    let existing: Option<(Uuid, Option<i64>)> = sqlx::query_as(
      "SELECT id, due_ms FROM carehub_learning_assignment \
       WHERE course_id = $1 AND target_kind = $2 AND target_id = $3 FOR UPDATE",
    )
    .bind(course)
    .bind(kind)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let asg = match existing {
      Some((asg, old_due)) => {
        sqlx::query(
          "UPDATE carehub_learning_assignment SET due_ms = $2, tz_minutes = $3, required = $4, \
             note = $5, calendar = $6, notify = $7, updated_ms = $8 WHERE id = $1",
        )
        .bind(asg)
        .bind(due)
        .bind(tz)
        .bind(required)
        .bind(&note)
        .bind(calendar)
        .bind(notify)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if old_due != due {
          rearm(&mut tx, asg).await?;
        }
        updated += 1;
        asg
      },
      None => {
        let asg = Uuid::new_v4();
        sqlx::query(
          "INSERT INTO carehub_learning_assignment (id, workspace_id, course_id, target_kind, \
             target_id, due_ms, tz_minutes, required, note, calendar, notify, created_by, \
             created_ms, updated_ms) \
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)",
        )
        .bind(asg)
        .bind(c.ws)
        .bind(course)
        .bind(kind)
        .bind(id)
        .bind(due)
        .bind(tz)
        .bind(required)
        .bind(&note)
        .bind(calendar)
        .bind(notify)
        .bind(c.uid())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        created += 1;
        asg
      },
    };
    sync_assignment(&mut tx, asg, course, c.ws, notify, want, now).await?;
    covered.extend(want.iter().copied());
  }
  tx.commit().await?;
  after_change(pool, vec![course]);
  Ok(json!({ "created": created, "updated": updated, "people": covered.len() }))
}

/// GET /assignments?course_id=
async fn list_assignments(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  query: &HashMap<String, String>,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_read(&c)?;
  let only = match query.get("course_id").map(|s| s.trim()).filter(|s| !s.is_empty()) {
    Some(raw) => Some(parse_uuid(raw, COURSE_LINK_MSG)?),
    None => None,
  };
  let pool = &state.pg_pool;
  let board = Board::load(pool, c.ws, only).await?;
  let mut conn = pool.acquire().await?;
  let groups = groups_of(&mut conn, c.ws).await?;
  let names = names_of(pool, board.asgs.iter().map(|a| a.created_by).collect()).await?;
  let mut asgs: Vec<&Asg> = board.asgs.iter().collect();
  asgs.sort_by(|a, b| {
    b.created_ms
      .cmp(&a.created_ms)
      .then_with(|| a.id.cmp(&b.id))
  });
  let assignments: Vec<Value> = asgs
    .iter()
    .map(|a| board.assignment_json(a, &groups, &names))
    .collect();
  let tallies = board.course_tallies();
  let mut courses: Vec<(String, Value)> = board
    .courses
    .iter()
    .map(|(id, course)| {
      let status = if course.status == "archived" {
        "archived"
      } else {
        "published"
      };
      let v = json!({
        "id": id.to_string(),
        "title": course.title,
        "status": status,
        "catalog": course.catalog,
        "version": course.version,
      });
      let t = tallies.get(id).copied().unwrap_or_default();
      (course.title.to_lowercase(), with_counts(v, &t, "assigned"))
    })
    .collect();
  courses.sort_by(|a, b| a.0.cmp(&b.0));
  let courses: Vec<Value> = courses.into_iter().map(|x| x.1).collect();
  Ok(json!({ "assignments": assignments, "courses": courses }))
}

/// GET /assignments/{id}
async fn get_assignment(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_read(&c)?;
  let asg = parse_uuid(id, ASSIGNMENT_LINK_MSG)?;
  let pool = &state.pg_pool;
  let course: Option<Uuid> = sqlx::query_scalar(
    "SELECT course_id FROM carehub_learning_assignment WHERE id = $1 AND workspace_id = $2",
  )
  .bind(asg)
  .bind(c.ws)
  .fetch_optional(pool)
  .await?;
  let course = course.ok_or_else(assignment_missing)?;
  let board = Board::load(pool, c.ws, Some(course)).await?;
  let a = board
    .asgs
    .iter()
    .find(|a| a.id == asg)
    .ok_or_else(assignment_missing)?;
  let mut conn = pool.acquire().await?;
  let groups = groups_of(&mut conn, c.ws).await?;
  let names = names_of(pool, vec![a.created_by]).await?;
  Ok(json!({
    "assignment": board.assignment_json(a, &groups, &names),
    "people": board.people_json(a),
  }))
}

/// PUT /assignments/{id}: a missing field stays as it is.
async fn update_assignment(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_assign(&c)?;
  let asg = parse_uuid(id, ASSIGNMENT_LINK_MSG)?;
  let v = body_json(body)?;
  let pool = &state.pg_pool;
  let now = now_ms();
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  let row: Option<(Uuid, Option<i64>, i32, bool, String, bool)> = sqlx::query_as(
    "SELECT course_id, due_ms, tz_minutes, required, note, calendar \
     FROM carehub_learning_assignment WHERE id = $1 AND workspace_id = $2 FOR UPDATE",
  )
  .bind(asg)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let (course, old_due, old_tz, old_required, old_note, old_calendar) =
    row.ok_or_else(assignment_missing)?;
  let due = match v.get("due_ms") {
    Some(raw) => parse_due(Some(raw))?,
    None => old_due,
  };
  if due != old_due && due.map(|d| d <= now).unwrap_or(false) {
    return Err(bad(PAST_DUE_MSG));
  }
  let tz = parse_tz(&v).unwrap_or(old_tz);
  let required = v
    .get("required")
    .and_then(|x| x.as_bool())
    .unwrap_or(old_required);
  let note = parse_note(&v)?.unwrap_or(old_note);
  let calendar = v
    .get("calendar")
    .and_then(|x| x.as_bool())
    .unwrap_or(old_calendar);
  sqlx::query(
    "UPDATE carehub_learning_assignment SET due_ms = $2, tz_minutes = $3, required = $4, \
       note = $5, calendar = $6, updated_ms = $7 WHERE id = $1",
  )
  .bind(asg)
  .bind(due)
  .bind(tz)
  .bind(required)
  .bind(note)
  .bind(calendar)
  .bind(now)
  .execute(&mut *tx)
  .await?;
  if due != old_due {
    rearm(&mut tx, asg).await?;
  }
  tx.commit().await?;
  after_change(pool, vec![course]);
  Ok(json!({ "ok": true }))
}

/// DELETE /assignments/{id}: progress is kept.
async fn delete_assignment(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_assign(&c)?;
  let asg = parse_uuid(id, ASSIGNMENT_LINK_MSG)?;
  let pool = &state.pg_pool;
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  let course: Option<Uuid> = sqlx::query_scalar(
    "DELETE FROM carehub_learning_assignment WHERE id = $1 AND workspace_id = $2 \
     RETURNING course_id",
  )
  .bind(asg)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let course = course.ok_or_else(assignment_missing)?;
  tx.commit().await?;
  after_change(pool, vec![course]);
  Ok(json!({ "ok": true }))
}

// ---------------------------------------------------------------------------
// Learner: assigned courses and certificates
// ---------------------------------------------------------------------------

/// GET /assigned
async fn assigned(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let pool = &state.pg_pool;
  let uid = c.uid();
  let rows: Vec<(Uuid, Option<i64>, bool, String, i64, i64)> = sqlx::query_as(
    "SELECT a.course_id, a.due_ms, a.required, a.note, a.updated_ms, s.assigned_ms \
     FROM carehub_learning_assignee s \
     JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
     WHERE a.workspace_id = $1 AND s.uid = $2",
  )
  .bind(c.ws)
  .bind(uid)
  .fetch_all(pool)
  .await?;
  let mut eff: HashMap<Uuid, Eff> = HashMap::new();
  for (course, due_ms, required, note, updated_ms, assigned_ms) in rows {
    eff
      .entry(course)
      .or_insert_with(Eff::new)
      .add(due_ms, required, &note, updated_ms, assigned_ms);
  }
  if eff.is_empty() {
    return Ok(json!({ "courses": [] }));
  }
  let ids: Vec<Uuid> = eff.keys().copied().collect();
  // Same card as Phase 1 /catalog and /my (the version the caller is on).
  let cards: Vec<CardRow> = sqlx::query_as(
    "SELECT c.id, c.status, v.published_ms, v.version, \
       COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), NULLIF(c.title, ''), 'Untitled course'), \
       COALESCE(v.content->>'summary', ''), COALESCE(v.content->>'cover', ''), \
       COALESCE(v.content->>'preset', 'standard'), \
       (CASE WHEN jsonb_typeof(v.content->'lessons') = 'array' \
         THEN jsonb_array_length(v.content->'lessons') ELSE 0 END)::INT, \
       (CASE WHEN jsonb_typeof(v.content->'est_minutes') = 'number' \
         THEN LEAST(GREATEST((v.content->>'est_minutes')::NUMERIC, 0), 100000) ELSE 0 END)::INT, \
       (COALESCE(v.content->'quiz'->>'enabled', 'false') = 'true' \
         AND (CASE WHEN jsonb_typeof(v.content->'quiz'->'questions') = 'array' \
           THEN jsonb_array_length(v.content->'quiz'->'questions') ELSE 0 END) > 0), \
       COALESCE(v.content->'settings'->>'catalog', 'true') <> 'false' \
     FROM carehub_course c \
     LEFT JOIN carehub_enrollment e ON e.course_id = c.id AND e.uid = $2 \
     JOIN carehub_course_version v ON v.course_id = c.id \
       AND v.version = COALESCE(e.version, c.published_version) \
     WHERE c.workspace_id = $1 AND c.published_version IS NOT NULL \
       AND c.status = 'published' AND c.id = ANY($3)",
  )
  .bind(c.ws)
  .bind(uid)
  .bind(ids.clone())
  .fetch_all(pool)
  .await?;
  let records: Vec<MyRow> = sqlx::query_as(
    "SELECT e.course_id, e.status, e.percent, e.score, e.attempts, e.time_ms, e.started_ms, \
       e.last_ms, e.completed_ms, e.ack_ms, e.awaiting_ack, e.version \
     FROM carehub_enrollment e WHERE e.uid = $1 AND e.course_id = ANY($2)",
  )
  .bind(uid)
  .bind(ids)
  .fetch_all(pool)
  .await?;
  let by_course: HashMap<Uuid, &MyRow> = records.iter().map(|r| (r.0, r)).collect();
  let now = now_ms();
  let mut out: Vec<((u8, i64, String), Value)> = Vec::new();
  for card in &cards {
    let e = match eff.get(&card.0) {
      Some(e) => e,
      None => continue,
    };
    let my = by_course.get(&card.0).copied();
    let status = my.map(|m| m.1.as_str());
    let mut v = carehub_learning::card_json(card, my);
    if let Value::Object(m) = &mut v {
      m.insert("assignment".to_string(), e.info(status, now));
    }
    let rank: u8 = if e.overdue(status, now) {
      0
    } else if e.due_ms.is_some() {
      1
    } else {
      2
    };
    out.push(((rank, e.due_ms.unwrap_or(i64::MAX), card.4.to_lowercase()), v));
  }
  out.sort_by(|a, b| a.0.cmp(&b.0));
  let courses: Vec<Value> = out.into_iter().map(|x| x.1).collect();
  Ok(json!({ "courses": courses }))
}

type CertRow = (String, Uuid, String, String, Option<i32>, i64, i32);

fn cert_json(r: &CertRow) -> Value {
  json!({
    "code": r.0,
    "course_id": r.1.to_string(),
    "title": r.2,
    "person_name": r.3,
    "score": r.4,
    "issued_ms": r.5,
    "version": r.6,
  })
}

/// GET /certificates/mine
async fn my_certificates(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let pool = &state.pg_pool;
  if let Err(err) = issue_certificates(pool, Some(c.ws), Some(c.uid()), None).await {
    tracing::warn!("carehub learning assign: certificates: {}", err);
  }
  let rows: Vec<CertRow> = sqlx::query_as(
    "SELECT code, course_id, title, person_name, score, issued_ms, version \
     FROM carehub_learning_certificate WHERE workspace_id = $1 AND uid = $2 \
     ORDER BY issued_ms DESC, code",
  )
  .bind(c.ws)
  .bind(c.uid())
  .fetch_all(pool)
  .await?;
  let list: Vec<Value> = rows.iter().map(cert_json).collect();
  Ok(json!({ "certificates": list }))
}

/// Upper case, dashes and spaces optional: "abcd efgh-jkmn" → "ABCD-EFGH-JKMN".
fn normalise_code(raw: &str) -> Option<String> {
  let clean: String = raw
    .chars()
    .filter(|ch| !ch.is_whitespace() && *ch != '-')
    .flat_map(|ch| ch.to_uppercase())
    .collect();
  if clean.len() != 12 || !clean.bytes().all(|b| CODE_ALPHABET.contains(&b)) {
    return None;
  }
  Some(format!(
    "{}-{}-{}",
    clean.get(0..4)?,
    clean.get(4..8)?,
    clean.get(8..12)?
  ))
}

/// GET /certificates/{code}
async fn verify_certificate(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  code: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let code = normalise_code(code).ok_or_else(cert_missing)?;
  let row: Option<CertRow> = sqlx::query_as(
    "SELECT code, course_id, title, person_name, score, issued_ms, version \
     FROM carehub_learning_certificate WHERE code = $1 AND workspace_id = $2",
  )
  .bind(code)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let row = row.ok_or_else(cert_missing)?;
  Ok(json!({ "certificate": cert_json(&row) }))
}

fn new_code() -> String {
  let mut n = Uuid::new_v4().as_u128();
  let base = CODE_ALPHABET.len() as u128;
  let mut out = String::with_capacity(14);
  for i in 0..12 {
    if i == 4 || i == 8 {
      out.push('-');
    }
    let idx = usize::try_from(n % base).unwrap_or(0);
    n /= base;
    out.push(CODE_ALPHABET.get(idx).map(|b| char::from(*b)).unwrap_or('A'));
  }
  out
}

/// One certificate per finish on a version with settings.certificate = true.
/// Idempotent: the (course, person, completed_ms) key decides.
async fn issue_certificates(
  pool: &PgPool,
  ws: Option<Uuid>,
  uid: Option<i64>,
  course: Option<Uuid>,
) -> Result<usize, AppError> {
  let pending: Vec<(Uuid, i32)> = sqlx::query_as(
    "SELECT DISTINCT e.course_id, e.version FROM carehub_enrollment e \
     JOIN carehub_course c ON c.id = e.course_id \
     WHERE e.status IN ('completed', 'passed') AND e.completed_ms IS NOT NULL \
       AND ($1::UUID IS NULL OR c.workspace_id = $1) AND ($2::BIGINT IS NULL OR e.uid = $2) \
       AND ($3::UUID IS NULL OR e.course_id = $3) \
       AND NOT EXISTS (SELECT 1 FROM carehub_learning_certificate k \
         WHERE k.course_id = e.course_id AND k.uid = e.uid AND k.issued_ms = e.completed_ms)",
  )
  .bind(ws)
  .bind(uid)
  .bind(course)
  .fetch_all(pool)
  .await?;
  let mut on_courses: Vec<Uuid> = Vec::new();
  let mut on_versions: Vec<i32> = Vec::new();
  for key in pending {
    let known = CERT_FLAGS.lock().ok().and_then(|m| m.get(&key).copied());
    let enabled = match known {
      Some(x) => x,
      None => {
        let found: Option<bool> = sqlx::query_scalar(
          "SELECT COALESCE(content->'settings'->>'certificate', 'false') = 'true' \
           FROM carehub_course_version WHERE course_id = $1 AND version = $2",
        )
        .bind(key.0)
        .bind(key.1)
        .fetch_optional(pool)
        .await?;
        let x = found.unwrap_or(false);
        if let Ok(mut m) = CERT_FLAGS.lock() {
          m.insert(key, x);
        }
        x
      },
    };
    if enabled {
      on_courses.push(key.0);
      on_versions.push(key.1);
    }
  }
  if on_courses.is_empty() {
    return Ok(0);
  }
  #[allow(clippy::type_complexity)]
  let rows: Vec<(Uuid, i64, i32, Option<i32>, i64, Uuid, String, String)> = sqlx::query_as(
    "SELECT e.course_id, e.uid, e.version, e.score, e.completed_ms, c.workspace_id, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), \
       COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), NULLIF(c.title, ''), 'Untitled course') \
     FROM carehub_enrollment e \
     JOIN UNNEST($1::UUID[], $2::INT[]) AS w(course_id, version) \
       ON w.course_id = e.course_id AND w.version = e.version \
     JOIN carehub_course c ON c.id = e.course_id \
     JOIN carehub_course_version v ON v.course_id = e.course_id AND v.version = e.version \
     LEFT JOIN af_user u ON u.uid = e.uid \
     WHERE e.status IN ('completed', 'passed') AND e.completed_ms IS NOT NULL \
       AND ($3::UUID IS NULL OR c.workspace_id = $3) AND ($4::BIGINT IS NULL OR e.uid = $4) \
       AND NOT EXISTS (SELECT 1 FROM carehub_learning_certificate k \
         WHERE k.course_id = e.course_id AND k.uid = e.uid AND k.issued_ms = e.completed_ms) \
     LIMIT $5",
  )
  .bind(on_courses)
  .bind(on_versions)
  .bind(ws)
  .bind(uid)
  .bind(CERT_BATCH)
  .fetch_all(pool)
  .await?;
  let mut issued = 0usize;
  for (course_id, person, version, score, completed_ms, ws_id, name, title) in rows {
    for _ in 0..5 {
      let done = sqlx::query(
        "INSERT INTO carehub_learning_certificate \
           (code, workspace_id, course_id, uid, version, title, person_name, score, issued_ms) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING",
      )
      .bind(new_code())
      .bind(ws_id)
      .bind(course_id)
      .bind(person)
      .bind(version)
      .bind(&title)
      .bind(&name)
      .bind(score)
      .bind(completed_ms)
      .execute(pool)
      .await?;
      if done.rows_affected() > 0 {
        issued += 1;
        break;
      }
      // Either someone else issued it just now, or the code was taken.
      let exists: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM carehub_learning_certificate \
         WHERE course_id = $1 AND uid = $2 AND issued_ms = $3",
      )
      .bind(course_id)
      .bind(person)
      .bind(completed_ms)
      .fetch_optional(pool)
      .await?;
      if exists.is_some() {
        break;
      }
    }
  }
  Ok(issued)
}

// ---------------------------------------------------------------------------
// Reports: insights and teams
// ---------------------------------------------------------------------------

/// One final-quiz question with how attempts on the live version answered it.
fn question_stats(q: &Value, course: Uuid, attempts: &[(i64, Value, Value)]) -> Value {
  let qid = txt(q, "id");
  let kind = kind_of(q);
  let options = list(q, "options");
  let pairs = list(q, "pairs");
  let size = if kind == "matching" {
    pairs.len()
  } else {
    options.len()
  };
  let mut picked: Vec<i64> = vec![0; size];
  let mut answered: i64 = 0;
  let mut correct: i64 = 0;
  for (uid, answers, results) in attempts {
    let result = match results
      .as_array()
      .and_then(|r| r.iter().find(|x| txt(x, "question_id") == qid))
    {
      Some(r) => r,
      None => continue,
    };
    answered += 1;
    if flag(result, "correct", false) {
      correct += 1;
    }
    let answer = answers.get(qid);
    if kind == "matching" {
      let chosen = match answer.and_then(|a| a.as_object()) {
        Some(o) => o,
        None => continue,
      };
      let by_token: HashMap<String, &str> = pairs
        .iter()
        .map(|p| {
          (
            carehub_learning::pair_token(course, *uid, txt(p, "id")),
            txt(p, "right").trim(),
          )
        })
        .collect();
      for (i, p) in pairs.iter().enumerate() {
        let want = txt(p, "right").trim();
        let hit = chosen
          .get(txt(p, "id"))
          .and_then(|t| t.as_str())
          .and_then(|t| by_token.get(t))
          .map(|got| *got == want)
          .unwrap_or(false);
        if hit {
          if let Some(n) = picked.get_mut(i) {
            *n += 1;
          }
        }
      }
    } else {
      for (i, o) in options.iter().enumerate() {
        let oid = txt(o, "id");
        let hit = !oid.is_empty()
          && match answer {
            Some(Value::String(s)) => s.as_str() == oid,
            Some(Value::Array(a)) => a.iter().any(|x| x.as_str() == Some(oid)),
            _ => false,
          };
        if hit {
          if let Some(n) = picked.get_mut(i) {
            *n += 1;
          }
        }
      }
    }
  }
  let rows: Vec<Value> = if kind == "matching" {
    pairs
      .iter()
      .enumerate()
      .map(|(i, p)| {
        json!({
          "id": txt(p, "id"),
          "text": format!("{} → {}", txt(p, "left").trim(), txt(p, "right").trim()),
          "correct": true,
          "picked": picked.get(i).copied().unwrap_or(0),
        })
      })
      .collect()
  } else {
    options
      .iter()
      .enumerate()
      .map(|(i, o)| {
        json!({
          "id": txt(o, "id"),
          "text": txt(o, "text"),
          "correct": flag(o, "correct", false),
          "picked": picked.get(i).copied().unwrap_or(0),
        })
      })
      .collect()
  };
  json!({
    "id": qid,
    "prompt": txt(q, "prompt"),
    "kind": kind,
    "answered": answered,
    "correct_pct": pct(correct, answered),
    "answers": rows,
  })
}

/// GET /reports/courses/{id}/insights
async fn insights(state: &AppState, user: &UserUuid, ws: &str, id: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_reports(&c)?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let pool = &state.pg_pool;
  let head: Option<(Option<i32>, Option<String>)> = sqlx::query_as(
    "SELECT c.published_version, v.content::text FROM carehub_course c \
     LEFT JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.id = $1 AND c.workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(pool)
  .await?;
  let (version, content_text) = head.ok_or_else(missing)?;
  let content: Value = content_text
    .and_then(|t| serde_json::from_str::<Value>(&t).ok())
    .filter(|v| v.is_object())
    .unwrap_or_else(|| json!({}));
  if let Err(err) = issue_certificates(pool, Some(c.ws), None, Some(course)).await {
    tracing::warn!("carehub learning assign: certificates: {}", err);
  }
  let board = Board::load(pool, c.ws, Some(course)).await?;
  let mut conn = pool.acquire().await?;
  let groups = groups_of(&mut conn, c.ws).await?;

  // Assigned people and their effective due dates.
  let mut all = Tally::default();
  let mut due_rows: Vec<(String, Value)> = Vec::new();
  for ((cid, uid), e) in &board.eff {
    if *cid != course {
      continue;
    }
    let status = board.status(course, *uid);
    all.add(status, e.due_ms, board.now);
    let name = board
      .people
      .get(*uid)
      .map(|p| p.name.to_lowercase())
      .unwrap_or_default();
    due_rows.push((
      name,
      json!({
        "uid": uid.to_string(),
        "due_ms": e.due_ms,
        "required": e.required,
        "overdue": e.overdue(status, board.now),
        "assigned_ms": e.assigned_ms,
      }),
    ));
  }
  due_rows.sort_by(|a, b| a.0.cmp(&b.0));
  let due: Vec<Value> = due_rows.into_iter().map(|x| x.1).collect();

  // Final-quiz questions of the live version, over every attempt on it.
  let empty = json!({});
  let quiz = match content.get("quiz") {
    Some(q) if q.is_object() => q,
    _ => &empty,
  };
  let questions_src: &[Value] = if flag(quiz, "enabled", false) {
    list(quiz, "questions")
  } else {
    &[]
  };
  let attempts: Vec<(i64, String, String)> = match version {
    Some(v) if !questions_src.is_empty() => {
      sqlx::query_as(
        "SELECT uid, answers::text, results::text FROM carehub_quiz_attempt \
         WHERE course_id = $1 AND version = $2 ORDER BY created_ms DESC LIMIT $3",
      )
      .bind(course)
      .bind(v)
      .bind(QUIZ_ATTEMPTS_CAP)
      .fetch_all(&mut *conn)
      .await?
    },
    _ => Vec::new(),
  };
  let parsed: Vec<(i64, Value, Value)> = attempts
    .into_iter()
    .map(|(uid, a, r)| {
      (
        uid,
        serde_json::from_str::<Value>(&a).unwrap_or(Value::Null),
        serde_json::from_str::<Value>(&r).unwrap_or(Value::Null),
      )
    })
    .collect();
  let questions: Vec<Value> = questions_src
    .iter()
    .map(|q| question_stats(q, course, &parsed))
    .collect();

  // Lessons (drop-off) and knowledge checks, over current members' records.
  let records: Vec<(i64, String, String)> = sqlx::query_as(
    "SELECT e.uid, e.status, e.progress::text FROM carehub_enrollment e \
     JOIN af_workspace_member m ON m.uid = e.uid AND m.workspace_id = $2 \
     JOIN af_user u ON u.uid = e.uid \
     WHERE e.course_id = $1 AND m.role_id::INT <> $3 \
       AND COALESCE(u.email, '') NOT LIKE '%.removed-%'",
  )
  .bind(course)
  .bind(c.ws)
  .bind(guest_role())
  .fetch_all(&mut *conn)
  .await?;
  let opens: Vec<(i64, String)> = sqlx::query_as(
    "SELECT DISTINCT uid, lesson_id FROM carehub_learning_event \
     WHERE course_id = $1 AND kind = 'lesson_open'",
  )
  .bind(course)
  .fetch_all(&mut *conn)
  .await?;
  let enrolled: HashSet<i64> = records.iter().map(|r| r.0).collect();
  let mut opened_by: HashMap<String, HashSet<i64>> = HashMap::new();
  for (uid, lesson) in &opens {
    if enrolled.contains(uid) {
      opened_by.entry(lesson.clone()).or_default().insert(*uid);
    }
  }
  let mut done_by: HashMap<String, i64> = HashMap::new();
  let mut stopped_by: HashMap<String, i64> = HashMap::new();
  let mut kc_stats: HashMap<String, (i64, i64)> = HashMap::new();
  for (uid, status, text) in &records {
    let p = serde_json::from_str::<Value>(text).unwrap_or(Value::Null);
    if let Some(lessons) = p.get("lessons").and_then(|x| x.as_object()) {
      for (lid, l) in lessons {
        let done = flag(l, "done", false);
        if done || num(l, "active_ms", 0) > 0 || !list(l, "blocks_done").is_empty() {
          opened_by.entry(lid.clone()).or_default().insert(*uid);
        }
        if done {
          *done_by.entry(lid.clone()).or_insert(0) += 1;
        }
      }
    }
    if let Some(pos) = p.get("position") {
      let lid = txt(pos, "lesson_id");
      if !lid.is_empty() {
        opened_by.entry(lid.to_string()).or_default().insert(*uid);
        if !finished(status) {
          *stopped_by.entry(lid.to_string()).or_insert(0) += 1;
        }
      }
    }
    if let Some(kc) = p.get("kc").and_then(|x| x.as_object()) {
      for (block_id, entry) in kc {
        let s = kc_stats.entry(block_id.clone()).or_insert((0, 0));
        s.0 += 1;
        if flag(entry, "correct", false) {
          s.1 += 1;
        }
      }
    }
  }
  let mut lessons: Vec<Value> = Vec::new();
  let mut checks: Vec<Value> = Vec::new();
  for (i, lesson) in list(&content, "lessons").iter().enumerate() {
    let lid = txt(lesson, "id");
    let title = match txt(lesson, "title").trim() {
      "" => format!("Lesson {}", i + 1),
      t => t.to_string(),
    };
    lessons.push(json!({
      "id": lid,
      "title": title,
      "opened": opened_by.get(lid).map(|s| s.len()).unwrap_or(0),
      "done": done_by.get(lid).copied().unwrap_or(0),
      "stopped_here": stopped_by.get(lid).copied().unwrap_or(0),
    }));
    for b in list(lesson, "blocks") {
      if txt(b, "type") != "knowledge_check" {
        continue;
      }
      let block_id = txt(b, "id");
      let (answered, right) = kc_stats.get(block_id).copied().unwrap_or((0, 0));
      let prompt = b.get("question").map(|q| txt(q, "prompt")).unwrap_or("");
      checks.push(json!({
        "block_id": block_id,
        "lesson_id": lid,
        "lesson_title": title,
        "prompt": prompt,
        "answered": answered,
        "correct_pct": pct(right, answered),
      }));
    }
  }

  // Groups: their members who are assigned this course.
  let mut group_rows: Vec<Value> = Vec::new();
  for g in &groups {
    let mut t = Tally::default();
    let mut members: i64 = 0;
    for uid in &g.members {
      if !board.people.has(*uid) {
        continue;
      }
      members += 1;
      if let Some(e) = board.eff.get(&(course, *uid)) {
        t.add(board.status(course, *uid), e.due_ms, board.now);
      }
    }
    let v = json!({ "id": g.id.to_string(), "name": g.name, "members": members });
    group_rows.push(with_counts(v, &t, "assigned"));
  }

  let certs: Vec<(i64, String, i64)> = sqlx::query_as(
    "SELECT uid, code, issued_ms FROM carehub_learning_certificate \
     WHERE course_id = $1 AND workspace_id = $2 ORDER BY issued_ms DESC, code",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_all(&mut *conn)
  .await?;
  let certificates: Vec<Value> = certs
    .iter()
    .map(|r| json!({ "uid": r.0.to_string(), "code": r.1, "issued_ms": r.2 }))
    .collect();

  Ok(json!({
    "assigned": all.assigned,
    "overdue": all.overdue,
    "due": due,
    "questions": questions,
    "knowledge_checks": checks,
    "lessons": lessons,
    "groups": group_rows,
    "certificates": certificates,
  }))
}

/// GET /reports/teams: person-course pairs over live (published) courses.
async fn teams(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_reports(&c)?;
  let pool = &state.pg_pool;
  let board = Board::load(pool, c.ws, None).await?;
  let mut conn = pool.acquire().await?;
  let groups = groups_of(&mut conn, c.ws).await?;
  let live: HashSet<Uuid> = board
    .courses
    .iter()
    .filter(|(_, course)| course.status == "published")
    .map(|(id, _)| *id)
    .collect();
  let mut by_person: HashMap<i64, Vec<Uuid>> = HashMap::new();
  let mut per_course: HashMap<Uuid, Tally> = HashMap::new();
  for ((course, uid), e) in &board.eff {
    if !live.contains(course) {
      continue;
    }
    by_person.entry(*uid).or_default().push(*course);
    per_course
      .entry(*course)
      .or_default()
      .add(board.status(*course, *uid), e.due_ms, board.now);
  }
  let mut group_rows: Vec<Value> = Vec::new();
  for g in &groups {
    let mut t = Tally::default();
    let mut members: i64 = 0;
    for uid in &g.members {
      if !board.people.has(*uid) {
        continue;
      }
      members += 1;
      if let Some(courses) = by_person.get(uid) {
        for course in courses {
          t.add(board.status(*course, *uid), board.due(*course, *uid), board.now);
        }
      }
    }
    let v = json!({ "id": g.id.to_string(), "name": g.name, "members": members });
    group_rows.push(with_counts(v, &t, "assigned"));
  }
  let mut course_rows: Vec<(String, Value)> = Vec::new();
  for (id, course) in &board.courses {
    let t = match per_course.get(id) {
      Some(t) => *t,
      None => continue,
    };
    let v = json!({ "course_id": id.to_string(), "title": course.title });
    course_rows.push((course.title.to_lowercase(), with_counts(v, &t, "assigned")));
  }
  course_rows.sort_by(|a, b| a.0.cmp(&b.0));
  let courses: Vec<Value> = course_rows.into_iter().map(|x| x.1).collect();
  Ok(json!({ "groups": group_rows, "courses": courses }))
}

// ---------------------------------------------------------------------------
// Reminders (claim before send) and calendar entries
// ---------------------------------------------------------------------------

/// (course, uid, effective due, first assigned, required, tz of that due,
/// assigned message sent, assigned message pending, due reminder handled,
/// overdue reminder handled)
type PairRow = (Uuid, i64, Option<i64>, i64, bool, i32, bool, bool, bool, bool);

async fn send(pool: &PgPool, uid: i64, body: &str) {
  if let Err(err) = carehub_extras::notify_user(pool, uid, body).await {
    warn_rarely(&format!("could not send a learning message: {}", err));
  }
}

/// Marks the person's pending "assigned" rows for a course; returns how many.
async fn claim_assigned(pool: &PgPool, course: Uuid, uid: i64, value: i64) -> Result<u64, AppError> {
  let done = sqlx::query(
    "UPDATE carehub_learning_assignee SET sent_assigned_ms = $3 \
     WHERE course_id = $1 AND uid = $2 AND sent_assigned_ms = 0",
  )
  .bind(course)
  .bind(uid)
  .bind(value)
  .execute(pool)
  .await?;
  Ok(done.rows_affected())
}

/// Marks the person's pending due (or overdue) reminder rows for the
/// assignments with that due date; returns how many.
async fn claim_due(
  pool: &PgPool,
  course: Uuid,
  uid: i64,
  due: i64,
  overdue: bool,
  value: i64,
) -> Result<u64, AppError> {
  let sql = if overdue {
    "UPDATE carehub_learning_assignee s SET sent_overdue_ms = $4 \
     FROM carehub_learning_assignment a \
     WHERE a.id = s.assignment_id AND s.course_id = $1 AND s.uid = $2 AND a.due_ms = $3 \
       AND s.sent_overdue_ms = 0"
  } else {
    "UPDATE carehub_learning_assignee s SET sent_due_ms = $4 \
     FROM carehub_learning_assignment a \
     WHERE a.id = s.assignment_id AND s.course_id = $1 AND s.uid = $2 AND a.due_ms = $3 \
       AND s.sent_due_ms = 0"
  };
  let done = sqlx::query(sql)
    .bind(course)
    .bind(uid)
    .bind(due)
    .bind(value)
    .execute(pool)
    .await?;
  Ok(done.rows_affected())
}

/// Sends what is due: the "assigned" message once per person and course, the
/// due-tomorrow reminder at 09:00 local the day before, the overdue one at
/// 09:00 local the day after. Never to people who finished, left, or are on
/// unpublished / archived courses. `only` limits the work to some courses.
async fn deliver(pool: &PgPool, only: Option<Vec<Uuid>>) -> Result<(), AppError> {
  ready(pool).await?;
  let now = now_ms();
  let rows: Vec<PairRow> = sqlx::query_as(
    "SELECT g.course_id, g.uid, g.eff_due, g.first_ms, g.req, g.tz, \
       g.a_done, g.a_pending, g.d_done, g.o_done \
     FROM ( \
       SELECT r.course_id, r.uid, r.eff_due, r.first_ms, r.req, r.tz, \
         BOOL_OR(r.sent_assigned_ms > 0) AS a_done, \
         BOOL_OR(r.sent_assigned_ms = 0) AS a_pending, \
         COALESCE(BOOL_OR(r.due_ms = r.eff_due AND r.sent_due_ms <> 0), FALSE) AS d_done, \
         COALESCE(BOOL_OR(r.due_ms = r.eff_due AND r.sent_overdue_ms <> 0), FALSE) AS o_done \
       FROM ( \
         SELECT s.course_id, s.uid, s.sent_assigned_ms, s.sent_due_ms, s.sent_overdue_ms, \
           a.due_ms, MIN(a.due_ms) OVER w AS eff_due, MIN(s.assigned_ms) OVER w AS first_ms, \
           BOOL_OR(a.required) OVER w AS req, \
           FIRST_VALUE(a.tz_minutes) OVER (PARTITION BY s.course_id, s.uid \
             ORDER BY a.due_ms ASC NULLS LAST) AS tz \
         FROM carehub_learning_assignee s \
         JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
         JOIN carehub_course c ON c.id = s.course_id \
         JOIN af_workspace_member m ON m.workspace_id = c.workspace_id AND m.uid = s.uid \
         JOIN af_user u ON u.uid = s.uid \
         WHERE c.status = 'published' AND m.role_id::INT <> $3 \
           AND COALESCE(u.email, '') NOT LIKE '%.removed-%' \
           AND ($2::UUID[] IS NULL OR s.course_id = ANY($2)) \
         WINDOW w AS (PARTITION BY s.course_id, s.uid) \
       ) r \
       WHERE NOT EXISTS (SELECT 1 FROM carehub_enrollment e WHERE e.course_id = r.course_id \
         AND e.uid = r.uid AND e.status IN ('completed', 'passed')) \
       GROUP BY r.course_id, r.uid, r.eff_due, r.first_ms, r.req, r.tz \
     ) g \
     WHERE g.a_pending \
       OR (g.eff_due IS NOT NULL AND NOT g.d_done AND ((g.eff_due + g.tz::BIGINT * 60000) \
         / 86400000 - 1) * 86400000 + 32400000 - g.tz::BIGINT * 60000 <= $1) \
       OR (g.eff_due IS NOT NULL AND NOT g.o_done AND ((g.eff_due + g.tz::BIGINT * 60000) \
         / 86400000 + 1) * 86400000 + 32400000 - g.tz::BIGINT * 60000 <= $1) \
     LIMIT $4",
  )
  .bind(now)
  .bind(only)
  .bind(guest_role())
  .bind(TICK_PAIRS)
  .fetch_all(pool)
  .await?;
  if rows.is_empty() {
    return Ok(());
  }
  let mut ids: Vec<Uuid> = rows.iter().map(|r| r.0).collect();
  ids.sort();
  ids.dedup();
  let titles = titles_of(pool, &ids).await?;
  for (course, uid, eff_due, first_ms, required, tz, a_done, a_pending, d_done, o_done) in rows {
    let title = titles
      .get(&course)
      .cloned()
      .unwrap_or_else(|| "Untitled course".to_string());
    let link = course_link(course);
    if a_pending {
      if a_done {
        // Already told about this course through another assignment.
        claim_assigned(pool, course, uid, -1).await?;
      } else if claim_assigned(pool, course, uid, now).await? > 0 {
        let due_part = eff_due
          .map(|d| format!(" — due {}", day_label(local_day(d, tz))))
          .unwrap_or_default();
        let required_part = if required { " (required)" } else { "" };
        let body = format!(
          "📚 New course for you: “{}”{}{}.\n{}",
          title, due_part, required_part, link
        );
        send(pool, uid, &body).await;
      }
    }
    let due = match eff_due {
      Some(d) => d,
      None => continue,
    };
    let day = local_day(due, tz);
    let remind_from = day_start(day - 1, tz) + REMIND_HOUR * HOUR_MS;
    let overdue_from = day_start(day + 1, tz) + REMIND_HOUR * HOUR_MS;
    if !d_done && now >= remind_from {
      // Too late (already the due day) or assigned after the reminder time:
      // the "assigned" message already showed the date.
      if now >= day_start(day, tz) || first_ms >= remind_from {
        claim_due(pool, course, uid, due, false, -1).await?;
      } else if claim_due(pool, course, uid, due, false, now).await? > 0 {
        let body = format!(
          "⏰ Reminder: “{}” is due tomorrow ({}).\n{}",
          title,
          day_label(day),
          link
        );
        send(pool, uid, &body).await;
      }
    }
    if !o_done && now >= overdue_from {
      if first_ms >= due {
        // Assigned after the due date had passed: no overdue nagging.
        claim_due(pool, course, uid, due, true, -1).await?;
      } else if claim_due(pool, course, uid, due, true, now).await? > 0 {
        let body = format!(
          "⚠️ “{}” was due {}. Please finish it when you can.\n{}",
          title,
          day_label(day),
          link
        );
        send(pool, uid, &body).await;
      }
    }
  }
  Ok(())
}

fn calendar_event_id(course: Uuid, uid: i64) -> Uuid {
  Uuid::new_v5(
    &Uuid::NAMESPACE_OID,
    format!("carehub-learning-due:{}:{}", course, uid).as_bytes(),
  )
}

/// One all-day entry per person and course on the local due date (the
/// earliest due among assignments that asked for a calendar entry). Moved when
/// the date changes, deleted when nothing covers the person any more; left
/// alone once they finish.
async fn sync_calendar(pool: &PgPool, only: Option<Vec<Uuid>>) -> Result<(), AppError> {
  ready(pool).await?;
  carehub_calendar::ensure_tables(pool).await?;
  let want: Vec<(Uuid, i64, Uuid, i64, i32)> = sqlx::query_as(
    "SELECT DISTINCT ON (s.course_id, s.uid) s.course_id, s.uid, c.workspace_id, a.due_ms, \
       a.tz_minutes \
     FROM carehub_learning_assignee s \
     JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
     JOIN carehub_course c ON c.id = s.course_id \
     JOIN af_workspace_member m ON m.workspace_id = c.workspace_id AND m.uid = s.uid \
     JOIN af_user u ON u.uid = s.uid \
     WHERE a.calendar AND a.due_ms IS NOT NULL AND c.status = 'published' \
       AND m.role_id::INT <> $1 AND COALESCE(u.email, '') NOT LIKE '%.removed-%' \
       AND ($2::UUID[] IS NULL OR s.course_id = ANY($2)) \
     ORDER BY s.course_id, s.uid, a.due_ms ASC",
  )
  .bind(guest_role())
  .bind(only.clone())
  .fetch_all(pool)
  .await?;
  let have: Vec<(Uuid, i64, Uuid, i64)> = sqlx::query_as(
    "SELECT course_id, uid, event_id, start_ms FROM carehub_learning_calendar \
     WHERE ($1::UUID[] IS NULL OR course_id = ANY($1))",
  )
  .bind(only)
  .fetch_all(pool)
  .await?;
  let have_start: HashMap<(Uuid, i64), i64> = have.iter().map(|r| ((r.0, r.1), r.3)).collect();
  let mut wanted: HashSet<(Uuid, i64)> = HashSet::new();
  let mut todo: Vec<(Uuid, i64, Uuid, i64)> = Vec::new();
  for (course, uid, ws, due, tz) in &want {
    wanted.insert((*course, *uid));
    let start = day_start(local_day(*due, *tz), *tz);
    if have_start.get(&(*course, *uid)) != Some(&start) {
      todo.push((*course, *uid, *ws, start));
    }
  }
  for (course, uid, event, _) in &have {
    if wanted.contains(&(*course, *uid)) {
      continue;
    }
    sqlx::query("DELETE FROM carehub_calendar_event WHERE id = $1 AND owner = $2")
      .bind(*event)
      .bind(*uid)
      .execute(pool)
      .await?;
    sqlx::query("DELETE FROM carehub_learning_calendar WHERE course_id = $1 AND uid = $2")
      .bind(*course)
      .bind(*uid)
      .execute(pool)
      .await?;
  }
  if todo.is_empty() {
    return Ok(());
  }
  todo.truncate(CALENDAR_BATCH);
  let mut ids: Vec<Uuid> = todo.iter().map(|t| t.0).collect();
  ids.sort();
  ids.dedup();
  let done: Vec<(Uuid, i64)> = sqlx::query_as(
    "SELECT course_id, uid FROM carehub_enrollment \
     WHERE course_id = ANY($1) AND status IN ('completed', 'passed')",
  )
  .bind(ids.clone())
  .fetch_all(pool)
  .await?;
  let done: HashSet<(Uuid, i64)> = done.into_iter().collect();
  let titles = titles_of(pool, &ids).await?;
  let now = now_ms();
  for (course, uid, ws, start) in todo {
    let event = calendar_event_id(course, uid);
    // Finished: the entry stays where it is (none is made); only remember the
    // date so this person isn't looked at again every minute.
    if !done.contains(&(course, uid)) {
      let title = format!(
        "📚 Due: {}",
        titles
          .get(&course)
          .map(|t| t.as_str())
          .unwrap_or("Untitled course")
      );
      sqlx::query(
        "INSERT INTO carehub_calendar_event \
           (id, owner, title, start_ms, end_ms, all_day, location, notes, color) \
         VALUES ($1, $2, $3, $4, $5, TRUE, '', $6, '') \
         ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, start_ms = EXCLUDED.start_ms, \
           end_ms = EXCLUDED.end_ms, all_day = TRUE, notes = EXCLUDED.notes, updated_at = NOW() \
         WHERE carehub_calendar_event.owner = EXCLUDED.owner",
      )
      .bind(event)
      .bind(uid)
      .bind(clip(&title, 500))
      .bind(start)
      .bind(start + DAY_MS)
      .bind(format!(
        "Assigned course in Care Hub Learning: {}",
        course_path(course)
      ))
      .execute(pool)
      .await?;
    }
    sqlx::query(
      "INSERT INTO carehub_learning_calendar \
         (course_id, uid, workspace_id, event_id, start_ms, updated_ms) \
       VALUES ($1, $2, $3, $4, $5, $6) \
       ON CONFLICT (course_id, uid) DO UPDATE SET event_id = EXCLUDED.event_id, \
         start_ms = EXCLUDED.start_ms, updated_ms = EXCLUDED.updated_ms",
    )
    .bind(course)
    .bind(uid)
    .bind(ws)
    .bind(event)
    .bind(start)
    .bind(now)
    .execute(pool)
    .await?;
  }
  Ok(())
}

// ---------------------------------------------------------------------------
// Background task
// ---------------------------------------------------------------------------

/// Role / everyone / group targets follow the workspace: new members get the
/// course, people who left the role / group / workspace lose unfinished rows.
async fn sync_targets(pool: &PgPool, ws: Uuid) -> Result<(), AppError> {
  let people = people_of(pool, ws).await?;
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, ws).await?;
  let asgs: Vec<(Uuid, Uuid, String, String, bool)> = sqlx::query_as(
    "SELECT a.id, a.course_id, a.target_kind, a.target_id, a.notify \
     FROM carehub_learning_assignment a JOIN carehub_course c ON c.id = a.course_id \
     WHERE a.workspace_id = $1 AND c.status = 'published' \
       AND a.target_kind IN ('role', 'everyone', 'group')",
  )
  .bind(ws)
  .fetch_all(&mut *tx)
  .await?;
  if asgs.is_empty() {
    return Ok(());
  }
  let groups = groups_of(&mut tx, ws).await?;
  let have: Vec<(Uuid, i64)> = sqlx::query_as(
    "SELECT s.assignment_id, s.uid FROM carehub_learning_assignee s \
     JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
     WHERE a.workspace_id = $1 AND a.target_kind IN ('role', 'everyone', 'group')",
  )
  .bind(ws)
  .fetch_all(&mut *tx)
  .await?;
  let done: Vec<(Uuid, i64)> = sqlx::query_as(
    "SELECT e.course_id, e.uid FROM carehub_enrollment e \
     JOIN carehub_course c ON c.id = e.course_id \
     WHERE c.workspace_id = $1 AND e.status IN ('completed', 'passed')",
  )
  .bind(ws)
  .fetch_all(&mut *tx)
  .await?;
  let done: HashSet<(Uuid, i64)> = done.into_iter().collect();
  let mut by_asg: HashMap<Uuid, HashSet<i64>> = HashMap::new();
  for (asg, uid) in have {
    by_asg.entry(asg).or_default().insert(uid);
  }
  let none: HashSet<i64> = HashSet::new();
  let mut todo: Vec<(Uuid, Uuid, bool, Vec<i64>)> = Vec::new();
  for (id, course, kind, target, notify) in asgs {
    let want = want_for(&kind, &target, &people, &groups);
    let want_set: HashSet<i64> = want.iter().copied().collect();
    let have_set = by_asg.get(&id).unwrap_or(&none);
    let missing_people = want.iter().any(|uid| !have_set.contains(uid));
    let extra_people = have_set
      .iter()
      .any(|uid| !want_set.contains(uid) && !done.contains(&(course, *uid)));
    if missing_people || extra_people {
      todo.push((id, course, notify, want));
    }
  }
  if todo.is_empty() {
    return Ok(());
  }
  let now = now_ms();
  for (id, course, notify, want) in &todo {
    sync_assignment(&mut tx, *id, *course, ws, *notify, want, now).await?;
  }
  tx.commit().await?;
  Ok(())
}

async fn tick(pool: &PgPool) -> Result<(), AppError> {
  carehub_extras::ensure_tables(pool).await?;
  ready(pool).await?;
  let spaces: Vec<Uuid> = sqlx::query_scalar(
    "SELECT DISTINCT workspace_id FROM carehub_learning_assignment WHERE target_kind <> 'person'",
  )
  .fetch_all(pool)
  .await?;
  for ws in spaces {
    if let Err(err) = sync_targets(pool, ws).await {
      warn_rarely(&format!("could not refresh assignments: {}", err));
    }
  }
  if let Err(err) = deliver(pool, None).await {
    warn_rarely(&format!("reminders: {}", err));
  }
  if let Err(err) = sync_calendar(pool, None).await {
    warn_rarely(&format!("calendar entries: {}", err));
  }
  if let Err(err) = issue_certificates(pool, None, None, None).await {
    warn_rarely(&format!("certificates: {}", err));
  }
  Ok(())
}

/// Started once from application.rs.
pub fn start_background(pool: PgPool) {
  tokio::spawn(async move {
    tokio::time::sleep(std::time::Duration::from_secs(25)).await;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
      interval.tick().await;
      if let Err(err) = tick(&pool).await {
        warn_rarely(&format!("background task: {}", err));
      }
    }
  });
}

// ---------------------------------------------------------------------------
// HTTP handlers and routes
// ---------------------------------------------------------------------------

async fn members_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  reply(members(&state, &user, &ws).await)
}

async fn groups_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  reply(groups(&state, &user, &ws).await)
}

async fn group_create_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
  body: Bytes,
) -> Reply {
  let ws = path.into_inner();
  reply(create_group(&state, &user, &ws, &body).await)
}

async fn group_update_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(update_group(&state, &user, &ws, &id, &body).await)
}

async fn group_delete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(delete_group(&state, &user, &ws, &id).await)
}

async fn assignments_list_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
  query: web::Query<HashMap<String, String>>,
) -> Reply {
  let ws = path.into_inner();
  reply(list_assignments(&state, &user, &ws, &query).await)
}

async fn assignments_create_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
  body: Bytes,
) -> Reply {
  let ws = path.into_inner();
  reply(create_assignments(&state, &user, &ws, &body).await)
}

async fn assignment_get_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(get_assignment(&state, &user, &ws, &id).await)
}

async fn assignment_update_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(update_assignment(&state, &user, &ws, &id, &body).await)
}

async fn assignment_delete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(delete_assignment(&state, &user, &ws, &id).await)
}

async fn assigned_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  reply(assigned(&state, &user, &ws).await)
}

async fn my_certificates_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
) -> Reply {
  let ws = path.into_inner();
  reply(my_certificates(&state, &user, &ws).await)
}

async fn certificate_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, code) = path.into_inner();
  reply(verify_certificate(&state, &user, &ws, &code).await)
}

async fn insights_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(insights(&state, &user, &ws, &id).await)
}

async fn teams_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  reply(teams(&state, &user, &ws).await)
}

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/members").route(web::get().to(members_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/groups")
        .route(web::get().to(groups_handler))
        .route(web::post().to(group_create_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/groups/{id}")
        .route(web::put().to(group_update_handler))
        .route(web::delete().to(group_delete_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/assignments")
        .route(web::get().to(assignments_list_handler))
        .route(web::post().to(assignments_create_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/assignments/{id}")
        .route(web::get().to(assignment_get_handler))
        .route(web::put().to(assignment_update_handler))
        .route(web::delete().to(assignment_delete_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/assigned").route(web::get().to(assigned_handler)),
    )
    // "mine" first, so it isn't read as a certificate code.
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/certificates/mine")
        .route(web::get().to(my_certificates_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/certificates/{code}")
        .route(web::get().to(certificate_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/courses/{id}/insights")
        .route(web::get().to(insights_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/teams")
        .route(web::get().to(teams_handler)),
    );
}
