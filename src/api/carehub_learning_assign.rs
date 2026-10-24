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
//!
//! Lane paths3 (hunks marked P1 due_days / P2 recert / P3 paths): due "N
//! days after they're added", renewal of finishes after N months (heads-up,
//! progress reset, renewal due) and the assignment side of learning paths
//! (carehub_learning_paths.rs).
//!
//! Lane notices (hunks marked P6 notices): after a new version is published,
//! telling the people who finished an earlier one what changed (or asking
//! them to take it again), "check this course" reminders for course owners,
//! marking someone complete by hand (and undoing it) and the learning path
//! report (carehub_learning_paths.rs).

use crate::api::carehub_calendar;
use crate::api::carehub_extras;
use crate::api::carehub_learning::{self, CardRow, Ctx, MyRow};
use crate::api::carehub_learning_paths;
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
/// P2 recert: settings.recert_months per published version (None = never).
static RECERT: Mutex<BTreeMap<(Uuid, i32), Option<i64>>> = Mutex::new(BTreeMap::new());
/// P6 notices: settings.review_months per published version (None = never).
static REVIEW: Mutex<BTreeMap<(Uuid, i32), Option<i64>>> = Mutex::new(BTreeMap::new());

const TABLE_LOCK_KEY: i64 = 7_316_402_220;
const WS_LOCK_CLASS: i32 = 7_316_403;
pub(crate) const DAY_MS: i64 = 86_400_000;
pub(crate) const HOUR_MS: i64 = 3_600_000;
/// Reminders go out at 09:00 in the assignment's time zone.
pub(crate) const REMIND_HOUR: i64 = 9;
const MAX_TARGETS: usize = 200;
const MAX_NOTE_CHARS: usize = 500;
const MAX_GROUP_NAME_CHARS: usize = 80;
const MAX_GROUP_DESCRIPTION_CHARS: usize = 500;
const MAX_GROUP_PEOPLE: usize = 1000;
/// 2200-01-01; anything later is a typo.
const MAX_DUE_MS: i64 = 7_258_118_400_000;
const TICK_PAIRS: i64 = 500;
const CERT_BATCH: i64 = 500;
pub(crate) const CALENDAR_BATCH: usize = 1000;
/// P1 due_days: "due N days after they're added", N = 1-365.
const MAX_DUE_DAYS: i64 = 365;
/// P2 recert: heads-ups / renewals handled per run (the rest wait a minute).
const RENEW_BATCH: i64 = 200;
/// P2 recert: a finish of someone who isn't assigned is looked at again after this.
const RECHECK_MS: i64 = 600_000;
const MAX_RECERT_MONTHS: i64 = 60;
/// P2 recert: days from the heads-up to the day a finish runs out (1-month
/// renewals: 7), and from a renewal to its due.
const HEADS_UP_DAYS: i64 = 30;
const HEADS_UP_DAYS_MONTHLY: i64 = 7;
const RENEW_DAYS: i64 = 30;
const QUIZ_ATTEMPTS_CAP: i64 = 5000;
/// No 0/O/1/I/L, so codes can be read out and typed without mistakes.
const CODE_ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
/// P6 notices: "check this course" every 1-24 months; reminders per run.
const MAX_REVIEW_MONTHS: i64 = 24;
const REVIEW_BATCH: usize = 50;
/// P6 notices: the reason for marking someone complete.
const MAX_REASON_CHARS: usize = 200;
/// P6 notices: default days to take a course again after an update.
const RETAKE_DAYS: i32 = 14;
/// P6 notices: certificate rounds (CERT_BATCH each) before a "take it again".
const CERT_ROUNDS: usize = 20;

const SETUP_MSG: &str = "Learning is still being set up. Please try again in a minute.";
const SERVER_MSG: &str = "Something went wrong on the server. Please try again in a moment.";
const BODY_MSG: &str = "That request couldn't be read. Please reload the page and try again.";
pub(crate) const COURSE_LINK_MSG: &str = "That course link doesn't look right.";
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
// P1 due_days
const BOTH_DUE_MSG: &str = "Pick a due date or a number of days, not both.";
const DAYS_MSG: &str = "Pick a number of days between 1 and 365.";
// P2 recert
const RETAKE_MSG: &str = "You can take this course again once it runs out.";
const RECERT_MSG: &str = "Renewal must be between 1 and 60 months (or leave it empty).";
// P6 notices
const BUILD_MSG: &str =
  "You need the \"Build courses\" permission for this. Ask an admin to update your role.";
const VERSION_MSG: &str =
  "That isn't the version that's live now. Please reload the page and try again.";
const LIVE_MSG: &str = "This course isn't published right now, so nobody was told.";
const CHANGES_MSG: &str = "Add a note about what changed before telling people.";
const PERSON_LINK_MSG: &str = "That person link doesn't look right.";
const NOT_ON_MSG: &str =
  "That person isn't on this assignment any more. Please reload the page and try again.";
const DONE_MSG: &str = "They've already finished this course.";
const REASON_MSG: &str = "Keep the reason to 200 characters or fewer.";
const UNDO_MSG: &str =
  "There's nothing to undo: their progress has changed since they were marked complete.";
const MARK_DRAFT_MSG: &str = "Publish this course before marking anyone complete.";
const MARK_ARCHIVED_MSG: &str =
  "This course is archived. Unarchive it before marking anyone complete.";

/// P1 due_days / P2 recert: the due of one person's row (s = assignee, a =
/// assignment) in SQL: a renewal due, else 23:59:59.999 local on the day N
/// days after they were added (or N was set), else the date. Twin: seat_due.
pub(crate) const ROW_DUE: &str = "COALESCE(s.renew_due_ms, CASE WHEN a.due_days IS NULL \
  THEN a.due_ms ELSE ((GREATEST(s.assigned_ms, a.days_from_ms) + a.tz_minutes::BIGINT * 60000) \
  / 86400000 + a.due_days + 1) * 86400000 - a.tz_minutes::BIGINT * 60000 - 1 END)";

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
  // P3 paths (the table first: assignments point at it)
  "CREATE TABLE IF NOT EXISTS carehub_learning_path (\
     id UUID PRIMARY KEY, workspace_id UUID NOT NULL, title TEXT NOT NULL, \
     description TEXT NOT NULL DEFAULT '', course_ids UUID[] NOT NULL DEFAULT '{}', \
     in_order BOOLEAN NOT NULL DEFAULT TRUE, archived BOOLEAN NOT NULL DEFAULT FALSE, \
     created_by BIGINT NOT NULL DEFAULT 0, created_ms BIGINT NOT NULL DEFAULT 0, \
     updated_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE UNIQUE INDEX IF NOT EXISTS carehub_learning_path_title \
     ON carehub_learning_path (workspace_id, LOWER(title))",
  "CREATE TABLE IF NOT EXISTS carehub_learning_path_calendar (\
     path_id UUID NOT NULL, uid BIGINT NOT NULL, workspace_id UUID NOT NULL, \
     event_id UUID NOT NULL, start_ms BIGINT NOT NULL, updated_ms BIGINT NOT NULL DEFAULT 0, \
     PRIMARY KEY (path_id, uid))",
  // P1 due_days
  "ALTER TABLE carehub_learning_assignment ADD COLUMN IF NOT EXISTS due_days INT",
  "ALTER TABLE carehub_learning_assignment \
     ADD COLUMN IF NOT EXISTS days_from_ms BIGINT NOT NULL DEFAULT 0",
  // P3 paths: null = a direct assignment.
  "ALTER TABLE carehub_learning_assignment ADD COLUMN IF NOT EXISTS path_id UUID \
     REFERENCES carehub_learning_path (id) ON DELETE CASCADE",
  "CREATE INDEX IF NOT EXISTS carehub_learning_assignment_path \
     ON carehub_learning_assignment (path_id)",
  // P3 paths: one direct assignment per course + target, and one per path. The
  // index keeps its name, so an older build's CREATE IF NOT EXISTS is a no-op.
  "DO $$ BEGIN \
     IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() \
       AND indexname = 'carehub_learning_assignment_target' AND indexdef LIKE '%path_id%') THEN \
       DROP INDEX IF EXISTS carehub_learning_assignment_target; \
       CREATE UNIQUE INDEX carehub_learning_assignment_target ON carehub_learning_assignment \
         (course_id, target_kind, target_id, \
          COALESCE(path_id, '00000000-0000-0000-0000-000000000000'::UUID)); \
     END IF; \
   END $$",
  // P2 recert
  "ALTER TABLE carehub_learning_assignee ADD COLUMN IF NOT EXISTS renew_due_ms BIGINT",
  "CREATE INDEX IF NOT EXISTS carehub_learning_assignee_renew \
     ON carehub_learning_assignee (course_id, uid) WHERE renew_due_ms IS NOT NULL",
  "CREATE TABLE IF NOT EXISTS carehub_learning_renewal (\
     course_id UUID NOT NULL REFERENCES carehub_course (id) ON DELETE CASCADE, \
     uid BIGINT NOT NULL, finished_ms BIGINT NOT NULL, workspace_id UUID NOT NULL, \
     months BIGINT NOT NULL DEFAULT 0, expires_ms BIGINT NOT NULL DEFAULT 0, \
     wake_ms BIGINT NOT NULL DEFAULT 0, heads_up_ms BIGINT NOT NULL DEFAULT 0, \
     renewed_ms BIGINT NOT NULL DEFAULT 0, due_ms BIGINT, \
     PRIMARY KEY (course_id, uid, finished_ms))",
  // P6 notices: the last version people were told about (once per version),
  // the "check this course" clock and finishes marked by hand (kept after an
  // undo, as the record).
  "CREATE TABLE IF NOT EXISTS carehub_learning_announce (\
     course_id UUID PRIMARY KEY REFERENCES carehub_course (id) ON DELETE CASCADE, \
     workspace_id UUID NOT NULL, version INT NOT NULL, action TEXT NOT NULL, due_days INT, \
     people INT NOT NULL DEFAULT 0, by_uid BIGINT NOT NULL, created_ms BIGINT NOT NULL)",
  "CREATE TABLE IF NOT EXISTS carehub_learning_review (\
     course_id UUID PRIMARY KEY REFERENCES carehub_course (id) ON DELETE CASCADE, \
     workspace_id UUID NOT NULL, checked_ms BIGINT NOT NULL DEFAULT 0, checked_by BIGINT, \
     sent_due_ms BIGINT NOT NULL DEFAULT 0, sent_ms BIGINT NOT NULL DEFAULT 0, sent_to BIGINT)",
  "CREATE TABLE IF NOT EXISTS carehub_learning_manual (\
     course_id UUID NOT NULL REFERENCES carehub_course (id) ON DELETE CASCADE, \
     uid BIGINT NOT NULL, completed_ms BIGINT NOT NULL, workspace_id UUID NOT NULL, \
     started_ms BIGINT NOT NULL, by_uid BIGINT NOT NULL, reason TEXT NOT NULL DEFAULT '', \
     before JSONB, renew_dues JSONB NOT NULL DEFAULT '[]'::jsonb, event_id BIGINT, \
     undone_ms BIGINT, undone_by BIGINT, PRIMARY KEY (course_id, uid, completed_ms))",
  "CREATE INDEX IF NOT EXISTS carehub_learning_manual_ws \
     ON carehub_learning_manual (workspace_id, course_id)",
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

pub(crate) type Reply = ActixResult<Json<AppResponse<Value>>>;

pub(crate) fn reply(result: Result<Value, AppError>) -> Reply {
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

pub(crate) fn bad(msg: &str) -> AppError {
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
pub(crate) fn warn_rarely(what: &str) {
  let now = now_ms();
  let last = LAST_WARN_MS.load(Ordering::Relaxed);
  if now - last >= 600_000 {
    LAST_WARN_MS.store(now, Ordering::Relaxed);
    tracing::warn!("carehub learning assign: {}", what);
  }
}

pub(crate) fn now_ms() -> i64 {
  chrono::Utc::now().timestamp_millis()
}

pub(crate) fn parse_uuid(raw: &str, msg: &str) -> Result<Uuid, AppError> {
  Uuid::parse_str(raw.trim()).map_err(|_| bad(msg))
}

pub(crate) fn body_json(body: &[u8]) -> Result<Value, AppError> {
  if body.iter().all(|b| b.is_ascii_whitespace()) {
    return Ok(json!({}));
  }
  match serde_json::from_slice::<Value>(body) {
    Ok(v) if v.is_object() => Ok(v),
    _ => Err(bad(BODY_MSG)),
  }
}

pub(crate) fn txt<'a>(v: &'a Value, key: &str) -> &'a str {
  v.get(key).and_then(|x| x.as_str()).unwrap_or("")
}

pub(crate) fn list<'a>(v: &'a Value, key: &str) -> &'a [Value] {
  match v.get(key).and_then(|x| x.as_array()) {
    Some(a) => a.as_slice(),
    None => &[],
  }
}

pub(crate) fn flag(v: &Value, key: &str, default: bool) -> bool {
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

pub(crate) fn clip(s: &str, max: usize) -> String {
  s.chars().take(max).collect()
}

pub(crate) fn guest_role() -> i32 {
  AFRole::Guest.into()
}

fn uid_of(v: Option<&Value>) -> Option<i64> {
  match v {
    Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
    Some(other) => other.as_i64(),
    None => None,
  }
}

pub(crate) fn finished(status: &str) -> bool {
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
    "fill_blank" => "fill_blank",
    "ordering" => "ordering",
    "hotspot" => "hotspot",
    _ => "single",
  }
}

/// Serialises writes to assignments / groups of one workspace (requests and
/// the background re-sync), for the rest of the transaction.
pub(crate) async fn lock_ws(conn: &mut PgConnection, ws: Uuid) -> Result<(), AppError> {
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
pub(crate) fn course_link(course: Uuid) -> String {
  web_link(&course_path(course))
}

/// P3 paths: the same for any page of the app ("/app/learning").
pub(crate) fn web_link(path: &str) -> String {
  let base = std::env::var("APPFLOWY_WEB_URL").unwrap_or_default();
  let base = base.trim().trim_end_matches('/');
  if base.starts_with("https://") || base.starts_with("http://") {
    format!("{}{}", base, path)
  } else {
    path.to_string()
  }
}

/// Day number (days since 1970-01-01) of a moment in a time zone given in
/// minutes east of UTC.
pub(crate) fn local_day(ms: i64, tz: i32) -> i64 {
  (ms + i64::from(tz) * 60_000).div_euclid(DAY_MS)
}

/// UTC ms of local midnight that starts day `day`.
pub(crate) fn day_start(day: i64, tz: i32) -> i64 {
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
pub(crate) fn day_label(day: i64) -> String {
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
pub(crate) async fn ctx(state: &AppState, user: &UserUuid, ws: &str) -> Result<Ctx, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  ready(&state.pg_pool).await?;
  Ok(c)
}

pub(crate) fn need_assign(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_assign") {
    Ok(())
  } else {
    Err(bad(ASSIGN_MSG))
  }
}

pub(crate) fn need_read(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_assign") || c.has("learning_build") || c.has("learning_analytics") {
    Ok(())
  } else {
    Err(bad(READ_MSG))
  }
}

/// P6 notices: pub(crate) for the path report.
pub(crate) fn need_reports(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_build") || c.has("learning_analytics") {
    Ok(())
  } else {
    Err(bad(REPORTS_MSG))
  }
}

// ---------------------------------------------------------------------------
// Members, roles, groups, courses, assignments
// ---------------------------------------------------------------------------

pub(crate) struct Person {
  uid: i64,
  pub(crate) name: String,
  pub(crate) email: String,
  role_id: String,
  role_name: String,
}

/// Current non-guest members with their Care Hub role, plus the role list.
pub(crate) struct People {
  list: Vec<Person>,
  index: HashMap<i64, usize>,
  roles: Vec<Value>,
}

impl People {
  pub(crate) fn has(&self, uid: i64) -> bool {
    self.index.contains_key(&uid)
  }

  pub(crate) fn get(&self, uid: i64) -> Option<&Person> {
    self.index.get(&uid).and_then(|i| self.list.get(*i))
  }
}

pub(crate) async fn people_of(pool: &PgPool, ws: Uuid) -> Result<People, AppError> {
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

pub(crate) struct Group {
  id: Uuid,
  name: String,
  description: String,
  updated_ms: i64,
  members: Vec<i64>,
}

pub(crate) async fn groups_of(conn: &mut PgConnection, ws: Uuid) -> Result<Vec<Group>, AppError> {
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

pub(crate) struct Course {
  pub(crate) title: String,
  pub(crate) status: String,
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

pub(crate) async fn names_of(
  pool: &PgPool,
  uids: Vec<i64>,
) -> Result<HashMap<i64, String>, AppError> {
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

pub(crate) struct Asg {
  pub(crate) id: Uuid,
  pub(crate) course: Uuid,
  pub(crate) kind: String,
  pub(crate) target: String,
  pub(crate) due_ms: Option<i64>,
  pub(crate) tz: i32,
  pub(crate) required: bool,
  pub(crate) note: String,
  pub(crate) calendar: bool,
  pub(crate) created_by: i64,
  pub(crate) created_ms: i64,
  pub(crate) updated_ms: i64,
  /// P1 due_days: due N days after each person is added (or N was set).
  pub(crate) due_days: Option<i32>,
  days_from_ms: i64,
  /// P3 paths: the path this assignment belongs to (None = direct).
  pub(crate) path: Option<Uuid>,
}

impl Asg {
  /// P1 due_days: one person's due on this assignment.
  pub(crate) fn due_for(&self, assigned_ms: i64, renew_due_ms: Option<i64>) -> Option<i64> {
    seat_due(
      self.due_ms,
      self.due_days,
      self.days_from_ms,
      self.tz,
      assigned_ms,
      renew_due_ms,
    )
  }
}

/// P1 due_days / P2 recert: one person's due on one assignment (ROW_DUE in
/// SQL): a renewal due, else 23:59:59.999 local on the day N days after they
/// were added (or after N was set), else the assignment's date.
pub(crate) fn seat_due(
  due_ms: Option<i64>,
  due_days: Option<i32>,
  days_from_ms: i64,
  tz: i32,
  assigned_ms: i64,
  renew_due_ms: Option<i64>,
) -> Option<i64> {
  if renew_due_ms.is_some() {
    return renew_due_ms;
  }
  match due_days {
    Some(n) => {
      let day = local_day(assigned_ms.max(days_from_ms), tz) + i64::from(n);
      Some(day_start(day + 1, tz) - 1)
    },
    None => due_ms,
  }
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
  Option<i32>,
  i64,
  Option<Uuid>,
);

async fn assignments_of(pool: &PgPool, ws: Uuid, only: Option<Uuid>) -> Result<Vec<Asg>, AppError> {
  let rows: Vec<AsgRow> = sqlx::query_as(
    "SELECT id, course_id, target_kind, target_id, due_ms, tz_minutes, required, note, calendar, \
       created_by, created_ms, updated_ms, due_days, days_from_ms, path_id \
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
        due_days: r.12,
        days_from_ms: r.13,
        path: r.14,
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
  /// P2 recert: they're taking it again because their finish ran out.
  renewal: bool,
  /// P3 paths: the paths that assign it to them; direct = also assigned outside a path.
  paths: Vec<Uuid>,
  direct: bool,
}

impl Eff {
  fn new() -> Eff {
    Eff {
      due_ms: None,
      required: false,
      assigned_ms: i64::MAX,
      note: String::new(),
      note_ms: i64::MIN,
      renewal: false,
      paths: Vec::new(),
      direct: false,
    }
  }

  /// P2 recert / P3 paths: where one of their rows comes from.
  fn mark(&mut self, path: Option<Uuid>, renewal: bool) {
    match path {
      Some(p) => {
        if !self.paths.contains(&p) {
          self.paths.push(p);
        }
      },
      None => self.direct = true,
    }
    self.renewal = self.renewal || renewal;
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
      // P2 recert / P3 paths
      "renewal": self.renewal,
      "paths": self.paths.iter().map(|p| p.to_string()).collect::<Vec<String>>(),
      "direct": self.direct,
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

pub(crate) struct Enr {
  pub(crate) status: String,
  pub(crate) percent: i32,
  /// P6 notices: pub(crate) for the path report.
  pub(crate) completed_ms: Option<i64>,
}

/// Everything the assignment lists and reports need, loaded once per request.
/// Only current non-guest members are counted.
pub(crate) struct Board {
  pub(crate) now: i64,
  pub(crate) people: People,
  pub(crate) courses: BTreeMap<Uuid, Course>,
  pub(crate) asgs: Vec<Asg>,
  pub(crate) rows: HashMap<Uuid, Vec<i64>>,
  /// P1 due_days: (assignment, uid) → (assigned_ms, their due on it).
  pub(crate) seats: HashMap<(Uuid, i64), (i64, Option<i64>)>,
  eff: HashMap<(Uuid, i64), Eff>,
  pub(crate) enr: HashMap<(Uuid, i64), Enr>,
  /// P2 recert: renewal months per course.
  recert: HashMap<Uuid, i64>,
  /// P3 paths: titles of the workspace's paths.
  path_titles: HashMap<Uuid, String>,
}

impl Board {
  pub(crate) async fn load(pool: &PgPool, ws: Uuid, only: Option<Uuid>) -> Result<Board, AppError> {
    let people = people_of(pool, ws).await?;
    let courses = courses_of(pool, ws, only).await?;
    let asgs = assignments_of(pool, ws, only).await?;
    let raw: Vec<(Uuid, i64, i64, Option<i64>)> = sqlx::query_as(
      "SELECT s.assignment_id, s.uid, s.assigned_ms, s.renew_due_ms \
       FROM carehub_learning_assignee s \
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
    let mut seats: HashMap<(Uuid, i64), (i64, Option<i64>)> = HashMap::new();
    let mut eff: HashMap<(Uuid, i64), Eff> = HashMap::new();
    for (asg_id, uid, assigned_ms, renew_due_ms) in raw {
      if !people.has(uid) {
        continue;
      }
      let a = match by_id.get(&asg_id).and_then(|i| asgs.get(*i)) {
        Some(a) => a,
        None => continue,
      };
      rows.entry(asg_id).or_default().push(uid);
      // P1 due_days / P2 recert: each person's own due on this assignment.
      let due = a.due_for(assigned_ms, renew_due_ms);
      seats.insert((asg_id, uid), (assigned_ms, due));
      let e = eff.entry((a.course, uid)).or_insert_with(Eff::new);
      e.add(due, a.required, &a.note, a.updated_ms, assigned_ms);
      e.mark(a.path, renew_due_ms.is_some());
    }
    // P2 recert: renewal months of these courses (cached per version).
    let live: Vec<(Uuid, i32)> = courses.iter().map(|(id, c)| (*id, c.version)).collect();
    let recert = recert_for(pool, &live).await?;
    // P3 paths: names for path rows.
    let titles: Vec<(Uuid, String)> =
      sqlx::query_as("SELECT id, title FROM carehub_learning_path WHERE workspace_id = $1")
        .bind(ws)
        .fetch_all(pool)
        .await?;
    let path_titles: HashMap<Uuid, String> = titles.into_iter().collect();
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
      seats,
      eff,
      enr,
      recert,
      path_titles,
    })
  }

  fn status(&self, course: Uuid, uid: i64) -> Option<&str> {
    self.enr.get(&(course, uid)).map(|e| e.status.as_str())
  }

  fn due(&self, course: Uuid, uid: i64) -> Option<i64> {
    self.eff.get(&(course, uid)).and_then(|e| e.due_ms)
  }

  pub(crate) fn target_name(&self, a: &Asg, groups: &[Group]) -> String {
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
      "due_days": a.due_days,
      "tz_minutes": a.tz,
      "required": a.required,
      "note": a.note,
      "calendar": a.calendar,
      "created_ms": a.created_ms,
      "created_by_name": names.get(&a.created_by).cloned().unwrap_or_default(),
      "updated_ms": a.updated_ms,
      // P3 paths
      "path_id": a.path.map(|p| p.to_string()),
      "path_title": a.path.and_then(|p| self.path_titles.get(&p).cloned()),
    });
    with_counts(v, &t, "people")
  }

  /// P6 notices: `marked` = finishes marked by hand that are still theirs.
  fn people_json(&self, a: &Asg, marked: &HashMap<i64, Value>) -> Vec<Value> {
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
        // P2 recert: when a finish runs out.
        let expires = e
          .filter(|x| finished(&x.status))
          .and_then(|x| x.completed_ms)
          .and_then(|t| self.recert.get(&a.course).map(|n| add_months(t, *n)));
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
            "expires_ms": expires,
            "expired": expires.map(|t| t <= self.now).unwrap_or(false),
            // P6 notices
            "marked": marked.get(uid).cloned().unwrap_or(Value::Null),
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
pub(crate) fn want_for(kind: &str, id: &str, people: &People, groups: &[Group]) -> Vec<i64> {
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
pub(crate) async fn sync_assignment(
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

/// A new due date: due / overdue reminders start again from scratch (P2
/// recert: not for people on a renewal due, which a date change doesn't move).
async fn rearm(conn: &mut PgConnection, asg: Uuid) -> Result<(), AppError> {
  sqlx::query(
    "UPDATE carehub_learning_assignee SET sent_due_ms = 0, sent_overdue_ms = 0 \
     WHERE assignment_id = $1 AND renew_due_ms IS NULL",
  )
  .bind(asg)
  .execute(&mut *conn)
  .await?;
  Ok(())
}

/// After a change commits: reminders and calendar entries for those courses,
/// in the background so the request returns straight away.
pub(crate) fn after_change(pool: &PgPool, mut courses: Vec<Uuid>) {
  courses.sort();
  courses.dedup();
  if courses.is_empty() {
    return;
  }
  let pool = pool.clone();
  tokio::spawn(async move {
    // P2 recert: people just assigned whose finish ran out start again now.
    if let Err(err) = renewals(&pool, Some(courses.as_slice())).await {
      warn_rarely(&format!("renewals: {}", err));
    }
    if let Err(err) = deliver(&pool, Some(courses.clone())).await {
      warn_rarely(&format!("reminders: {}", err));
    }
    if let Err(err) = sync_calendar(&pool, Some(courses)).await {
      warn_rarely(&format!("calendar entries: {}", err));
    }
    // P3 paths: path messages, reminders and calendar entries.
    if let Err(err) = carehub_learning_paths::tick_paths(&pool).await {
      warn_rarely(&format!("learning paths: {}", err));
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
    "SELECT a.target_id, COUNT(DISTINCT a.course_id)::BIGINT FROM carehub_learning_assignment a \
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

/// P1 due_days: None / null = no days; else a whole number 1-365.
fn parse_days(v: Option<&Value>) -> Result<Option<i32>, AppError> {
  match v {
    None | Some(Value::Null) => Ok(None),
    Some(x) => {
      let n = x
        .as_i64()
        .or_else(|| {
          x.as_f64()
            .filter(|f| f.is_finite() && f.fract().abs() < 1e-9 && f.abs() < 1e9)
            .map(|f| f as i64)
        })
        .ok_or_else(|| bad(DAYS_MSG))?;
      if !(1..=MAX_DUE_DAYS).contains(&n) {
        return Err(bad(DAYS_MSG));
      }
      i32::try_from(n).map(Some).map_err(|_| bad(DAYS_MSG))
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

/// P1 due_days / P3 paths: the settings of POST /assignments and POST
/// /paths/{id}/assign.
pub(crate) struct Plan {
  pub(crate) due_ms: Option<i64>,
  pub(crate) due_days: Option<i32>,
  pub(crate) tz: i32,
  pub(crate) required: bool,
  pub(crate) note: String,
  pub(crate) calendar: bool,
  pub(crate) notify: bool,
}

/// Checks in the order POST /assignments always used (targets, then dates).
pub(crate) fn plan_of(v: &Value, now: i64) -> Result<Plan, AppError> {
  let raw = list(v, "targets");
  if raw.is_empty() {
    return Err(bad(NO_TARGETS_MSG));
  }
  if raw.len() > MAX_TARGETS {
    return Err(bad(TOO_MANY_TARGETS_MSG));
  }
  let due_ms = parse_due(v.get("due_ms"))?;
  // P1 due_days
  let due_days = parse_days(v.get("due_days"))?;
  if due_ms.is_some() && due_days.is_some() {
    return Err(bad(BOTH_DUE_MSG));
  }
  if due_ms.map(|d| d <= now).unwrap_or(false) {
    return Err(bad(PAST_DUE_MSG));
  }
  Ok(Plan {
    due_ms,
    due_days,
    tz: parse_tz(v).unwrap_or(0),
    required: flag(v, "required", false),
    note: parse_note(v)?.unwrap_or_default(),
    calendar: flag(v, "calendar", true),
    notify: flag(v, "notify", true),
  })
}

pub(crate) type Picked = Vec<(String, String, Vec<i64>)>;

/// The request's targets in stored form with who they cover now (unknown ones
/// dropped, repeats once).
pub(crate) fn pick_targets(
  v: &Value,
  people: &People,
  groups: &[Group],
) -> Result<Picked, AppError> {
  let mut seen: HashSet<(String, String)> = HashSet::new();
  let mut targets: Picked = Vec::new();
  for t in list(v, "targets") {
    let id = match t.get("id") {
      Some(Value::String(s)) => s.trim().to_string(),
      Some(Value::Number(n)) => n.to_string(),
      _ => String::new(),
    };
    let (kind, id) = match normalise_target(txt(t, "kind").trim(), &id, people, groups) {
      Some(x) => x,
      None => continue,
    };
    if !seen.insert((kind.clone(), id.clone())) {
      continue;
    }
    let want = want_for(&kind, &id, people, groups);
    targets.push((kind, id, want));
  }
  if targets.is_empty() {
    return Err(bad(UNKNOWN_TARGETS_MSG));
  }
  Ok(targets)
}

/// P1 due_days / P3 paths: creates or updates the assignment of one course to
/// one target, the direct one (path None: path_id IS NULL) or a path's.
/// Returns (id, newly created). A new due date or number of days re-arms
/// reminders; switching to days or a new N counts from now for people on it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn put_assignment(
  conn: &mut PgConnection,
  ws: Uuid,
  course: Uuid,
  path: Option<Uuid>,
  kind: &str,
  target: &str,
  plan: &Plan,
  by: i64,
  now: i64,
) -> Result<(Uuid, bool), AppError> {
  let existing: Option<(Uuid, Option<i64>, Option<i32>, i64)> = sqlx::query_as(
    "SELECT id, due_ms, due_days, days_from_ms FROM carehub_learning_assignment \
     WHERE course_id = $1 AND target_kind = $2 AND target_id = $3 \
       AND path_id IS NOT DISTINCT FROM $4 FOR UPDATE",
  )
  .bind(course)
  .bind(kind)
  .bind(target)
  .bind(path)
  .fetch_optional(&mut *conn)
  .await?;
  match existing {
    Some((asg, old_due, old_days, old_from)) => {
      let days_from = if plan.due_days.is_some() && plan.due_days != old_days {
        now
      } else {
        old_from
      };
      sqlx::query(
        "UPDATE carehub_learning_assignment SET due_ms = $2, tz_minutes = $3, required = $4, \
           note = $5, calendar = $6, notify = $7, updated_ms = $8, due_days = $9, \
           days_from_ms = $10 WHERE id = $1",
      )
      .bind(asg)
      .bind(plan.due_ms)
      .bind(plan.tz)
      .bind(plan.required)
      .bind(&plan.note)
      .bind(plan.calendar)
      .bind(plan.notify)
      .bind(now)
      .bind(plan.due_days)
      .bind(days_from)
      .execute(&mut *conn)
      .await?;
      if old_due != plan.due_ms || old_days != plan.due_days {
        rearm(conn, asg).await?;
      }
      Ok((asg, false))
    },
    None => {
      let asg = Uuid::new_v4();
      sqlx::query(
        "INSERT INTO carehub_learning_assignment (id, workspace_id, course_id, target_kind, \
           target_id, due_ms, tz_minutes, required, note, calendar, notify, created_by, \
           created_ms, updated_ms, due_days, path_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13, $14, $15)",
      )
      .bind(asg)
      .bind(ws)
      .bind(course)
      .bind(kind)
      .bind(target)
      .bind(plan.due_ms)
      .bind(plan.tz)
      .bind(plan.required)
      .bind(&plan.note)
      .bind(plan.calendar)
      .bind(plan.notify)
      .bind(by)
      .bind(now)
      .bind(plan.due_days)
      .bind(path)
      .execute(&mut *conn)
      .await?;
      Ok((asg, true))
    },
  }
}

/// P3 paths: PUT / DELETE on a path's assignment are refused.
async fn path_row(conn: &mut PgConnection, path: Uuid) -> AppError {
  let title: Option<String> =
    sqlx::query_scalar("SELECT title FROM carehub_learning_path WHERE id = $1")
      .bind(path)
      .fetch_optional(&mut *conn)
      .await
      .ok()
      .flatten();
  bad(&format!(
    "This course is assigned through the learning path “{}”. Change it on the Paths tab.",
    title.unwrap_or_else(|| "Untitled path".to_string())
  ))
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
  let now = now_ms();
  let plan = plan_of(&v, now)?;
  let people = people_of(pool, c.ws).await?;
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  let groups = groups_of(&mut tx, c.ws).await?;
  let targets = pick_targets(&v, &people, &groups)?;
  let mut created: i64 = 0;
  let mut updated: i64 = 0;
  let mut covered: HashSet<i64> = HashSet::new();
  for (kind, id, want) in &targets {
    // P3 paths: only the direct assignment (path_id IS NULL) is edited here.
    let (asg, fresh) =
      put_assignment(&mut tx, c.ws, course, None, kind, id, &plan, c.uid(), now).await?;
    if fresh {
      created += 1;
    } else {
      updated += 1;
    }
    sync_assignment(&mut tx, asg, course, c.ws, plan.notify, want, now).await?;
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
  // P6 notices
  let marked = marked_on(pool, c.ws, course).await?;
  Ok(json!({
    "assignment": board.assignment_json(a, &groups, &names),
    "people": board.people_json(a, &marked),
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
  #[allow(clippy::type_complexity)]
  let row: Option<(Uuid, Option<i64>, i32, bool, String, bool, Option<i32>, i64, Option<Uuid>)> =
    sqlx::query_as(
      "SELECT course_id, due_ms, tz_minutes, required, note, calendar, due_days, days_from_ms, \
         path_id FROM carehub_learning_assignment WHERE id = $1 AND workspace_id = $2 FOR UPDATE",
    )
    .bind(asg)
    .bind(c.ws)
    .fetch_optional(&mut *tx)
    .await?;
  let (course, old_due, old_tz, old_required, old_note, old_calendar, old_days, old_from, path) =
    row.ok_or_else(assignment_missing)?;
  // P3 paths: a path's assignment is changed on the Paths tab.
  if let Some(p) = path {
    return Err(path_row(&mut tx, p).await);
  }
  // P1 due_days: due_ms / due_days are replaced as a pair (a missing one = none).
  let (due, days) = if v.get("due_ms").is_some() || v.get("due_days").is_some() {
    let due = parse_due(v.get("due_ms"))?;
    let days = parse_days(v.get("due_days"))?;
    if due.is_some() && days.is_some() {
      return Err(bad(BOTH_DUE_MSG));
    }
    (due, days)
  } else {
    (old_due, old_days)
  };
  if due != old_due && due.map(|d| d <= now).unwrap_or(false) {
    return Err(bad(PAST_DUE_MSG));
  }
  let days_from = if days.is_some() && days != old_days {
    now
  } else {
    old_from
  };
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
       note = $5, calendar = $6, updated_ms = $7, due_days = $8, days_from_ms = $9 WHERE id = $1",
  )
  .bind(asg)
  .bind(due)
  .bind(tz)
  .bind(required)
  .bind(note)
  .bind(calendar)
  .bind(now)
  .bind(days)
  .bind(days_from)
  .execute(&mut *tx)
  .await?;
  if due != old_due || days != old_days {
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
  // P3 paths: a path's assignment is removed on the Paths tab.
  let path: Option<Option<Uuid>> = sqlx::query_scalar(
    "SELECT path_id FROM carehub_learning_assignment WHERE id = $1 AND workspace_id = $2 \
     FOR UPDATE",
  )
  .bind(asg)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  if let Some(Some(p)) = path {
    return Err(path_row(&mut tx, p).await);
  }
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
  #[allow(clippy::type_complexity)]
  let rows: Vec<(
    Uuid,
    Option<i64>,
    bool,
    String,
    i64,
    i64,
    Option<i32>,
    i64,
    i32,
    Option<Uuid>,
    Option<i64>,
  )> = sqlx::query_as(
    "SELECT a.course_id, a.due_ms, a.required, a.note, a.updated_ms, s.assigned_ms, \
       a.due_days, a.days_from_ms, a.tz_minutes, a.path_id, s.renew_due_ms \
     FROM carehub_learning_assignee s \
     JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
     WHERE a.workspace_id = $1 AND s.uid = $2",
  )
  .bind(c.ws)
  .bind(uid)
  .fetch_all(pool)
  .await?;
  let mut eff: HashMap<Uuid, Eff> = HashMap::new();
  for r in rows {
    // P1 due_days / P2 recert / P3 paths: (course, due_ms, required, note,
    // updated_ms, assigned_ms, due_days, days_from_ms, tz, path, renew_due_ms)
    let due = seat_due(r.1, r.6, r.7, r.8, r.5, r.10);
    let e = eff.entry(r.0).or_insert_with(Eff::new);
    e.add(due, r.2, &r.3, r.4, r.5);
    e.mark(r.9, r.10.is_some());
  }
  if eff.is_empty() {
    return Ok(json!({ "courses": [] }));
  }
  let ids: Vec<Uuid> = eff.keys().copied().collect();
  // Same card as Phase 1 /catalog and /my (the version the caller is on).
  // P6 notices: side lessons (`branch: true`) aren't counted in `lessons`.
  let cards: Vec<CardRow> = sqlx::query_as(
    "SELECT c.id, c.status, v.published_ms, v.version, \
       COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), NULLIF(c.title, ''), 'Untitled course'), \
       COALESCE(v.content->>'summary', ''), COALESCE(v.content->>'cover', ''), \
       COALESCE(v.content->>'preset', 'standard'), \
       (SELECT COUNT(*) FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v.content->'lessons') \
         = 'array' THEN v.content->'lessons' ELSE '[]'::jsonb END) AS x \
         WHERE x->'branch' IS DISTINCT FROM 'true'::jsonb)::INT, \
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
  let mut courses: Vec<Value> = out.into_iter().map(|x| x.1).collect();
  // P2 recert
  add_card_expiry(pool, &mut courses).await;
  Ok(json!({ "courses": courses }))
}

type CertRow = (String, Uuid, String, String, Option<i32>, i64, i32);

/// P2 recert: `months` = the course's renewal period (the certificate runs out).
fn cert_json(r: &CertRow, months: Option<i64>, now: i64) -> Value {
  let expires = months.map(|n| add_months(r.5, n));
  json!({
    "code": r.0,
    "course_id": r.1.to_string(),
    "title": r.2,
    "person_name": r.3,
    "score": r.4,
    "issued_ms": r.5,
    "version": r.6,
    "expires_ms": expires,
    "expired": expires.map(|t| t <= now).unwrap_or(false),
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
  // P2 recert
  let mut ids: Vec<Uuid> = rows.iter().map(|r| r.1).collect();
  ids.sort();
  ids.dedup();
  let months = recert_by_course(pool, &ids).await?;
  let now = now_ms();
  let list: Vec<Value> = rows
    .iter()
    .map(|r| cert_json(r, months.get(&r.1).copied(), now))
    .collect();
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
  // P2 recert
  let months = recert_by_course(&state.pg_pool, &[row.1]).await?;
  Ok(json!({ "certificate": cert_json(&row, months.get(&row.1).copied(), now_ms()) }))
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
      // S2: read by text (today's tokens, or the old per-person ones).
      let hits = carehub_learning::matching_hits(q, course, *uid, chosen);
      for (i, hit) in hits.into_iter().enumerate() {
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
  let rows: Vec<Value> = if let Some(rows) = carehub_learning::insight_rows(q, attempts) {
    rows
  } else if kind == "matching" {
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

pub(crate) async fn send(pool: &PgPool, uid: i64, body: &str) {
  if let Err(err) = carehub_extras::notify_user(pool, uid, body).await {
    warn_rarely(&format!("could not send a learning message: {}", err));
  }
}

/// Marks the person's pending "assigned" rows for a course; returns how many.
/// P3 paths: direct rows only (a path has its own message).
async fn claim_assigned(pool: &PgPool, course: Uuid, uid: i64, value: i64) -> Result<u64, AppError> {
  let done = sqlx::query(
    "UPDATE carehub_learning_assignee s SET sent_assigned_ms = $3 \
     FROM carehub_learning_assignment a \
     WHERE a.id = s.assignment_id AND a.path_id IS NULL AND s.course_id = $1 AND s.uid = $2 \
       AND s.sent_assigned_ms = 0",
  )
  .bind(course)
  .bind(uid)
  .bind(value)
  .execute(pool)
  .await?;
  Ok(done.rows_affected())
}

/// Marks the person's pending due (or overdue) reminder rows for the
/// assignments with that due date; returns how many. P1 due_days: their own
/// due on each row; P3 paths: path rows too, so one course and date never get
/// two reminders.
async fn claim_due(
  pool: &PgPool,
  course: Uuid,
  uid: i64,
  due: i64,
  overdue: bool,
  value: i64,
) -> Result<u64, AppError> {
  let column = if overdue {
    "sent_overdue_ms"
  } else {
    "sent_due_ms"
  };
  let sql = format!(
    "UPDATE carehub_learning_assignee s SET {column} = $4 \
     FROM carehub_learning_assignment a \
     WHERE a.id = s.assignment_id AND s.course_id = $1 AND s.uid = $2 AND {ROW_DUE} = $3 \
       AND s.{column} = 0"
  );
  let done = sqlx::query(&sql)
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
  // P1 due_days: each row's own due; P3 paths: direct rows only (paths remind
  // about their next course themselves).
  let sql = format!(
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
           {ROW_DUE} AS due_ms, MIN({ROW_DUE}) OVER w AS eff_due, \
           MIN(s.assigned_ms) OVER w AS first_ms, BOOL_OR(a.required) OVER w AS req, \
           FIRST_VALUE(a.tz_minutes) OVER (PARTITION BY s.course_id, s.uid \
             ORDER BY {ROW_DUE} ASC NULLS LAST) AS tz \
         FROM carehub_learning_assignee s \
         JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
         JOIN carehub_course c ON c.id = s.course_id \
         JOIN af_workspace_member m ON m.workspace_id = c.workspace_id AND m.uid = s.uid \
         JOIN af_user u ON u.uid = s.uid \
         WHERE c.status = 'published' AND m.role_id::INT <> $3 AND a.path_id IS NULL \
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
     LIMIT $4"
  );
  let rows: Vec<PairRow> = sqlx::query_as(&sql)
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
  // P1 due_days: each row's own due; P3 paths: direct rows only (a path has
  // one entry per person, carehub_learning_paths.rs).
  let sql = format!(
    "SELECT DISTINCT ON (s.course_id, s.uid) s.course_id, s.uid, c.workspace_id, \
       {ROW_DUE} AS due, a.tz_minutes \
     FROM carehub_learning_assignee s \
     JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
     JOIN carehub_course c ON c.id = s.course_id \
     JOIN af_workspace_member m ON m.workspace_id = c.workspace_id AND m.uid = s.uid \
     JOIN af_user u ON u.uid = s.uid \
     WHERE a.calendar AND a.path_id IS NULL AND {ROW_DUE} IS NOT NULL \
       AND c.status = 'published' \
       AND m.role_id::INT <> $1 AND COALESCE(u.email, '') NOT LIKE '%.removed-%' \
       AND ($2::UUID[] IS NULL OR s.course_id = ANY($2)) \
     ORDER BY s.course_id, s.uid, due ASC"
  );
  let want: Vec<(Uuid, i64, Uuid, i64, i32)> = sqlx::query_as(&sql)
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
// P2 recert: finishes that run out (heads-up, progress reset, renewal due)
// ---------------------------------------------------------------------------

/// settings.recert_months: missing / null = never, else 1-60 whole months.
fn recert_value(v: Option<&Value>) -> Result<Option<i64>, &'static str> {
  match v {
    None | Some(Value::Null) => Ok(None),
    Some(x) => x
      .as_i64()
      .or_else(|| {
        x.as_f64()
          .filter(|f| f.is_finite() && f.fract().abs() < 1e-9 && f.abs() < 1e9)
          .map(|f| f as i64)
      })
      .filter(|n| (1..=MAX_RECERT_MONTHS).contains(n))
      .map(Some)
      .ok_or(RECERT_MSG),
  }
}

/// The publish check (carehub_learning.rs publish_problems).
pub(crate) fn recert_problem(settings: &Value) -> Option<&'static str> {
  recert_value(settings.get("recert_months")).err()
}

/// Renewal months of published versions, cached like CERT_FLAGS (versions
/// never change); an invalid stored value counts as never.
async fn recert_for(pool: &PgPool, live: &[(Uuid, i32)]) -> Result<HashMap<Uuid, i64>, AppError> {
  let mut out: HashMap<Uuid, i64> = HashMap::new();
  let mut ask_courses: Vec<Uuid> = Vec::new();
  let mut ask_versions: Vec<i32> = Vec::new();
  for key in live {
    let known = RECERT.lock().ok().and_then(|m| m.get(key).copied());
    match known {
      Some(Some(n)) => {
        out.insert(key.0, n);
      },
      Some(None) => {},
      None => {
        ask_courses.push(key.0);
        ask_versions.push(key.1);
      },
    }
  }
  if ask_courses.is_empty() {
    return Ok(out);
  }
  let rows: Vec<(Uuid, i32, Option<String>)> = sqlx::query_as(
    "SELECT v.course_id, v.version, (v.content->'settings'->'recert_months')::TEXT \
     FROM carehub_course_version v \
     JOIN UNNEST($1::UUID[], $2::INT[]) AS w(course_id, version) \
       ON w.course_id = v.course_id AND w.version = v.version",
  )
  .bind(ask_courses)
  .bind(ask_versions)
  .fetch_all(pool)
  .await?;
  for (course, version, raw) in rows {
    let value: Option<Value> = raw.and_then(|s| serde_json::from_str(&s).ok());
    let months = recert_value(value.as_ref()).ok().flatten();
    if let Ok(mut m) = RECERT.lock() {
      m.insert((course, version), months);
    }
    if let Some(n) = months {
      out.insert(course, n);
    }
  }
  Ok(out)
}

/// Renewal months by course (latest published version, archived included).
async fn recert_by_course(pool: &PgPool, ids: &[Uuid]) -> Result<HashMap<Uuid, i64>, AppError> {
  if ids.is_empty() {
    return Ok(HashMap::new());
  }
  let live: Vec<(Uuid, i32)> = sqlx::query_as(
    "SELECT id, published_version FROM carehub_course \
     WHERE id = ANY($1) AND published_version IS NOT NULL",
  )
  .bind(ids.to_vec())
  .fetch_all(pool)
  .await?;
  recert_for(pool, &live).await
}

/// Day number of a (year, month 1-12, day) date (civil_from_days reversed).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
  let y = if m <= 2 { y - 1 } else { y };
  let era = y.div_euclid(400);
  let yoe = y.rem_euclid(400);
  let mp = if m > 2 { m - 3 } else { m + 9 };
  let doy = (153 * mp + 2) / 5 + d - 1;
  let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  era * 146_097 + doe - 719_468
}

/// `ms` plus whole calendar months: UTC, same time of day, the day clamped to
/// the month's end (31 Jan + 1 month = 28 / 29 Feb).
pub(crate) fn add_months(ms: i64, months: i64) -> i64 {
  let (y, m, d) = civil_from_days(ms.div_euclid(DAY_MS));
  let index = y * 12 + (m - 1) + months;
  let (y2, m2) = (index.div_euclid(12), index.rem_euclid(12) + 1);
  let (y3, m3) = if m2 == 12 { (y2 + 1, 1) } else { (y2, m2 + 1) };
  let last = days_from_civil(y3, m3, 1) - days_from_civil(y2, m2, 1);
  days_from_civil(y2, m2, d.min(last)) * DAY_MS + ms.rem_euclid(DAY_MS)
}

fn heads_up_days(months: i64) -> i64 {
  if months == 1 {
    HEADS_UP_DAYS_MONTHLY
  } else {
    HEADS_UP_DAYS
  }
}

/// 09:00 local, HEADS_UP_DAYS before the local day a finish runs out.
fn heads_up_at(expires: i64, months: i64, tz: i32) -> i64 {
  day_start(local_day(expires, tz) - heads_up_days(months), tz) + REMIND_HOUR * HOUR_MS
}

/// 23:59:59.999 local, RENEW_DAYS after the day of the renewal.
fn renewal_due(now: i64, tz: i32) -> i64 {
  day_start(local_day(now, tz) + RENEW_DAYS + 1, tz) - 1
}

/// Finishes from before this may need a heads-up or a renewal now: N months
/// are at least 28·N days and a heads-up comes less than heads_up_days + 1
/// days before the end (one day spare).
fn recert_cutoff(now: i64, months: i64) -> i64 {
  now - (months * 28 - heads_up_days(months) - 2) * DAY_MS
}

/// expires_ms / expired on one progress object (status + completed_ms).
fn put_row_expiry(row: &mut Value, months: Option<i64>, now: i64) {
  let expires = if finished(txt(row, "status")) {
    row
      .get("completed_ms")
      .and_then(|t| t.as_i64())
      .and_then(|t| months.map(|n| add_months(t, n)))
  } else {
    None
  };
  if let Some(o) = row.as_object_mut() {
    o.insert("expires_ms".to_string(), json!(expires));
    o.insert(
      "expired".to_string(),
      json!(expires.map(|t| t <= now).unwrap_or(false)),
    );
  }
}

/// Course cards (Phase 1 /catalog and /my, /assigned): expires_ms / expired on
/// `my`. Never fails the request (on a database error the fields are left out).
pub(crate) async fn add_card_expiry(pool: &PgPool, cards: &mut [Value]) {
  let mut ids: Vec<Uuid> = cards
    .iter()
    .filter_map(|c| Uuid::parse_str(txt(c, "id")).ok())
    .collect();
  ids.sort();
  ids.dedup();
  let months = match recert_by_course(pool, &ids).await {
    Ok(m) => m,
    Err(err) => {
      warn_rarely(&format!("renewal dates: {}", err));
      return;
    },
  };
  let now = now_ms();
  for card in cards.iter_mut() {
    let n = Uuid::parse_str(txt(card, "id"))
      .ok()
      .and_then(|c| months.get(&c).copied());
    if let Some(my) = card.get_mut("my") {
      put_row_expiry(my, n, now);
    }
  }
}

fn row_course(row: &Value, course: Option<Uuid>) -> Option<Uuid> {
  course.or_else(|| Uuid::parse_str(txt(row, "course_id")).ok())
}

/// Progress rows of one course (`course`) or with a course_id each: report
/// rows and the Player's enrollment. Never fails the request.
/// P6 notices: `marked` too (add_row_marked).
pub(crate) async fn add_row_expiry(pool: &PgPool, rows: &mut [Value], course: Option<Uuid>) {
  // P6 notices
  add_row_marked(pool, rows, course).await;
  let mut ids: Vec<Uuid> = rows.iter().filter_map(|r| row_course(r, course)).collect();
  ids.sort();
  ids.dedup();
  let months = match recert_by_course(pool, &ids).await {
    Ok(m) => m,
    Err(err) => {
      warn_rarely(&format!("renewal dates: {}", err));
      return;
    },
  };
  let now = now_ms();
  for row in rows.iter_mut() {
    let n = row_course(row, course).and_then(|c| months.get(&c).copied());
    put_row_expiry(row, n, now);
  }
}

/// want_for for one person.
fn covers(kind: &str, id: &str, uid: i64, people: &People, groups: &[Group]) -> bool {
  if !people.has(uid) {
    return false;
  }
  match kind {
    "person" => id.parse::<i64>().map(|x| x == uid).unwrap_or(false),
    "role" => people.get(uid).map(|p| p.role_id == id).unwrap_or(false),
    "everyone" => true,
    "group" => Uuid::parse_str(id)
      .ok()
      .and_then(|g| groups.iter().find(|x| x.id == g))
      .map(|g| g.members.contains(&uid))
      .unwrap_or(false),
    _ => false,
  }
}

/// Someone's rows on one course: `keep` = their target covers them now (they
/// are assigned), `stale` = kept only because they had finished.
struct Cover {
  keep: Vec<Uuid>,
  stale: Vec<Uuid>,
  /// Earliest assigned_ms of a covering row.
  first_ms: i64,
  /// The time zone of their reminders (the covering row with the earliest due).
  tz: i32,
  rank: Option<(bool, i64)>,
}

impl Cover {
  fn new() -> Cover {
    Cover {
      keep: Vec::new(),
      stale: Vec::new(),
      first_ms: i64::MAX,
      tz: 0,
      rank: None,
    }
  }

  fn add(&mut self, asg: Uuid, covering: bool, assigned_ms: i64, tz: i32, due: Option<i64>) {
    if covering {
      self.keep.push(asg);
      self.first_ms = self.first_ms.min(assigned_ms);
    } else {
      self.stale.push(asg);
    }
    let rank = (!covering, due.unwrap_or(i64::MAX));
    if self.rank.map(|r| rank < r).unwrap_or(true) {
      self.rank = Some(rank);
      self.tz = tz;
    }
  }
}

/// The rows of some (course, person) pairs of one workspace.
async fn covers_of(
  pool: &PgPool,
  pairs: &[(Uuid, i64)],
  people: &People,
  groups: &[Group],
) -> Result<HashMap<(Uuid, i64), Cover>, AppError> {
  let courses: Vec<Uuid> = pairs.iter().map(|p| p.0).collect();
  let uids: Vec<i64> = pairs.iter().map(|p| p.1).collect();
  let sql = format!(
    "SELECT s.course_id, s.uid, a.id, a.target_kind, a.target_id, s.assigned_ms, \
       a.tz_minutes, {ROW_DUE} \
     FROM UNNEST($1::UUID[], $2::BIGINT[]) AS w(course_id, uid) \
     JOIN carehub_learning_assignee s ON s.course_id = w.course_id AND s.uid = w.uid \
     JOIN carehub_learning_assignment a ON a.id = s.assignment_id"
  );
  #[allow(clippy::type_complexity)]
  let rows: Vec<(Uuid, i64, Uuid, String, String, i64, i32, Option<i64>)> = sqlx::query_as(&sql)
    .bind(courses)
    .bind(uids)
    .fetch_all(pool)
    .await?;
  let mut out: HashMap<(Uuid, i64), Cover> = HashMap::new();
  for (course, uid, asg, kind, target, assigned_ms, tz, due) in rows {
    let covering = covers(&kind, &target, uid, people, groups);
    out
      .entry((course, uid))
      .or_insert_with(Cover::new)
      .add(asg, covering, assigned_ms, tz, due);
  }
  Ok(out)
}

/// One finish that can run out.
struct Finish {
  course: Uuid,
  uid: i64,
  ws: Uuid,
  completed_ms: i64,
  months: i64,
  expires: i64,
}

/// How a renewal went.
enum Renewal {
  /// Nothing to do any more (reset, finished again, or renewed already).
  Gone,
  /// Progress reset only: assigned after it ran out (their own due applies).
  Reset,
  /// Progress reset with a renewal due.
  Due(i64),
}

/// Remembers when to look at a finish again.
async fn put_expiry(pool: &PgPool, f: &Finish, wake: i64) -> Result<(), AppError> {
  sqlx::query(
    "INSERT INTO carehub_learning_renewal \
       (course_id, uid, finished_ms, workspace_id, months, expires_ms, wake_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7) \
     ON CONFLICT (course_id, uid, finished_ms) DO UPDATE SET months = EXCLUDED.months, \
       expires_ms = EXCLUDED.expires_ms, wake_ms = EXCLUDED.wake_ms \
     WHERE carehub_learning_renewal.renewed_ms = 0",
  )
  .bind(f.course)
  .bind(f.uid)
  .bind(f.completed_ms)
  .bind(f.ws)
  .bind(f.months)
  .bind(f.expires)
  .bind(wake)
  .execute(pool)
  .await?;
  Ok(())
}

/// Locks their enrollment; while it is still this finish, the data of the
/// "reset" event that ends it.
async fn still_finished(
  conn: &mut PgConnection,
  f: &Finish,
  by: &str,
) -> Result<Option<Value>, AppError> {
  let enr: Option<(String, Option<i64>, Option<i32>, i32)> = sqlx::query_as(
    "SELECT status, completed_ms, score, version FROM carehub_enrollment \
     WHERE course_id = $1 AND uid = $2 FOR UPDATE",
  )
  .bind(f.course)
  .bind(f.uid)
  .fetch_optional(&mut *conn)
  .await?;
  Ok(match enr {
    Some((status, Some(t), score, version)) if t == f.completed_ms && finished(&status) => {
      Some(json!({
        "by": by,
        "completed_ms": t,
        "status": status,
        "score": score,
        "version": version,
      }))
    },
    _ => None,
  })
}

/// Their rows that no longer cover them (a sync would remove them now that
/// they are unfinished).
async fn drop_rows(conn: &mut PgConnection, f: &Finish, cover: &Cover) -> Result<(), AppError> {
  if cover.stale.is_empty() {
    return Ok(());
  }
  sqlx::query(
    "DELETE FROM carehub_learning_assignee \
     WHERE course_id = $1 AND uid = $2 AND assignment_id = ANY($3)",
  )
  .bind(f.course)
  .bind(f.uid)
  .bind(cover.stale.clone())
  .execute(&mut *conn)
  .await?;
  Ok(())
}

/// Renews the finish of an assigned person: the certificate for it first, then
/// in one transaction the claim, the progress reset (the existing reset path),
/// their other rows removed and, when they were assigned before it ran out, the
/// renewal due on the rest (reminders re-armed, a pending "New course" message
/// marked done); the message after the commit.
async fn renew(
  pool: &PgPool,
  f: &Finish,
  cover: &Cover,
  title: &str,
  now: i64,
) -> Result<Renewal, AppError> {
  issue_certificates(pool, Some(f.ws), Some(f.uid), Some(f.course)).await?;
  let full = cover.first_ms < f.expires;
  let due = renewal_due(now, cover.tz);
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, f.ws).await?;
  let data = match still_finished(&mut tx, f, "renewal").await? {
    Some(d) => d,
    None => return Ok(Renewal::Gone),
  };
  let claimed = sqlx::query(
    "INSERT INTO carehub_learning_renewal (course_id, uid, finished_ms, workspace_id, months, \
       expires_ms, wake_ms, renewed_ms, due_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8) \
     ON CONFLICT (course_id, uid, finished_ms) DO UPDATE SET months = EXCLUDED.months, \
       expires_ms = EXCLUDED.expires_ms, wake_ms = EXCLUDED.wake_ms, \
       renewed_ms = EXCLUDED.renewed_ms, due_ms = EXCLUDED.due_ms \
     WHERE carehub_learning_renewal.renewed_ms = 0",
  )
  .bind(f.course)
  .bind(f.uid)
  .bind(f.completed_ms)
  .bind(f.ws)
  .bind(f.months)
  .bind(f.expires)
  .bind(now)
  .bind(full.then_some(due))
  .execute(&mut *tx)
  .await?;
  if claimed.rows_affected() == 0 {
    return Ok(Renewal::Gone);
  }
  carehub_learning::reset_progress(&mut tx, f.ws, f.course, f.uid, data).await?;
  drop_rows(&mut tx, f, cover).await?;
  if full {
    sqlx::query(
      "UPDATE carehub_learning_assignee SET renew_due_ms = $3, sent_due_ms = 0, \
         sent_overdue_ms = 0, \
         sent_assigned_ms = CASE WHEN sent_assigned_ms = 0 THEN -1 ELSE sent_assigned_ms END \
       WHERE course_id = $1 AND uid = $2",
    )
    .bind(f.course)
    .bind(f.uid)
    .bind(due)
    .execute(&mut *tx)
    .await?;
  }
  tx.commit().await?;
  if !full {
    return Ok(Renewal::Reset);
  }
  let body = format!(
    "🔁 Time to renew “{}”. Please take it again by {}.\n{}",
    title,
    day_label(local_day(due, cover.tz)),
    course_link(f.course)
  );
  send(pool, f.uid, &body).await;
  Ok(Renewal::Due(due))
}

/// The heads-up for one finish (claimed first, so it goes out once).
async fn heads_up(
  pool: &PgPool,
  f: &Finish,
  tz: i32,
  title: &str,
  now: i64,
) -> Result<(), AppError> {
  let cert: Option<i32> = sqlx::query_scalar(
    "SELECT 1 FROM carehub_learning_certificate \
     WHERE course_id = $1 AND uid = $2 AND issued_ms = $3",
  )
  .bind(f.course)
  .bind(f.uid)
  .bind(f.completed_ms)
  .fetch_optional(pool)
  .await?;
  let claimed = sqlx::query(
    "INSERT INTO carehub_learning_renewal (course_id, uid, finished_ms, workspace_id, months, \
       expires_ms, wake_ms, heads_up_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $6, $7) \
     ON CONFLICT (course_id, uid, finished_ms) DO UPDATE SET months = EXCLUDED.months, \
       expires_ms = EXCLUDED.expires_ms, wake_ms = EXCLUDED.wake_ms, \
       heads_up_ms = EXCLUDED.heads_up_ms \
     WHERE carehub_learning_renewal.heads_up_ms = 0 AND carehub_learning_renewal.renewed_ms = 0",
  )
  .bind(f.course)
  .bind(f.uid)
  .bind(f.completed_ms)
  .bind(f.ws)
  .bind(f.months)
  .bind(f.expires)
  .bind(now)
  .execute(pool)
  .await?;
  if claimed.rows_affected() == 0 {
    return Ok(());
  }
  let what = if cert.is_some() {
    "certificate"
  } else {
    "training"
  };
  let body = format!(
    "🔁 Your “{}” {} runs out on {}. You'll be asked to take the course again.\n{}",
    title,
    what,
    day_label(local_day(f.expires, tz)),
    course_link(f.course)
  );
  send(pool, f.uid, &body).await;
  Ok(())
}

/// What one finish needs now: run out → renewal (assigned) or a look again in
/// a while; heads-up time not reached (or heads-up sent) → wait for it / for
/// the end; else the heads-up (assigned) or a look again in a while.
async fn look_at(
  pool: &PgPool,
  f: &Finish,
  heads_up_ms: i64,
  cover: Option<&Cover>,
  titles: &HashMap<Uuid, String>,
  now: i64,
) -> Result<(), AppError> {
  let assigned = cover.filter(|c| !c.keep.is_empty());
  let tz = cover.map(|c| c.tz).unwrap_or(0);
  let title = titles
    .get(&f.course)
    .map(|t| t.as_str())
    .unwrap_or("Untitled course");
  if f.expires <= now {
    return match assigned {
      Some(c) => {
        renew(pool, f, c, title, now).await?;
        Ok(())
      },
      None => put_expiry(pool, f, now + RECHECK_MS).await,
    };
  }
  let at = heads_up_at(f.expires, f.months, tz);
  if heads_up_ms != 0 || now < at {
    let wake = if heads_up_ms == 0 { at } else { f.expires };
    return put_expiry(pool, f, wake).await;
  }
  if assigned.is_none() {
    return put_expiry(pool, f, f.expires.min(now + RECHECK_MS)).await;
  }
  heads_up(pool, f, tz, title, now).await
}

/// Heads-ups and renewals that are due, at most RENEW_BATCH per run (the
/// longest waiting first). `only` (after an assignment change): those courses,
/// every finish looked at again (someone may have just been assigned).
pub(crate) async fn renewals(pool: &PgPool, only: Option<&[Uuid]>) -> Result<(), AppError> {
  ready(pool).await?;
  let now = now_ms();
  let live: Vec<(Uuid, i32)> = sqlx::query_as(
    "SELECT id, published_version FROM carehub_course \
     WHERE status = 'published' AND published_version IS NOT NULL \
       AND ($1::UUID[] IS NULL OR id = ANY($1))",
  )
  .bind(only.map(|x| x.to_vec()))
  .fetch_all(pool)
  .await?;
  let months = recert_for(pool, &live).await?;
  if months.is_empty() {
    return Ok(());
  }
  let mut courses: Vec<Uuid> = Vec::new();
  let mut lengths: Vec<i64> = Vec::new();
  let mut cutoffs: Vec<i64> = Vec::new();
  for (course, n) in &months {
    courses.push(*course);
    lengths.push(*n);
    cutoffs.push(recert_cutoff(now, *n));
  }
  let rows: Vec<(Uuid, i64, Uuid, i64, i64, i64)> = sqlx::query_as(
    "SELECT e.course_id, e.uid, c.workspace_id, e.completed_ms, w.months, \
       COALESCE(r.heads_up_ms, 0) \
     FROM UNNEST($1::UUID[], $2::BIGINT[], $3::BIGINT[]) AS w(course_id, months, cutoff) \
     JOIN carehub_course c ON c.id = w.course_id \
     JOIN carehub_enrollment e ON e.course_id = w.course_id \
     JOIN af_workspace_member m ON m.workspace_id = c.workspace_id AND m.uid = e.uid \
     JOIN af_user u ON u.uid = e.uid \
     LEFT JOIN carehub_learning_renewal r ON r.course_id = e.course_id AND r.uid = e.uid \
       AND r.finished_ms = e.completed_ms \
     WHERE c.status = 'published' AND e.status IN ('completed', 'passed') \
       AND e.completed_ms IS NOT NULL AND e.completed_ms < w.cutoff \
       AND m.role_id::INT <> $4 AND COALESCE(u.email, '') NOT LIKE '%.removed-%' \
       AND COALESCE(r.renewed_ms, 0) = 0 \
       AND ($5 OR r.course_id IS NULL OR r.months <> w.months OR r.wake_ms <= $6) \
       AND EXISTS (SELECT 1 FROM carehub_learning_assignee s \
         WHERE s.course_id = e.course_id AND s.uid = e.uid) \
     ORDER BY COALESCE(r.wake_ms, 0) \
     LIMIT $7",
  )
  .bind(courses)
  .bind(lengths)
  .bind(cutoffs)
  .bind(guest_role())
  .bind(only.is_some())
  .bind(now)
  .bind(RENEW_BATCH)
  .fetch_all(pool)
  .await?;
  if rows.is_empty() {
    return Ok(());
  }
  // Per workspace: people and groups are loaded once.
  let mut by_ws: BTreeMap<Uuid, Vec<(Finish, i64)>> = BTreeMap::new();
  for (course, uid, ws, completed_ms, n, heads_up_ms) in rows {
    let f = Finish {
      course,
      uid,
      ws,
      completed_ms,
      months: n,
      expires: add_months(completed_ms, n),
    };
    by_ws.entry(ws).or_default().push((f, heads_up_ms));
  }
  let mut ids: Vec<Uuid> = by_ws.values().flatten().map(|x| x.0.course).collect();
  ids.sort();
  ids.dedup();
  let titles = titles_of(pool, &ids).await?;
  for (ws, todo) in by_ws {
    let people = people_of(pool, ws).await?;
    let mut conn = pool.acquire().await?;
    let groups = groups_of(&mut conn, ws).await?;
    drop(conn);
    let pairs: Vec<(Uuid, i64)> = todo.iter().map(|x| (x.0.course, x.0.uid)).collect();
    let covers = covers_of(pool, &pairs, &people, &groups).await?;
    for (f, heads_up_ms) in &todo {
      let cover = covers.get(&(f.course, f.uid));
      if let Err(err) = look_at(pool, f, *heads_up_ms, cover, &titles, now).await {
        warn_rarely(&format!("renewal: {}", err));
      }
    }
  }
  Ok(())
}

/// A renewal due ends once they finish again (their rows go back to their own
/// due).
async fn end_renewal_dues(pool: &PgPool) -> Result<(), AppError> {
  sqlx::query(
    "UPDATE carehub_learning_assignee s SET renew_due_ms = NULL \
     WHERE s.renew_due_ms IS NOT NULL AND EXISTS (SELECT 1 FROM carehub_enrollment e \
       WHERE e.course_id = s.course_id AND e.uid = s.uid AND e.status IN ('completed', 'passed'))",
  )
  .execute(pool)
  .await?;
  Ok(())
}

/// POST /courses/{id}/retake: once my latest finish has run out. Assigned →
/// the renewal now (renewal due + message); not assigned → a plain reset.
async fn retake(state: &AppState, user: &UserUuid, ws: &str, id: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let pool = &state.pg_pool;
  let uid = c.uid();
  let status: Option<String> =
    sqlx::query_scalar("SELECT status FROM carehub_course WHERE id = $1 AND workspace_id = $2")
      .bind(course)
      .bind(c.ws)
      .fetch_optional(pool)
      .await?;
  if status.ok_or_else(missing)? != "published" {
    return Err(bad(RETAKE_MSG));
  }
  let months = recert_by_course(pool, &[course])
    .await?
    .get(&course)
    .copied()
    .ok_or_else(|| bad(RETAKE_MSG))?;
  let enr: Option<(String, Option<i64>)> = sqlx::query_as(
    "SELECT status, completed_ms FROM carehub_enrollment WHERE course_id = $1 AND uid = $2",
  )
  .bind(course)
  .bind(uid)
  .fetch_optional(pool)
  .await?;
  let completed_ms = match enr {
    Some((s, Some(t))) if finished(&s) => t,
    _ => return Err(bad(RETAKE_MSG)),
  };
  let now = now_ms();
  let f = Finish {
    course,
    uid,
    ws: c.ws,
    completed_ms,
    months,
    expires: add_months(completed_ms, months),
  };
  if f.expires > now {
    return Err(bad(RETAKE_MSG));
  }
  let people = people_of(pool, c.ws).await?;
  let mut conn = pool.acquire().await?;
  let groups = groups_of(&mut conn, c.ws).await?;
  drop(conn);
  let covers = covers_of(pool, &[(course, uid)], &people, &groups).await?;
  let none = Cover::new();
  let cover = covers.get(&(course, uid)).unwrap_or(&none);
  let due = if cover.keep.is_empty() {
    let mut tx = pool.begin().await?;
    lock_ws(&mut tx, c.ws).await?;
    if let Some(data) = still_finished(&mut tx, &f, "self").await? {
      carehub_learning::reset_progress(&mut tx, c.ws, course, uid, data).await?;
      drop_rows(&mut tx, &f, cover).await?;
      tx.commit().await?;
    }
    None
  } else {
    let titles = titles_of(pool, &[course]).await?;
    let title = titles
      .get(&course)
      .map(|t| t.as_str())
      .unwrap_or("Untitled course");
    match renew(pool, &f, cover, title, now).await? {
      Renewal::Due(d) => Some(d),
      Renewal::Reset | Renewal::Gone => None,
    }
  };
  after_change(pool, vec![course]);
  Ok(json!({ "ok": true, "renewed": due.is_some(), "due_ms": due }))
}

// ---------------------------------------------------------------------------
// P6 notices: telling people about a new version, "check this course"
// reminders, marking someone complete by hand
// ---------------------------------------------------------------------------

/// P6 notices: the publish permission (same words as carehub_learning.rs).
fn need_build(c: &Ctx) -> Result<(), AppError> {
  if c.has("learning_build") {
    Ok(())
  } else {
    Err(bad(BUILD_MSG))
  }
}

/// P6 notices: a count for an INT column.
fn count_i32(n: usize) -> i32 {
  i32::try_from(n).unwrap_or(i32::MAX)
}

/// P6 notices: one learning event (the row carehub_learning.rs logs); its id.
async fn add_event(
  conn: &mut PgConnection,
  ws: Uuid,
  course: Uuid,
  uid: i64,
  kind: &str,
  data: &Value,
  ts: i64,
) -> Result<i64, AppError> {
  let id: i64 = sqlx::query_scalar(
    "INSERT INTO carehub_learning_event \
       (workspace_id, course_id, uid, kind, lesson_id, block_id, data, ts_ms) \
     VALUES ($1, $2, $3, $4, '', '', $5::jsonb, $6) RETURNING id",
  )
  .bind(ws)
  .bind(course)
  .bind(uid)
  .bind(kind)
  .bind(data.to_string())
  .bind(ts)
  .fetch_one(&mut *conn)
  .await?;
  Ok(id)
}

/// P6 notices: messages sent after the reply (best effort, never retried).
fn send_later(pool: &PgPool, messages: Vec<(i64, String)>) {
  if messages.is_empty() {
    return;
  }
  let pool = pool.clone();
  tokio::spawn(async move {
    for (uid, body) in messages {
      send(&pool, uid, &body).await;
    }
  });
}

/// P6 notices: current non-guest members other than the caller who finished a
/// course (on a version below `below`, if given), and their rows on it.
struct Finishers {
  course: Uuid,
  uids: Vec<i64>,
  covers: HashMap<(Uuid, i64), Cover>,
}

impl Finishers {
  /// An assignment covers them now (they would get a due date).
  fn assigned(&self, uid: i64) -> bool {
    self
      .covers
      .get(&(self.course, uid))
      .map(|c| !c.keep.is_empty())
      .unwrap_or(false)
  }

  fn assigned_count(&self) -> usize {
    self.uids.iter().filter(|u| self.assigned(**u)).count()
  }
}

async fn finishers(
  pool: &PgPool,
  ws: Uuid,
  course: Uuid,
  caller: i64,
  below: Option<i32>,
) -> Result<Finishers, AppError> {
  let found: Vec<i64> = sqlx::query_scalar(
    "SELECT uid FROM carehub_enrollment WHERE course_id = $1 \
       AND status IN ('completed', 'passed') AND ($2::INT IS NULL OR version < $2) ORDER BY uid",
  )
  .bind(course)
  .bind(below)
  .fetch_all(pool)
  .await?;
  let people = people_of(pool, ws).await?;
  let uids: Vec<i64> = found
    .into_iter()
    .filter(|u| *u != caller && people.has(*u))
    .collect();
  let mut covers = HashMap::new();
  if !uids.is_empty() {
    let mut conn = pool.acquire().await?;
    let groups = groups_of(&mut conn, ws).await?;
    drop(conn);
    let pairs: Vec<(Uuid, i64)> = uids.iter().map(|u| (course, *u)).collect();
    covers = covers_of(pool, &pairs, &people, &groups).await?;
  }
  Ok(Finishers {
    course,
    uids,
    covers,
  })
}

/// P6 notices: GET /studio/{id}/announce (build): the live and the last
/// announced version, who finished (not the caller) and how many of them are
/// assigned. Never published → nulls and zeros.
async fn announce_info(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_build(&c)?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let pool = &state.pg_pool;
  let head: Option<(Option<i32>, Option<i32>)> = sqlx::query_as(
    "SELECT c.published_version, a.version FROM carehub_course c \
     LEFT JOIN carehub_learning_announce a ON a.course_id = c.id \
     WHERE c.id = $1 AND c.workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(pool)
  .await?;
  let (live, announced) = head.ok_or_else(missing)?;
  let (finished_n, assigned_n) = match live {
    Some(_) => {
      let f = finishers(pool, c.ws, course, c.uid(), None).await?;
      (f.uids.len(), f.assigned_count())
    },
    None => (0, 0),
  };
  Ok(json!({
    "published_version": live,
    "announced_version": announced,
    "finished": finished_n,
    "assigned": assigned_n,
  }))
}

/// P6 notices: the row that claims a version for an announcement.
struct Announce<'a> {
  course: Uuid,
  ws: Uuid,
  version: i32,
  action: &'a str,
  due_days: Option<i32>,
  by: i64,
  now: i64,
}

/// false = this version (or a later one) was announced already.
async fn claim_announce(
  conn: &mut PgConnection,
  a: &Announce<'_>,
  people: usize,
) -> Result<bool, AppError> {
  let done = sqlx::query(
    "INSERT INTO carehub_learning_announce \
       (course_id, workspace_id, version, action, due_days, people, by_uid, created_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
     ON CONFLICT (course_id) DO UPDATE SET workspace_id = EXCLUDED.workspace_id, \
       version = EXCLUDED.version, action = EXCLUDED.action, due_days = EXCLUDED.due_days, \
       people = EXCLUDED.people, by_uid = EXCLUDED.by_uid, created_ms = EXCLUDED.created_ms \
     WHERE carehub_learning_announce.version < EXCLUDED.version",
  )
  .bind(a.course)
  .bind(a.ws)
  .bind(a.version)
  .bind(a.action)
  .bind(a.due_days)
  .bind(count_i32(people))
  .bind(a.by)
  .bind(a.now)
  .execute(&mut *conn)
  .await?;
  Ok(done.rows_affected() > 0)
}

/// P6 notices: POST /studio/{id}/announce (build), once per version, for the
/// people who finished an earlier version: "tell" sends what changed;
/// "retake" issues the certificates of their finishes, then in one
/// transaction resets them (assigned → a renewal-style due `due_days` from
/// today; not assigned → a fresh start on the new version). Messages go out
/// in the background after the commit.
async fn announce(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_build(&c)?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let body = body_json(body)?;
  let action = match txt(&body, "action") {
    "tell" => "tell",
    "retake" => "retake",
    _ => return Err(bad(BODY_MSG)),
  };
  let version = body
    .get("version")
    .and_then(|v| v.as_i64())
    .and_then(|v| i32::try_from(v).ok())
    .ok_or_else(|| bad(BODY_MSG))?;
  let due_days = if action == "retake" {
    Some(parse_days(body.get("due_days"))?.unwrap_or(RETAKE_DAYS))
  } else {
    None
  };
  let pool = &state.pg_pool;
  let head: Option<(String, Option<i32>, Option<i32>)> = sqlx::query_as(
    "SELECT c.status, c.published_version, a.version FROM carehub_course c \
     LEFT JOIN carehub_learning_announce a ON a.course_id = c.id \
     WHERE c.id = $1 AND c.workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(pool)
  .await?;
  let (status, live, announced) = head.ok_or_else(missing)?;
  match live {
    Some(v) if status == "published" => {
      if v != version {
        return Err(bad(VERSION_MSG));
      }
    },
    _ => return Err(bad(LIVE_MSG)),
  }
  let already = json!({
    "ok": true,
    "already": true,
    "version": version,
    "action": action,
    "people": 0,
    "assigned": 0,
  });
  if announced.map(|x| x >= version).unwrap_or(false) {
    return Ok(already);
  }
  let note: Option<String> = sqlx::query_scalar(
    "SELECT note FROM carehub_course_version WHERE course_id = $1 AND version = $2",
  )
  .bind(course)
  .bind(version)
  .fetch_optional(pool)
  .await?;
  let note = note.unwrap_or_default().trim().to_string();
  if note.is_empty() {
    return Err(bad(CHANGES_MSG));
  }
  let title = titles_of(pool, &[course])
    .await?
    .remove(&course)
    .unwrap_or_else(|| "Untitled course".to_string());
  let link = course_link(course);
  let a = Announce {
    course,
    ws: c.ws,
    version,
    action,
    due_days,
    by: c.uid(),
    now: now_ms(),
  };
  if action == "tell" {
    let f = finishers(pool, c.ws, course, c.uid(), Some(version)).await?;
    let mut conn = pool.acquire().await?;
    if !claim_announce(&mut conn, &a, f.uids.len()).await? {
      return Ok(already);
    }
    drop(conn);
    let body = format!(
      "📝 “{}” has been updated. What changed: {}\n{}",
      title, note, link
    );
    send_later(pool, f.uids.iter().map(|u| (*u, body.clone())).collect());
    return Ok(json!({
      "ok": true,
      "already": false,
      "version": version,
      "action": action,
      "people": f.uids.len(),
      "assigned": f.assigned_count(),
    }));
  }
  // Take it again: certificates for their finishes first (as renewals do).
  for _ in 0..CERT_ROUNDS {
    if issue_certificates(pool, Some(c.ws), None, Some(course)).await? == 0 {
      break;
    }
  }
  let f = finishers(pool, c.ws, course, c.uid(), Some(version)).await?;
  let days = i64::from(due_days.unwrap_or(RETAKE_DAYS));
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  if !claim_announce(&mut tx, &a, f.uids.len()).await? {
    return Ok(already);
  }
  let none = Cover::new();
  let mut messages: Vec<(i64, String)> = Vec::new();
  let mut assigned_n = 0usize;
  for uid in &f.uids {
    let uid = *uid;
    let enr: Option<(String, Option<i64>, Option<i32>, i32)> = sqlx::query_as(
      "SELECT status, completed_ms, score, version FROM carehub_enrollment \
       WHERE course_id = $1 AND uid = $2 FOR UPDATE",
    )
    .bind(course)
    .bind(uid)
    .fetch_optional(&mut *tx)
    .await?;
    // Still finished on an earlier version (else someone got there first).
    let data = match enr {
      Some((st, completed_ms, score, v)) if finished(&st) && v < version => json!({
        "by": "new_version",
        "to_version": version,
        "completed_ms": completed_ms,
        "status": st,
        "score": score,
        "version": v,
      }),
      _ => continue,
    };
    carehub_learning::reset_progress(&mut tx, c.ws, course, uid, data).await?;
    let cover = f.covers.get(&(course, uid)).unwrap_or(&none);
    // Rows that no longer cover them go (a sync would remove them now).
    if !cover.stale.is_empty() {
      sqlx::query(
        "DELETE FROM carehub_learning_assignee \
         WHERE course_id = $1 AND uid = $2 AND assignment_id = ANY($3)",
      )
      .bind(course)
      .bind(uid)
      .bind(cover.stale.clone())
      .execute(&mut *tx)
      .await?;
    }
    if cover.keep.is_empty() {
      // Not assigned: a fresh start on the new version (My learning → In progress).
      let started = sqlx::query(
        "INSERT INTO carehub_enrollment \
           (course_id, uid, version, status, progress, started_ms, last_ms) \
         VALUES ($1, $2, $3, 'in_progress', '{}'::jsonb, $4, $4) \
         ON CONFLICT (course_id, uid) DO NOTHING",
      )
      .bind(course)
      .bind(uid)
      .bind(version)
      .bind(a.now)
      .execute(&mut *tx)
      .await?;
      if started.rows_affected() > 0 {
        let data = json!({ "by": "new_version" });
        add_event(&mut tx, c.ws, course, uid, "start", &data, a.now).await?;
      }
      let body = format!(
        "🔁 “{}” has been updated. Please take it again. What changed: {}\n{}",
        title, note, link
      );
      messages.push((uid, body));
    } else {
      // Assigned: the renewal path (reminders re-armed, a pending "New course"
      // message marked done), due 23:59:59.999 local `days` days from today.
      let due = day_start(local_day(a.now, cover.tz) + days + 1, cover.tz) - 1;
      sqlx::query(
        "UPDATE carehub_learning_assignee SET renew_due_ms = $3, sent_due_ms = 0, \
           sent_overdue_ms = 0, \
           sent_assigned_ms = CASE WHEN sent_assigned_ms = 0 THEN -1 ELSE sent_assigned_ms END \
         WHERE course_id = $1 AND uid = $2",
      )
      .bind(course)
      .bind(uid)
      .bind(due)
      .execute(&mut *tx)
      .await?;
      let body = format!(
        "🔁 “{}” has been updated. Please take it again by {}. What changed: {}\n{}",
        title,
        day_label(local_day(due, cover.tz)),
        note,
        link
      );
      messages.push((uid, body));
      assigned_n += 1;
    }
  }
  let people_n = messages.len();
  if people_n != f.uids.len() {
    sqlx::query("UPDATE carehub_learning_announce SET people = $2 WHERE course_id = $1")
      .bind(course)
      .bind(count_i32(people_n))
      .execute(&mut *tx)
      .await?;
  }
  tx.commit().await?;
  send_later(pool, messages);
  after_change(pool, vec![course]);
  Ok(json!({
    "ok": true,
    "already": false,
    "version": version,
    "action": action,
    "people": people_n,
    "assigned": assigned_n,
  }))
}

/// P6 notices: a whole number of months 1-24, else None (no reminders).
fn review_value(v: Option<&Value>) -> Option<i64> {
  v.and_then(|x| {
    x.as_i64().or_else(|| {
      x.as_f64()
        .filter(|f| f.is_finite() && f.fract().abs() < 1e-9 && f.abs() < 1e9)
        .map(|f| f as i64)
    })
  })
  .filter(|n| (1..=MAX_REVIEW_MONTHS).contains(n))
}

/// P6 notices: review months of published versions, cached like RECERT.
async fn review_for(pool: &PgPool, live: &[(Uuid, i32)]) -> Result<HashMap<Uuid, i64>, AppError> {
  let mut out: HashMap<Uuid, i64> = HashMap::new();
  let mut ask_courses: Vec<Uuid> = Vec::new();
  let mut ask_versions: Vec<i32> = Vec::new();
  for key in live {
    let known = REVIEW.lock().ok().and_then(|m| m.get(key).copied());
    match known {
      Some(Some(n)) => {
        out.insert(key.0, n);
      },
      Some(None) => {},
      None => {
        ask_courses.push(key.0);
        ask_versions.push(key.1);
      },
    }
  }
  if ask_courses.is_empty() {
    return Ok(out);
  }
  let rows: Vec<(Uuid, i32, Option<String>)> = sqlx::query_as(
    "SELECT v.course_id, v.version, (v.content->'settings'->'review_months')::TEXT \
     FROM carehub_course_version v \
     JOIN UNNEST($1::UUID[], $2::INT[]) AS w(course_id, version) \
       ON w.course_id = v.course_id AND w.version = v.version",
  )
  .bind(ask_courses)
  .bind(ask_versions)
  .fetch_all(pool)
  .await?;
  for (course, version, raw) in rows {
    let value: Option<Value> = raw.and_then(|s| serde_json::from_str(&s).ok());
    let months = review_value(value.as_ref());
    if let Ok(mut m) = REVIEW.lock() {
      m.insert((course, version), months);
    }
    if let Some(n) = months {
      out.insert(course, n);
    }
  }
  Ok(out)
}

/// P6 notices: (last_kind, last_ms, due_ms) of a "check this course" clock.
fn review_clock(published_ms: i64, checked_ms: i64, months: i64) -> (&'static str, i64, i64) {
  let (kind, last) = if checked_ms > published_ms {
    ("checked", checked_ms)
  } else {
    ("published", published_ms)
  };
  (kind, last, add_months(last, months))
}

/// P6 notices: "3 Jun" (UTC day), with the year when it isn't this year.
fn short_date(ms: i64, now: i64) -> String {
  let (y, m, d) = civil_from_days(ms.div_euclid(DAY_MS));
  let (this_year, _, _) = civil_from_days(now.div_euclid(DAY_MS));
  let month = usize::try_from(m - 1)
    .ok()
    .and_then(|i| MONTHS.get(i).copied())
    .unwrap_or("");
  if y == this_year {
    format!("{} {}", d, month)
  } else {
    format!("{} {} {}", d, month, y)
  }
}

/// P6 notices: GET /content-checks (build): the clock of every published
/// course whose live version asks for reminders.
async fn content_checks(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_build(&c)?;
  let pool = &state.pg_pool;
  let rows: Vec<(Uuid, i32, i64, i64)> = sqlx::query_as(
    "SELECT c.id, c.published_version, v.published_ms, COALESCE(r.checked_ms, 0) \
     FROM carehub_course c \
     JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     LEFT JOIN carehub_learning_review r ON r.course_id = c.id \
     WHERE c.workspace_id = $1 AND c.status = 'published'",
  )
  .bind(c.ws)
  .fetch_all(pool)
  .await?;
  let live: Vec<(Uuid, i32)> = rows.iter().map(|r| (r.0, r.1)).collect();
  let months = review_for(pool, &live).await?;
  let now = now_ms();
  let mut checks: Vec<(i64, Value)> = Vec::new();
  for (course, _, published_ms, checked_ms) in rows {
    let n = match months.get(&course) {
      Some(n) => *n,
      None => continue,
    };
    let (kind, last_ms, due_ms) = review_clock(published_ms, checked_ms, n);
    checks.push((
      due_ms,
      json!({
        "course_id": course.to_string(),
        "every_months": n,
        "last_kind": kind,
        "last_ms": last_ms,
        "due_ms": due_ms,
        "due": now >= due_ms,
      }),
    ));
  }
  checks.sort_by(|a, b| a.0.cmp(&b.0));
  Ok(json!({ "checks": checks.into_iter().map(|x| x.1).collect::<Vec<Value>>() }))
}

/// P6 notices: POST /studio/{id}/mark-checked (build): restarts the clock
/// without publishing (any course status).
async fn mark_checked(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_build(&c)?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  body_json(body)?;
  let pool = &state.pg_pool;
  let head: Option<(String, Option<i32>, Option<i64>)> = sqlx::query_as(
    "SELECT c.status, c.published_version, v.published_ms FROM carehub_course c \
     LEFT JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.id = $1 AND c.workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(pool)
  .await?;
  let (status, live, published_ms) = head.ok_or_else(missing)?;
  let now = now_ms();
  sqlx::query(
    "INSERT INTO carehub_learning_review (course_id, workspace_id, checked_ms, checked_by) \
     VALUES ($1, $2, $3, $4) \
     ON CONFLICT (course_id) DO UPDATE SET checked_ms = EXCLUDED.checked_ms, \
       checked_by = EXCLUDED.checked_by",
  )
  .bind(course)
  .bind(c.ws)
  .bind(now)
  .bind(c.uid())
  .execute(pool)
  .await?;
  let due = match live {
    Some(v) if status == "published" => review_for(pool, &[(course, v)])
      .await?
      .get(&course)
      .map(|n| review_clock(published_ms.unwrap_or(0), now, *n).2),
    _ => None,
  };
  Ok(json!({ "ok": true, "checked_ms": now, "due_ms": due }))
}

/// P6 notices: a current non-guest member who can build courses.
async fn can_build(pool: &PgPool, ws: Uuid, uid: i64) -> bool {
  if uid <= 0 {
    return false;
  }
  match carehub_extras::me_in(pool, ws, uid).await {
    Ok(me) => {
      let has = |p: &str| me.perms.iter().any(|x| x == p || x == "admin");
      (me.af_role != guest_role() || has("admin")) && has("learning_build")
    },
    Err(_) => false,
  }
}

/// P6 notices: one "check this course" reminder that is due.
struct Check {
  course: Uuid,
  ws: Uuid,
  owner: i64,
  publisher: i64,
  kind: &'static str,
  last_ms: i64,
  due_ms: i64,
  checked_ms: i64,
}

/// P6 notices: "Time to check" messages to course owners (the creator, else
/// the publisher of the live version, if they can still build courses), once
/// per due date (claimed first), longest-waiting first, REVIEW_BATCH per run.
async fn review_reminders(pool: &PgPool) -> Result<(), AppError> {
  let now = now_ms();
  // A due is at least 28 days (one month) after the last publish or check.
  #[allow(clippy::type_complexity)]
  let rows: Vec<(Uuid, Uuid, i32, i64, i64, i64, i64, i64)> = sqlx::query_as(
    "SELECT c.id, c.workspace_id, c.published_version, c.created_by, v.published_by, \
       v.published_ms, COALESCE(r.checked_ms, 0), COALESCE(r.sent_due_ms, 0) \
     FROM carehub_course c \
     JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     LEFT JOIN carehub_learning_review r ON r.course_id = c.id \
     WHERE c.status = 'published' AND GREATEST(v.published_ms, COALESCE(r.checked_ms, 0)) <= $1",
  )
  .bind(now - 28 * DAY_MS)
  .fetch_all(pool)
  .await?;
  if rows.is_empty() {
    return Ok(());
  }
  let live: Vec<(Uuid, i32)> = rows.iter().map(|r| (r.0, r.2)).collect();
  let months = review_for(pool, &live).await?;
  let mut todo: Vec<Check> = Vec::new();
  for (course, ws, _, created_by, published_by, published_ms, checked_ms, sent_due_ms) in rows {
    let n = match months.get(&course) {
      Some(n) => *n,
      None => continue,
    };
    let (kind, last_ms, due_ms) = review_clock(published_ms, checked_ms, n);
    if now >= due_ms && sent_due_ms != due_ms {
      todo.push(Check {
        course,
        ws,
        owner: created_by,
        publisher: published_by,
        kind,
        last_ms,
        due_ms,
        checked_ms,
      });
    }
  }
  if todo.is_empty() {
    return Ok(());
  }
  todo.sort_by_key(|x| x.due_ms);
  todo.truncate(REVIEW_BATCH);
  let ids: Vec<Uuid> = todo.iter().map(|x| x.course).collect();
  let titles = titles_of(pool, &ids).await?;
  for x in &todo {
    let to = if can_build(pool, x.ws, x.owner).await {
      Some(x.owner)
    } else if can_build(pool, x.ws, x.publisher).await {
      Some(x.publisher)
    } else {
      None
    };
    // Not claimed: sent already, or marked as checked since we looked.
    let claimed = sqlx::query(
      "INSERT INTO carehub_learning_review \
         (course_id, workspace_id, sent_due_ms, sent_ms, sent_to) \
       VALUES ($1, $2, $3, $4, $5) \
       ON CONFLICT (course_id) DO UPDATE SET sent_due_ms = EXCLUDED.sent_due_ms, \
         sent_ms = EXCLUDED.sent_ms, sent_to = EXCLUDED.sent_to \
       WHERE carehub_learning_review.sent_due_ms <> EXCLUDED.sent_due_ms \
         AND carehub_learning_review.checked_ms = $6",
    )
    .bind(x.course)
    .bind(x.ws)
    .bind(x.due_ms)
    .bind(now)
    .bind(to)
    .bind(x.checked_ms)
    .execute(pool)
    .await?;
    if claimed.rows_affected() == 0 {
      continue;
    }
    let to = match to {
      Some(uid) => uid,
      None => {
        warn_rarely("review reminder: nobody who can build that course is left to tell");
        continue;
      },
    };
    let title = titles
      .get(&x.course)
      .map(|t| t.as_str())
      .unwrap_or("Untitled course");
    let what = if x.kind == "checked" {
      "checked"
    } else {
      "updated"
    };
    let body = format!(
      "📅 Time to check “{}”. It was last {} on {}.\n{}",
      title,
      what,
      short_date(x.last_ms, now),
      web_link(&format!("/app/learning/studio/{}", x.course))
    );
    send(pool, to, &body).await;
  }
  Ok(())
}

/// P6 notices: MarkedComplete.
fn marked_json(by_name: &str, ms: i64, reason: &str) -> Value {
  json!({ "by_name": by_name, "ms": ms, "reason": reason })
}

/// P6 notices: people on a course whose current finish was marked by hand
/// (status completed, the same finish time, not undone).
async fn marked_on(pool: &PgPool, ws: Uuid, course: Uuid) -> Result<HashMap<i64, Value>, AppError> {
  let rows: Vec<(i64, i64, String, String)> = sqlx::query_as(
    "SELECT m.uid, m.completed_ms, m.reason, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
     FROM carehub_learning_manual m \
     JOIN carehub_enrollment e ON e.course_id = m.course_id AND e.uid = m.uid \
       AND e.completed_ms = m.completed_ms AND e.status = 'completed' \
     LEFT JOIN af_user u ON u.uid = m.by_uid \
     WHERE m.workspace_id = $1 AND m.course_id = $2 AND m.undone_ms IS NULL",
  )
  .bind(ws)
  .bind(course)
  .fetch_all(pool)
  .await?;
  Ok(
    rows
      .into_iter()
      .map(|(uid, ms, reason, by_name)| (uid, marked_json(&by_name, ms, &reason)))
      .collect(),
  )
}

/// P6 notices: "marked" on progress rows (see add_row_expiry): course rows
/// match on uid, the others (no uid) on their start time; the database is
/// only asked when a row is completed. A failure leaves the field out.
async fn add_row_marked(pool: &PgPool, rows: &mut [Value], course: Option<Uuid>) {
  let mut courses: Vec<Uuid> = Vec::new();
  let mut times: Vec<i64> = Vec::new();
  for row in rows.iter() {
    if txt(row, "status") != "completed" {
      continue;
    }
    let t = row.get("completed_ms").and_then(|x| x.as_i64());
    if let (Some(c), Some(t)) = (row_course(row, course), t) {
      courses.push(c);
      times.push(t);
    }
  }
  #[allow(clippy::type_complexity)]
  let mut found: Vec<(Uuid, i64, i64, i64, String, String)> = Vec::new();
  if !courses.is_empty() {
    if ready(pool).await.is_err() {
      return;
    }
    #[allow(clippy::type_complexity)]
    let res: Result<Vec<(Uuid, i64, i64, i64, String, String)>, sqlx::Error> = sqlx::query_as(
      "SELECT m.course_id, m.completed_ms, m.uid, m.started_ms, m.reason, \
         COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), '') \
       FROM UNNEST($1::UUID[], $2::BIGINT[]) AS w(course_id, completed_ms) \
       JOIN carehub_learning_manual m ON m.course_id = w.course_id \
         AND m.completed_ms = w.completed_ms \
       LEFT JOIN af_user u ON u.uid = m.by_uid \
       WHERE m.undone_ms IS NULL",
    )
    .bind(courses)
    .bind(times)
    .fetch_all(pool)
    .await;
    match res {
      Ok(r) => found = r,
      Err(err) => {
        warn_rarely(&format!("marked complete: {}", err));
        return;
      },
    }
  }
  for row in rows.iter_mut() {
    let mut marked = Value::Null;
    if txt(row, "status") == "completed" {
      let c = row_course(row, course);
      let t = row.get("completed_ms").and_then(|x| x.as_i64());
      let uid = row
        .get("uid")
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse::<i64>().ok());
      let started = row.get("started_ms").and_then(|x| x.as_i64());
      let hit = found.iter().find(|m| {
        Some(m.0) == c
          && Some(m.1) == t
          && match uid {
            Some(u) => m.2 == u,
            None => Some(m.3) == started,
          }
      });
      if let Some(m) = hit {
        marked = marked_json(&m.5, m.1, &m.4);
      }
    }
    if let Some(o) = row.as_object_mut() {
      o.insert("marked".to_string(), marked);
    }
  }
}

/// P6 notices: POST /assignments/{id}/mark-complete (assign): counts as
/// finishing the course everywhere. Their progress before (and the renewal
/// dues the finish will end) is kept so it can be undone.
async fn mark_complete(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_assign(&c)?;
  let asg = parse_uuid(id, ASSIGNMENT_LINK_MSG)?;
  let body = body_json(body)?;
  let pool = &state.pg_pool;
  let by_name = names_of(pool, vec![c.uid()])
    .await?
    .remove(&c.uid())
    .unwrap_or_default();
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  let head: Option<(Uuid, String, Option<i32>)> = sqlx::query_as(
    "SELECT a.course_id, c.status, c.published_version FROM carehub_learning_assignment a \
     JOIN carehub_course c ON c.id = a.course_id WHERE a.id = $1 AND a.workspace_id = $2",
  )
  .bind(asg)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let (course, status, live) = head.ok_or_else(assignment_missing)?;
  if status == "archived" {
    return Err(bad(MARK_ARCHIVED_MSG));
  }
  let live = match live {
    Some(v) if status == "published" => v,
    _ => return Err(bad(MARK_DRAFT_MSG)),
  };
  let uid = uid_of(body.get("uid")).ok_or_else(|| bad(PERSON_LINK_MSG))?;
  let on: bool = sqlx::query_scalar(
    "SELECT EXISTS (SELECT 1 FROM carehub_learning_assignee s \
       JOIN af_workspace_member m ON m.workspace_id = $3 AND m.uid = s.uid \
       JOIN af_user u ON u.uid = s.uid \
       WHERE s.assignment_id = $1 AND s.uid = $2 AND m.role_id::INT <> $4 \
         AND COALESCE(u.email, '') NOT LIKE '%.removed-%')",
  )
  .bind(asg)
  .bind(uid)
  .bind(c.ws)
  .bind(guest_role())
  .fetch_one(&mut *tx)
  .await?;
  if !on {
    return Err(bad(NOT_ON_MSG));
  }
  let enr: Option<(String, i64, String)> = sqlx::query_as(
    "SELECT e.status, e.started_ms, to_jsonb(e)::TEXT FROM carehub_enrollment e \
     WHERE e.course_id = $1 AND e.uid = $2 FOR UPDATE",
  )
  .bind(course)
  .bind(uid)
  .fetch_optional(&mut *tx)
  .await?;
  if enr.as_ref().map(|e| finished(&e.0)).unwrap_or(false) {
    return Err(bad(DONE_MSG));
  }
  let reason = txt(&body, "reason").trim().to_string();
  if reason.chars().count() > MAX_REASON_CHARS {
    return Err(bad(REASON_MSG));
  }
  let dues: Vec<(Uuid, Option<i64>)> = sqlx::query_as(
    "SELECT assignment_id, renew_due_ms FROM carehub_learning_assignee \
     WHERE course_id = $1 AND uid = $2 AND renew_due_ms IS NOT NULL",
  )
  .bind(course)
  .bind(uid)
  .fetch_all(&mut *tx)
  .await?;
  let dues: Vec<Value> = dues
    .into_iter()
    .filter_map(|(a, d)| d.map(|d| json!({ "assignment_id": a.to_string(), "renew_due_ms": d })))
    .collect();
  // A finish time no other finish on this course has (reports match on it).
  let now = now_ms();
  let taken: Vec<i64> = sqlx::query_scalar(
    "SELECT completed_ms FROM carehub_enrollment \
     WHERE course_id = $1 AND completed_ms >= $2 AND completed_ms < $3 \
     UNION SELECT completed_ms FROM carehub_learning_manual \
     WHERE course_id = $1 AND completed_ms >= $2 AND completed_ms < $3",
  )
  .bind(course)
  .bind(now)
  .bind(now + 1000)
  .fetch_all(&mut *tx)
  .await?;
  let taken: HashSet<i64> = taken.into_iter().collect();
  let mut t = now;
  while taken.contains(&t) {
    t += 1;
  }
  let (from, started_ms, before) = match enr {
    Some((st, started, snapshot)) => {
      sqlx::query(
        "UPDATE carehub_enrollment SET status = 'completed', percent = 100, completed_ms = $3, \
           last_ms = $3, awaiting_ack = FALSE WHERE course_id = $1 AND uid = $2",
      )
      .bind(course)
      .bind(uid)
      .bind(t)
      .execute(&mut *tx)
      .await?;
      (st, started, Some(snapshot))
    },
    None => {
      sqlx::query(
        "INSERT INTO carehub_enrollment \
           (course_id, uid, version, status, progress, started_ms, last_ms, completed_ms, percent) \
         VALUES ($1, $2, $3, 'completed', '{}'::jsonb, $4, $4, $4, 100)",
      )
      .bind(course)
      .bind(uid)
      .bind(live)
      .bind(t)
      .execute(&mut *tx)
      .await?;
      ("not_started".to_string(), t, None)
    },
  };
  let data = json!({
    "from": from,
    "to": "completed",
    "by": "manual",
    "by_uid": c.uid().to_string(),
  });
  let event = add_event(&mut tx, c.ws, course, uid, "status", &data, t).await?;
  sqlx::query(
    "INSERT INTO carehub_learning_manual (course_id, uid, completed_ms, workspace_id, \
       started_ms, by_uid, reason, before, renew_dues, event_id) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10)",
  )
  .bind(course)
  .bind(uid)
  .bind(t)
  .bind(c.ws)
  .bind(started_ms)
  .bind(c.uid())
  .bind(&reason)
  .bind(before)
  .bind(Value::Array(dues).to_string())
  .bind(event)
  .execute(&mut *tx)
  .await?;
  tx.commit().await?;
  after_change(pool, vec![course]);
  Ok(json!({
    "ok": true,
    "completed_ms": t,
    "marked": marked_json(&by_name, t, &reason),
  }))
}

/// P6 notices: POST /assignments/{id}/undo-complete (assign): while the
/// hand-marked finish is still their current one, their progress goes back
/// exactly as it was; that finish's certificate, renewal row and status event
/// go, and the renewal dues it ended come back.
async fn undo_complete(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  need_assign(&c)?;
  let asg = parse_uuid(id, ASSIGNMENT_LINK_MSG)?;
  let body = body_json(body)?;
  let pool = &state.pg_pool;
  let mut tx = pool.begin().await?;
  lock_ws(&mut tx, c.ws).await?;
  let course: Option<Uuid> = sqlx::query_scalar(
    "SELECT course_id FROM carehub_learning_assignment WHERE id = $1 AND workspace_id = $2",
  )
  .bind(asg)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let course = course.ok_or_else(assignment_missing)?;
  let uid = uid_of(body.get("uid")).ok_or_else(|| bad(PERSON_LINK_MSG))?;
  let manual: Option<(i64, Option<String>, String, Option<i64>)> = sqlx::query_as(
    "SELECT completed_ms, before::TEXT, renew_dues::TEXT, event_id FROM carehub_learning_manual \
     WHERE course_id = $1 AND uid = $2 AND workspace_id = $3 AND undone_ms IS NULL \
     ORDER BY completed_ms DESC LIMIT 1 FOR UPDATE",
  )
  .bind(course)
  .bind(uid)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let (t, before, dues, event) = manual.ok_or_else(|| bad(UNDO_MSG))?;
  let enr: Option<(String, Option<i64>)> = sqlx::query_as(
    "SELECT status, completed_ms FROM carehub_enrollment \
     WHERE course_id = $1 AND uid = $2 FOR UPDATE",
  )
  .bind(course)
  .bind(uid)
  .fetch_optional(&mut *tx)
  .await?;
  match enr {
    Some((st, Some(done))) if st == "completed" && done == t => {},
    _ => return Err(bad(UNDO_MSG)),
  }
  match &before {
    Some(snapshot) => {
      sqlx::query(
        "UPDATE carehub_enrollment e SET version = b.version, status = b.status, \
           progress = b.progress, score = b.score, attempts = b.attempts, time_ms = b.time_ms, \
           started_ms = b.started_ms, last_ms = b.last_ms, completed_ms = b.completed_ms, \
           ack_ms = b.ack_ms, awaiting_ack = b.awaiting_ack, percent = b.percent, \
           quiz_passed = b.quiz_passed \
         FROM jsonb_populate_record(NULL::carehub_enrollment, $3::jsonb) AS b \
         WHERE e.course_id = $1 AND e.uid = $2",
      )
      .bind(course)
      .bind(uid)
      .bind(snapshot)
      .execute(&mut *tx)
      .await?;
    },
    None => {
      sqlx::query("DELETE FROM carehub_enrollment WHERE course_id = $1 AND uid = $2")
        .bind(course)
        .bind(uid)
        .execute(&mut *tx)
        .await?;
    },
  }
  sqlx::query(
    "DELETE FROM carehub_learning_certificate WHERE course_id = $1 AND uid = $2 AND issued_ms = $3",
  )
  .bind(course)
  .bind(uid)
  .bind(t)
  .execute(&mut *tx)
  .await?;
  sqlx::query(
    "DELETE FROM carehub_learning_renewal WHERE course_id = $1 AND uid = $2 AND finished_ms = $3",
  )
  .bind(course)
  .bind(uid)
  .bind(t)
  .execute(&mut *tx)
  .await?;
  if let Some(ev) = event {
    sqlx::query("DELETE FROM carehub_learning_event WHERE id = $1 AND course_id = $2 AND uid = $3")
      .bind(ev)
      .bind(course)
      .bind(uid)
      .execute(&mut *tx)
      .await?;
  }
  let mut ids: Vec<Uuid> = Vec::new();
  let mut times: Vec<i64> = Vec::new();
  if let Ok(Value::Array(list)) = serde_json::from_str::<Value>(&dues) {
    for d in &list {
      let due = d.get("renew_due_ms").and_then(|x| x.as_i64());
      if let (Ok(a), Some(due)) = (Uuid::parse_str(txt(d, "assignment_id")), due) {
        ids.push(a);
        times.push(due);
      }
    }
  }
  if !ids.is_empty() {
    sqlx::query(
      "UPDATE carehub_learning_assignee s SET renew_due_ms = w.due \
       FROM UNNEST($3::UUID[], $4::BIGINT[]) AS w(id, due) \
       WHERE s.assignment_id = w.id AND s.course_id = $1 AND s.uid = $2 \
         AND s.renew_due_ms IS NULL",
    )
    .bind(course)
    .bind(uid)
    .bind(ids)
    .bind(times)
    .execute(&mut *tx)
    .await?;
  }
  sqlx::query(
    "UPDATE carehub_learning_manual SET undone_ms = $4, undone_by = $5 \
     WHERE course_id = $1 AND uid = $2 AND completed_ms = $3",
  )
  .bind(course)
  .bind(uid)
  .bind(t)
  .bind(now_ms())
  .bind(c.uid())
  .execute(&mut *tx)
  .await?;
  tx.commit().await?;
  after_change(pool, vec![course]);
  let snapshot: Value = before
    .as_deref()
    .and_then(|s| serde_json::from_str(s).ok())
    .unwrap_or(Value::Null);
  let status = match txt(&snapshot, "status") {
    "" => "not_started",
    s => s,
  };
  Ok(json!({
    "ok": true,
    "status": status,
    "percent": num(&snapshot, "percent", 0),
  }))
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
  // P2 recert: renewal dues end once they finish again; then heads-ups and renewals.
  if let Err(err) = end_renewal_dues(pool).await {
    warn_rarely(&format!("renewal dues: {}", err));
  }
  if let Err(err) = renewals(pool, None).await {
    warn_rarely(&format!("renewals: {}", err));
  }
  if let Err(err) = deliver(pool, None).await {
    warn_rarely(&format!("reminders: {}", err));
  }
  if let Err(err) = sync_calendar(pool, None).await {
    warn_rarely(&format!("calendar entries: {}", err));
  }
  // P3 paths
  if let Err(err) = carehub_learning_paths::tick_paths(pool).await {
    warn_rarely(&format!("learning paths: {}", err));
  }
  if let Err(err) = issue_certificates(pool, None, None, None).await {
    warn_rarely(&format!("certificates: {}", err));
  }
  // P6 notices: "check this course" reminders.
  if let Err(err) = review_reminders(pool).await {
    warn_rarely(&format!("review reminders: {}", err));
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

/// P2 recert
async fn retake_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(retake(&state, &user, &ws, &id).await)
}

async fn teams_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  reply(teams(&state, &user, &ws).await)
}

/// P6 notices
async fn announce_info_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(announce_info(&state, &user, &ws, &id).await)
}

async fn announce_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(announce(&state, &user, &ws, &id, &body).await)
}

async fn content_checks_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
) -> Reply {
  let ws = path.into_inner();
  reply(content_checks(&state, &user, &ws).await)
}

async fn mark_checked_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(mark_checked(&state, &user, &ws, &id, &body).await)
}

async fn mark_complete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(mark_complete(&state, &user, &ws, &id, &body).await)
}

async fn undo_complete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(undo_complete(&state, &user, &ws, &id, &body).await)
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
    )
    // P2 recert
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/courses/{id}/retake")
        .route(web::post().to(retake_handler)),
    )
    // P6 notices
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/announce")
        .route(web::get().to(announce_info_handler))
        .route(web::post().to(announce_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/mark-checked")
        .route(web::post().to(mark_checked_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/content-checks")
        .route(web::get().to(content_checks_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/assignments/{id}/mark-complete")
        .route(web::post().to(mark_complete_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/assignments/{id}/undo-complete")
        .route(web::post().to(undo_complete_handler)),
    );
  // P3 paths
  carehub_learning_paths::configure(cfg);
}
