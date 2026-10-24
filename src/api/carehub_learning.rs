//! CareHub Learning: build courses (Studio), take them (Player), My learning
//! and the catalog, and course reports.
//!
//! Courses live in a few small `carehub_*` tables that are created on first
//! use. If that fails the rest of the server keeps running and the learning
//! endpoints answer with a friendly "still being set up" message instead.
//!
//! Every workspace member except guests is a learner. Building courses needs
//! the `learning_build` permission; reports need `learning_build` or
//! `learning_analytics`. `admin` implies both.

use crate::api::carehub_extras::{me_in, Me};
use crate::api::carehub_learning_assign; // paths3 (P2 recert): hooks below
use crate::api::carehub_learning_bank as qbank; // qbank3: hooks below
use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Bytes, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use database_entity::dto::AFRole;
use serde_json::{json, Map, Value};
use shared_entity::response::AppResponse;
use sqlx::{PgConnection, PgPool};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

static READY: AtomicBool = AtomicBool::new(false);

const MAX_CONTENT_BYTES: usize = 4 * 1024 * 1024;
const STUDIO_BODY_BYTES: usize = 8 * 1024 * 1024;
const PLAYER_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_EVENTS: usize = 200;
/// S3: progress() stops storing events for a person and course past this.
const MAX_STORED_EVENTS: i64 = 5_000;
const HEARTBEAT_CAP_MS: i64 = 60_000;
const QUIZ_LESSON: &str = "_quiz";
const MAX_TITLE_CHARS: usize = 200;
const MAX_KC_BYTES: usize = 20_000;
const TABLE_LOCK_KEY: i64 = 7_316_402_219;

const GUEST_MSG: &str = "Courses aren't available for guest accounts yet.";
const NOT_MEMBER_MSG: &str = "You're not a member of this workspace.";
const BUILD_MSG: &str =
  "You need the \"Build courses\" permission for this. Ask an admin to update your role.";
const RESET_MSG: &str =
  "Only people who can build courses can reset someone's progress. Ask an admin to update your role.";
const REPORTS_MSG: &str =
  "You need the \"See course reports\" permission for this. Ask an admin to update your role.";
const SETUP_MSG: &str = "Learning is still being set up. Please try again in a minute.";
const SERVER_MSG: &str = "Something went wrong on the server. Please try again in a moment.";
const BODY_MSG: &str = "That request couldn't be read. Please reload the page and try again.";
const WS_LINK_MSG: &str = "That workspace link doesn't look right.";
const COURSE_LINK_MSG: &str = "That course link doesn't look right.";
const PERSON_LINK_MSG: &str = "That person link doesn't look right.";
const VERSION_LINK_MSG: &str = "That version number doesn't look right.";
const UNAVAILABLE_MSG: &str = "This course isn't available.";
const MISSING_MSG: &str = "That course couldn't be found. It may have been deleted.";
const VERSION_MISSING_MSG: &str = "That version of the course couldn't be found.";
const PERSON_MISSING_MSG: &str = "That person isn't a member of this workspace.";
const STALE_MSG: &str =
  "Someone else saved this course a moment ago. Reload to see their changes.";
const TOO_BIG_MSG: &str =
  "This course is too big to save (over 4 MB). Use links for big images and videos.";
const DRAFT_SHAPE_MSG: &str = "The course couldn't be saved because its content looks damaged. Please reload the page.";
const PUBLISHED_DELETE_MSG: &str = "This course has been published. Archive it instead.";
const NOT_STARTED_MSG: &str = "Start this course first.";
const NO_QUIZ_MSG: &str = "This course doesn't have a quiz.";
const QUIZ_LESSONS_MSG: &str = "Finish all lessons first.";
const QUIZ_PASSED_MSG: &str = "You've already passed this quiz.";
const QUIZ_NO_ATTEMPTS_MSG: &str = "No attempts left.";
const ACK_EARLY_MSG: &str = "There's nothing to acknowledge yet. Finish the course first.";
const TOO_MANY_MSG: &str = "Too many updates at once. Please try again.";
const UNKNOWN_LESSON_MSG: &str = "That lesson isn't part of this course any more.";
const UNKNOWN_BLOCK_MSG: &str = "That part of the lesson isn't in this course any more.";
const UNKNOWN_EVENT_MSG: &str = "That kind of update isn't recognised.";
const CONTINUE_MSG: &str = "Finish the practice questions and steps in this lesson first.";

const TABLES: &[&str] = &[
  "CREATE TABLE IF NOT EXISTS carehub_course (\
     id UUID PRIMARY KEY, workspace_id UUID NOT NULL, title TEXT NOT NULL DEFAULT '', \
     status TEXT NOT NULL DEFAULT 'draft', draft JSONB NOT NULL DEFAULT '{}'::jsonb, \
     draft_updated_ms BIGINT NOT NULL DEFAULT 0, updated_by BIGINT NOT NULL DEFAULT 0, \
     published_version INT, created_by BIGINT NOT NULL DEFAULT 0, \
     created_ms BIGINT NOT NULL DEFAULT 0, updated_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE INDEX IF NOT EXISTS carehub_course_ws ON carehub_course (workspace_id, updated_ms)",
  "CREATE TABLE IF NOT EXISTS carehub_course_version (\
     course_id UUID NOT NULL REFERENCES carehub_course (id) ON DELETE CASCADE, \
     version INT NOT NULL, content JSONB NOT NULL DEFAULT '{}'::jsonb, \
     note TEXT NOT NULL DEFAULT '', published_by BIGINT NOT NULL DEFAULT 0, \
     published_ms BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (course_id, version))",
  "CREATE TABLE IF NOT EXISTS carehub_enrollment (\
     course_id UUID NOT NULL REFERENCES carehub_course (id) ON DELETE CASCADE, \
     uid BIGINT NOT NULL, version INT NOT NULL, status TEXT NOT NULL DEFAULT 'in_progress', \
     progress JSONB NOT NULL DEFAULT '{}'::jsonb, score INT, attempts INT NOT NULL DEFAULT 0, \
     time_ms BIGINT NOT NULL DEFAULT 0, started_ms BIGINT NOT NULL DEFAULT 0, \
     last_ms BIGINT NOT NULL DEFAULT 0, completed_ms BIGINT, ack_ms BIGINT, \
     awaiting_ack BOOLEAN NOT NULL DEFAULT FALSE, percent INT NOT NULL DEFAULT 0, \
     quiz_passed BOOLEAN NOT NULL DEFAULT FALSE, PRIMARY KEY (course_id, uid))",
  "CREATE INDEX IF NOT EXISTS carehub_enrollment_uid ON carehub_enrollment (uid, last_ms)",
  "CREATE TABLE IF NOT EXISTS carehub_learning_event (\
     id BIGSERIAL PRIMARY KEY, workspace_id UUID NOT NULL, course_id UUID NOT NULL, \
     uid BIGINT NOT NULL, kind TEXT NOT NULL, lesson_id TEXT NOT NULL DEFAULT '', \
     block_id TEXT NOT NULL DEFAULT '', data JSONB, ts_ms BIGINT NOT NULL)",
  "CREATE INDEX IF NOT EXISTS carehub_learning_event_course \
     ON carehub_learning_event (course_id, uid, ts_ms)",
  "CREATE INDEX IF NOT EXISTS carehub_learning_event_ws ON carehub_learning_event (workspace_id, ts_ms)",
  "CREATE TABLE IF NOT EXISTS carehub_quiz_attempt (\
     id BIGSERIAL PRIMARY KEY, course_id UUID NOT NULL, uid BIGINT NOT NULL, \
     version INT NOT NULL, attempt INT NOT NULL, answers JSONB NOT NULL DEFAULT '{}'::jsonb, \
     results JSONB NOT NULL DEFAULT '[]'::jsonb, score INT NOT NULL DEFAULT 0, \
     passed BOOLEAN NOT NULL DEFAULT FALSE, created_ms BIGINT NOT NULL DEFAULT 0)",
  "CREATE INDEX IF NOT EXISTS carehub_quiz_attempt_person \
     ON carehub_quiz_attempt (course_id, uid, created_ms)",
  "CREATE INDEX IF NOT EXISTS carehub_quiz_attempt_version ON carehub_quiz_attempt (course_id, version)",
];

/// Creates the learning tables once. A failure is logged and turned into a
/// friendly message; it never stops the server.
pub(crate) async fn ready(pool: &PgPool) -> Result<(), AppError> {
  if READY.load(Ordering::Relaxed) {
    return Ok(());
  }
  match create_tables(pool).await {
    Ok(()) => {
      READY.store(true, Ordering::Relaxed);
      Ok(())
    },
    Err(err) => {
      tracing::warn!("carehub learning: could not create tables: {}", err);
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
      tracing::warn!("carehub learning: {}", other);
      bad(SERVER_MSG)
    },
  }
}

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn gone() -> AppError {
  AppError::RecordNotFound(UNAVAILABLE_MSG.to_string())
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

fn clean_title(raw: &str) -> String {
  let t = raw.trim();
  if t.is_empty() {
    return "Untitled course".to_string();
  }
  t.chars().take(MAX_TITLE_CHARS).collect()
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

/// Who is calling, in which workspace.
pub(crate) struct Ctx {
  pub(crate) ws: Uuid,
  me: Me,
}

impl Ctx {
  pub(crate) fn uid(&self) -> i64 {
    self.me.uid
  }

  pub(crate) fn has(&self, perm: &str) -> bool {
    self.me.perms.iter().any(|p| p == perm || p == "admin")
  }

  fn need_build(&self) -> Result<(), AppError> {
    if self.has("learning_build") {
      Ok(())
    } else {
      Err(bad(BUILD_MSG))
    }
  }

  fn need_reports(&self) -> Result<(), AppError> {
    if self.has("learning_build") || self.has("learning_analytics") {
      Ok(())
    } else {
      Err(bad(REPORTS_MSG))
    }
  }
}

/// Signed-in, non-guest member of the workspace, with the tables ready.
pub(crate) async fn ctx(state: &AppState, user: &UserUuid, ws: &str) -> Result<Ctx, AppError> {
  let ws = parse_uuid(ws, WS_LINK_MSG)?;
  let uid = state.user_cache.get_user_uid(user).await?;
  let me = me_in(&state.pg_pool, ws, uid).await.map_err(|err| match err {
    AppError::NotEnoughPermissions => bad(NOT_MEMBER_MSG),
    other => other,
  })?;
  let c = Ctx { ws, me };
  if c.me.af_role == guest_role() && !c.has("admin") {
    return Err(bad(GUEST_MSG));
  }
  ready(&state.pg_pool).await?;
  Ok(c)
}

// ---------------------------------------------------------------------------
// Course content helpers
// ---------------------------------------------------------------------------

/// The settings that matter on the server, with defaults and limits applied.
struct Settings {
  sequential: bool,
  min_seconds: i64,
  pass_mark: i64,
  max_attempts: i64,
  show_answers: String,
  require_ack: bool,
  completion: String,
  quiz_on: bool,
  quiz_needed: bool,
  lessons_needed: bool,
  pool_size: usize,
  shuffle: bool,
}

fn settings_of(content: &Value) -> Settings {
  let empty = json!({});
  let st = match content.get("settings") {
    Some(v) if v.is_object() => v,
    _ => &empty,
  };
  let quiz = match content.get("quiz") {
    Some(v) if v.is_object() => v,
    _ => &empty,
  };
  let quiz_on = flag(quiz, "enabled", false) && !list(quiz, "questions").is_empty();
  let completion = match txt(st, "completion") {
    "pass_quiz" if quiz_on => "pass_quiz",
    "all_lessons_and_quiz" if quiz_on => "all_lessons_and_quiz",
    _ => "all_lessons",
  };
  let show_answers = match txt(st, "show_answers") {
    "never" => "never",
    "after_submit" => "after_submit",
    _ => "after_pass",
  };
  Settings {
    sequential: flag(st, "sequential", false),
    min_seconds: num(st, "min_seconds_per_lesson", 0).clamp(0, 86_400),
    pass_mark: num(st, "pass_mark", 80).clamp(0, 100),
    max_attempts: num(st, "max_attempts", 0).clamp(0, 20),
    show_answers: show_answers.to_string(),
    require_ack: flag(st, "require_ack", false),
    completion: completion.to_string(),
    quiz_on,
    quiz_needed: completion != "all_lessons",
    lessons_needed: completion != "pass_quiz",
    pool_size: usize::try_from(num(quiz, "pool_size", 0).max(0)).unwrap_or(0),
    shuffle: flag(quiz, "shuffle", false),
  }
}

fn required_seconds(st: &Settings, lesson: &Value) -> i64 {
  let own = num(lesson, "min_seconds", 0).clamp(0, 86_400);
  if own > 0 {
    own
  } else {
    st.min_seconds
  }
}

fn clock(secs: i64) -> String {
  let secs = secs.max(0);
  format!("{}:{:02}", secs / 60, secs % 60)
}

fn has_block(lesson: &Value, block_id: &str) -> bool {
  !block_id.is_empty() && list(lesson, "blocks").iter().any(|b| txt(b, "id") == block_id)
}

fn points_of(q: &Value) -> i64 {
  num(q, "points", 1).clamp(1, 1000)
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

// Stable hashing for the deterministic quiz pool / order and matching tokens
// (FNV-1a plus a splitmix64 finaliser; no extra crates needed).

fn fnv(text: &str) -> u64 {
  let mut h: u64 = 0xcbf2_9ce4_8422_2325;
  for b in text.as_bytes() {
    h ^= u64::from(*b);
    h = h.wrapping_mul(0x0000_0100_0000_01b3);
  }
  h
}

fn mix(mut z: u64) -> u64 {
  z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
  z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
  z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
  z ^ (z >> 31)
}

// qbank3: pub(crate) for the question bank draws.
pub(crate) fn shuffle<T>(items: &mut [T], seed: &str) {
  let mut state = fnv(seed);
  let n = items.len();
  if n < 2 {
    return;
  }
  for i in (1..n).rev() {
    state = mix(state);
    let j = (state % ((i as u64) + 1)) as usize;
    items.swap(i, j);
  }
}

/// Matching tokens from before S2 (person + pair id). Only used to read quiz
/// attempts stored before that change (insights); answers now use text tokens.
fn old_pair_token(course: Uuid, uid: i64, pair_id: &str) -> String {
  let h = mix(fnv(&format!("{}:{}:{}", course, uid, pair_id)));
  format!("{:016x}", h).chars().take(12).collect()
}

// Fill in the blank, put in order and click the right spot (wave 3 kinds).

const MAX_BLANK_ANSWER_CHARS: usize = 200;
const MAX_ACCEPTED_CHARS: usize = 80;
const SPOT_MIN_SIZE: f64 = 2.0;
const SPOT_SLACK: f64 = 0.000_001;

/// Whitespace exactly as browsers see it in `trim()` and `/\s/`, so blanks
/// are matched the same way here and in the web app.
fn js_space(c: char) -> bool {
  (c.is_whitespace() && c != '\u{85}') || c == '\u{feff}'
}

fn js_trim(text: &str) -> &str {
  text.trim_matches(js_space)
}

/// Fill-in-the-blank match form: trim, collapse whitespace runs to one space,
/// drop one trailing ".", then lowercase unless capitals must match.
fn normalise_blank(raw: &str, case_sensitive: bool) -> String {
  let mut out = String::with_capacity(raw.len());
  let mut gap = false;
  for ch in js_trim(raw).chars() {
    if js_space(ch) {
      gap = true;
      continue;
    }
    if gap {
      out.push(' ');
      gap = false;
    }
    out.push(ch);
  }
  if out.ends_with('.') {
    out.pop();
  }
  if case_sensitive {
    out
  } else {
    out.to_lowercase()
  }
}

/// A fill-in-the-blank question's accepted answers (trimmed, blanks skipped).
fn blank_answers(q: &Value) -> Vec<&str> {
  list(q, "answers")
    .iter()
    .filter_map(|a| a.as_str())
    .map(js_trim)
    .filter(|a| !normalise_blank(a, true).is_empty())
    .collect()
}

fn blank_correct(q: &Value, answer: &Value) -> bool {
  let given = match answer.as_str() {
    Some(s) if s.chars().count() <= MAX_BLANK_ANSWER_CHARS => s,
    _ => return false,
  };
  let case_sensitive = flag(q, "case_sensitive", false);
  let got = normalise_blank(given, case_sensitive);
  !got.is_empty()
    && blank_answers(q)
      .iter()
      .any(|a| normalise_blank(a, case_sensitive) == got)
}

// S2: answer tokens for matching right sides and put-in-order items come only
// from the text, so neither a token nor the order it's shown in gives the right
// answer away.

/// The first 12 hex of mix(fnv("<course>:<text>")), plus "~2", "~3"... when the
/// same text comes up again in the question, so tokens stay unique. Never built
/// from ids, the person or the author's order.
fn text_tokens(course: Uuid, texts: &[&str]) -> Vec<String> {
  let mut seen: HashMap<&str, usize> = HashMap::new();
  texts
    .iter()
    .map(|text| {
      let n = seen.entry(*text).or_insert(0);
      *n += 1;
      let base: String = format!("{:016x}", mix(fnv(&format!("{}:{}", course, text))))
        .chars()
        .take(12)
        .collect();
      if *n > 1 {
        format!("{}~{}", base, n)
      } else {
        base
      }
    })
    .collect()
}

/// Each token back to its text (tokens are recomputed from the question).
fn text_by_token<'a>(course: Uuid, texts: &[&'a str]) -> HashMap<String, &'a str> {
  text_tokens(course, texts)
    .into_iter()
    .zip(texts.iter().copied())
    .collect()
}

/// A matching question's right sides (trimmed), in the author's order.
fn right_texts(q: &Value) -> Vec<&str> {
  list(q, "pairs")
    .iter()
    .map(|p| txt(p, "right").trim())
    .collect()
}

/// A put-in-order question's item texts (trimmed), in the right order.
fn item_texts(q: &Value) -> Vec<&str> {
  list(q, "items")
    .iter()
    .map(|it| txt(it, "text").trim())
    .collect()
}

/// Put-in-order items as learners get them: text tokens in a fresh random
/// order on every request (S2: no per-person seed a browser could undo), and
/// never already in the right order unless every item reads the same.
fn shuffled_items(q: &Value, course: Uuid) -> Vec<Value> {
  let items = list(q, "items");
  let texts = item_texts(q);
  let tokens = text_tokens(course, &texts);
  let mut idx: Vec<usize> = (0..items.len()).collect();
  shuffle(&mut idx, &Uuid::new_v4().to_string());
  let reads_right = idx
    .iter()
    .enumerate()
    .all(|(pos, i)| texts.get(*i) == texts.get(pos));
  if reads_right && idx.len() > 1 {
    idx.rotate_left(1);
  }
  idx
    .iter()
    .filter_map(|i| match (tokens.get(*i), items.get(*i)) {
      (Some(token), Some(it)) => Some(json!({ "token": token, "text": txt(it, "text") })),
      _ => None,
    })
    .collect()
}

/// Every token used once, and the texts read in the right order (items with
/// the same text are interchangeable, like matching's right sides).
fn ordering_correct(q: &Value, answer: &Value, course: Uuid) -> bool {
  let texts = item_texts(q);
  let got = match answer.as_array() {
    Some(a) => a,
    None => return false,
  };
  if texts.is_empty() || got.len() != texts.len() {
    return false;
  }
  let by_token: HashMap<String, usize> = text_tokens(course, &texts)
    .into_iter()
    .enumerate()
    .map(|(i, token)| (token, i))
    .collect();
  let mut used: HashSet<usize> = HashSet::new();
  got
    .iter()
    .enumerate()
    .all(|(pos, t)| match t.as_str().and_then(|t| by_token.get(t)) {
      Some(i) => used.insert(*i) && texts.get(*i) == texts.get(pos),
      None => false,
    })
}

fn coord(v: &Value, key: &str) -> Option<f64> {
  v.get(key)
    .and_then(|x| x.as_f64())
    .filter(|x| x.is_finite())
}

/// A right spot as (x, y, w, h), in percent of the picture.
fn spot_of(s: &Value) -> Option<(f64, f64, f64, f64)> {
  Some((coord(s, "x")?, coord(s, "y")?, coord(s, "w")?, coord(s, "h")?))
}

fn spot_inside(s: &Value) -> bool {
  match spot_of(s) {
    Some((x, y, w, h)) => {
      x >= -SPOT_SLACK
        && y >= -SPOT_SLACK
        && w >= SPOT_MIN_SIZE - SPOT_SLACK
        && h >= SPOT_MIN_SIZE - SPOT_SLACK
        && x + w <= 100.0 + SPOT_SLACK
        && y + h <= 100.0 + SPOT_SLACK
    },
    None => false,
  }
}

/// The click `{x, y}` (percent) is inside a right spot; edges count.
fn hotspot_hit(q: &Value, answer: &Value) -> bool {
  let (x, y) = match (coord(answer, "x"), coord(answer, "y")) {
    (Some(x), Some(y)) => (x, y),
    _ => return false,
  };
  if !(0.0..=100.0).contains(&x) || !(0.0..=100.0).contains(&y) {
    return false;
  }
  list(q, "spots")
    .iter()
    .filter_map(spot_of)
    .any(|(sx, sy, sw, sh)| x >= sx && x <= sx + sw && y >= sy && y <= sy + sh)
}

/// The hotspot picture as learners get it (never the right spots).
fn public_image(q: &Value) -> Value {
  let none = Value::Null;
  let image = q.get("image").unwrap_or(&none);
  json!({ "url": txt(image, "url"), "alt": txt(image, "alt") })
}

/// Insights: for each pair of a matching question, whether a stored answer
/// picked its right side. Reads today's text tokens (S2) and, for attempts
/// stored before S2, the old per-person tokens.
pub(crate) fn matching_hits(
  q: &Value,
  course: Uuid,
  uid: i64,
  chosen: &Map<String, Value>,
) -> Vec<bool> {
  let pairs = list(q, "pairs");
  let texts = right_texts(q);
  let mut by_token: HashMap<String, &str> = pairs
    .iter()
    .zip(texts.iter())
    .map(|(p, text)| (old_pair_token(course, uid, txt(p, "id")), *text))
    .collect();
  by_token.extend(text_by_token(course, &texts));
  pairs
    .iter()
    .zip(texts.iter())
    .map(|(p, want)| {
      chosen
        .get(txt(p, "id"))
        .and_then(|t| t.as_str())
        .and_then(|t| by_token.get(t))
        .map(|got| got == want)
        .unwrap_or(false)
    })
    .collect()
}

const MAX_BLANK_ROWS: usize = 10;

/// Insights rows (`questions[].answers`) for fill in the blank, put in order
/// and click the right spot; None for the other kinds. `attempts` holds
/// (uid, answers, results) of the quiz attempts on the live version.
pub(crate) fn insight_rows(q: &Value, attempts: &[(i64, Value, Value)]) -> Option<Vec<Value>> {
  let kind = kind_of(q);
  if !matches!(kind, "fill_blank" | "ordering" | "hotspot") {
    return None;
  }
  let qid = txt(q, "id");
  let case_sensitive = flag(q, "case_sensitive", false);
  let mut asked: i64 = 0;
  let mut right: i64 = 0;
  let mut other: i64 = 0;
  let mut counts: HashMap<String, i64> = HashMap::new();
  for (_, answers, results) in attempts {
    let result = match results
      .as_array()
      .and_then(|r| r.iter().find(|x| txt(x, "question_id") == qid))
    {
      Some(r) => r,
      None => continue,
    };
    asked += 1;
    if flag(result, "correct", false) {
      right += 1;
    }
    if kind != "fill_blank" {
      continue;
    }
    match answers.get(qid).and_then(|a| a.as_str()) {
      Some(given) if given.chars().count() > MAX_BLANK_ANSWER_CHARS => other += 1,
      Some(given) => {
        let text = normalise_blank(given, case_sensitive);
        if !text.is_empty() {
          *counts.entry(text).or_insert(0) += 1;
        }
      },
      None => {},
    }
  }
  let rows = match kind {
    "ordering" => vec![
      json!({ "id": "right", "text": "Right order", "correct": true, "picked": right }),
      json!({ "id": "wrong", "text": "Wrong order", "correct": false, "picked": asked - right }),
    ],
    "hotspot" => vec![
      json!({ "id": "hit", "text": "Clicked a right spot", "correct": true, "picked": right }),
      json!({ "id": "miss", "text": "Missed", "correct": false, "picked": asked - right }),
    ],
    _ => {
      let accepted: HashSet<String> = blank_answers(q)
        .iter()
        .map(|a| normalise_blank(a, case_sensitive))
        .collect();
      let mut common: Vec<(String, i64)> = counts.into_iter().collect();
      common.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
      let mut rows: Vec<Value> = Vec::new();
      for (i, (text, picked)) in common.into_iter().enumerate() {
        if i < MAX_BLANK_ROWS {
          let correct = accepted.contains(&text);
          rows.push(json!({
            "id": format!("a{}", i + 1),
            "text": text,
            "correct": correct,
            "picked": picked,
          }));
        } else {
          other += picked;
        }
      }
      if other > 0 {
        rows.push(json!({ "id": "other", "text": "Other answers", "correct": false, "picked": other }));
      }
      rows
    },
  };
  Some(rows)
}

// ---------------------------------------------------------------------------
// Progress records
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct LessonProg {
  done: bool,
  active_ms: i64,
  blocks_done: Vec<String>,
  done_ms: Option<i64>,
}

#[derive(Default, Clone)]
struct Prog {
  lessons: BTreeMap<String, LessonProg>,
  kc: Map<String, Value>,
  position: Option<(String, Option<String>)>,
  quiz_active_ms: i64,
}

impl Prog {
  fn parse(text: &str) -> Prog {
    let v: Value = serde_json::from_str(text).unwrap_or(Value::Null);
    let mut p = Prog::default();
    if let Some(lessons) = v.get("lessons").and_then(|x| x.as_object()) {
      for (id, l) in lessons {
        p.lessons.insert(
          id.clone(),
          LessonProg {
            done: flag(l, "done", false),
            active_ms: num(l, "active_ms", 0).max(0),
            blocks_done: list(l, "blocks_done")
              .iter()
              .filter_map(|b| b.as_str().map(|x| x.to_string()))
              .collect(),
            done_ms: l.get("done_ms").and_then(|x| x.as_i64()),
          },
        );
      }
    }
    if let Some(kc) = v.get("kc").and_then(|x| x.as_object()) {
      p.kc = kc.clone();
    }
    if let Some(pos) = v.get("position") {
      let lesson_id = txt(pos, "lesson_id");
      if !lesson_id.is_empty() {
        let block_id = pos
          .get("block_id")
          .and_then(|x| x.as_str())
          .map(|x| x.to_string());
        p.position = Some((lesson_id.to_string(), block_id));
      }
    }
    p.quiz_active_ms = num(&v, "quiz_active_ms", 0).max(0);
    p
  }

  fn to_json(&self) -> Value {
    let mut lessons = Map::new();
    for (id, l) in &self.lessons {
      lessons.insert(
        id.clone(),
        json!({
          "done": l.done,
          "active_ms": l.active_ms,
          "blocks_done": l.blocks_done,
          "done_ms": l.done_ms,
        }),
      );
    }
    let position = match &self.position {
      Some((lesson_id, block_id)) => json!({ "lesson_id": lesson_id, "block_id": block_id }),
      None => Value::Null,
    };
    let mut out = Map::new();
    out.insert("lessons".to_string(), Value::Object(lessons));
    out.insert("kc".to_string(), Value::Object(self.kc.clone()));
    out.insert("position".to_string(), position);
    out.insert("quiz_active_ms".to_string(), json!(self.quiz_active_ms));
    Value::Object(out)
  }

  fn lesson_done(&self, lesson_id: &str) -> bool {
    self.lessons.get(lesson_id).map(|l| l.done).unwrap_or(false)
  }
}

/// One person's record for one course ("started" in the UI).
struct Enr {
  version: i32,
  status: String,
  prog: Prog,
  score: Option<i32>,
  attempts: i32,
  time_ms: i64,
  started_ms: i64,
  last_ms: i64,
  completed_ms: Option<i64>,
  ack_ms: Option<i64>,
  awaiting_ack: bool,
  percent: i32,
  quiz_passed: bool,
}

type EnrRow = (
  i32,
  String,
  String,
  Option<i32>,
  i32,
  i64,
  i64,
  i64,
  Option<i64>,
  Option<i64>,
  bool,
  i32,
  bool,
);

impl Enr {
  fn from_row(r: EnrRow) -> Enr {
    Enr {
      version: r.0,
      status: r.1,
      prog: Prog::parse(&r.2),
      score: r.3,
      attempts: r.4,
      time_ms: r.5,
      started_ms: r.6,
      last_ms: r.7,
      completed_ms: r.8,
      ack_ms: r.9,
      awaiting_ack: r.10,
      percent: r.11,
      quiz_passed: r.12,
    }
  }

  fn my_json(&self) -> Value {
    json!({
      "status": self.status,
      "percent": self.percent,
      "score": self.score,
      "attempts": self.attempts,
      "time_ms": self.time_ms,
      "started_ms": self.started_ms,
      "last_ms": self.last_ms,
      "completed_ms": self.completed_ms,
      "ack_ms": self.ack_ms,
      "awaiting_ack": self.awaiting_ack,
      "version": self.version,
    })
  }

  fn full_json(&self) -> Value {
    let mut v = self.my_json();
    if let Some(o) = v.as_object_mut() {
      o.insert("progress".to_string(), self.prog.to_json());
    }
    v
  }

  fn finished(&self) -> bool {
    self.status == "completed" || self.status == "passed"
  }
}

/// Works out status, awaiting_ack, completed_ms and percent after a change.
fn refresh(e: &mut Enr, st: &Settings, lessons: &[Value], now: i64) {
  let total = lessons.len() as i64;
  let done = lessons
    .iter()
    .filter(|l| e.prog.lesson_done(txt(l, "id")))
    .count() as i64;
  let lessons_ok = done >= total;
  let quiz_ok = st.quiz_on && e.quiz_passed;
  let met = (!st.lessons_needed || lessons_ok) && (!st.quiz_needed || quiz_ok);
  if e.finished() {
    // Finished records never go back; a completed one may still become passed.
    e.awaiting_ack = false;
    if e.status == "completed" && quiz_ok {
      e.status = "passed".to_string();
    }
    if e.completed_ms.is_none() {
      e.completed_ms = Some(now);
    }
  } else if met {
    if st.require_ack && e.ack_ms.is_none() {
      e.awaiting_ack = true;
      e.status = "in_progress".to_string();
    } else {
      e.awaiting_ack = false;
      e.status = if quiz_ok { "passed" } else { "completed" }.to_string();
      if e.completed_ms.is_none() {
        e.completed_ms = Some(now);
      }
    }
  } else {
    e.awaiting_ack = false;
    let out_of_attempts = st.quiz_needed
      && !quiz_ok
      && st.max_attempts > 0
      && i64::from(e.attempts) >= st.max_attempts;
    if out_of_attempts {
      e.status = "failed".to_string();
    } else if e.status != "failed" {
      e.status = "in_progress".to_string();
    }
  }
  let mut units = total;
  let mut got = done.min(total);
  if st.quiz_needed {
    units += 1;
    if quiz_ok {
      got += 1;
    }
  }
  if st.require_ack {
    units += 1;
    if e.ack_ms.is_some() {
      got += 1;
    }
  }
  e.percent = if e.finished() {
    100
  } else if units <= 0 {
    0
  } else {
    i32::try_from(((got * 100) / units).clamp(0, 99)).unwrap_or(0)
  };
}

/// A stored learning event (accepted non-heartbeat updates, starts, quiz
/// submissions, acknowledgements, status changes and resets).
struct Ev {
  kind: String,
  lesson_id: String,
  block_id: String,
  data: Option<String>,
  ts: i64,
}

impl Ev {
  fn new(kind: &str, ts: i64) -> Ev {
    Ev {
      kind: kind.to_string(),
      lesson_id: String::new(),
      block_id: String::new(),
      data: None,
      ts,
    }
  }
}

fn status_change(before: &str, e: &Enr, now: i64) -> Option<Ev> {
  if before == e.status {
    return None;
  }
  let mut ev = Ev::new("status", now);
  ev.data = Some(json!({ "from": before, "to": e.status }).to_string());
  Some(ev)
}

async fn log_event(
  conn: &mut PgConnection,
  ws: Uuid,
  course: Uuid,
  uid: i64,
  ev: Ev,
) -> Result<(), AppError> {
  sqlx::query(
    "INSERT INTO carehub_learning_event \
       (workspace_id, course_id, uid, kind, lesson_id, block_id, data, ts_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)",
  )
  .bind(ws)
  .bind(course)
  .bind(uid)
  .bind(ev.kind)
  .bind(ev.lesson_id)
  .bind(ev.block_id)
  .bind(ev.data)
  .bind(ev.ts)
  .execute(&mut *conn)
  .await?;
  Ok(())
}

/// S3: events stored for this person and course, counted up to the cap.
async fn events_stored(conn: &mut PgConnection, course: Uuid, uid: i64) -> Result<i64, AppError> {
  let (n,): (i64,) = sqlx::query_as(
    "SELECT COUNT(*) FROM (SELECT 1 FROM carehub_learning_event \
       WHERE course_id = $1 AND uid = $2 LIMIT $3) AS capped",
  )
  .bind(course)
  .bind(uid)
  .bind(MAX_STORED_EVENTS)
  .fetch_one(&mut *conn)
  .await?;
  Ok(n)
}

/// S3: the same event (kind, lesson, block and data, its time aside) is
/// already stored for this person and course.
async fn event_seen(
  conn: &mut PgConnection,
  course: Uuid,
  uid: i64,
  ev: &Ev,
) -> Result<bool, AppError> {
  let (seen,): (bool,) = sqlx::query_as(
    "SELECT EXISTS (SELECT 1 FROM carehub_learning_event \
       WHERE course_id = $1 AND uid = $2 AND kind = $3 AND lesson_id = $4 AND block_id = $5 \
         AND (CASE WHEN jsonb_typeof(data) = 'object' THEN data - 'ms' ELSE data END) \
           IS NOT DISTINCT FROM $6::jsonb)",
  )
  .bind(course)
  .bind(uid)
  .bind(ev.kind.as_str())
  .bind(ev.lesson_id.as_str())
  .bind(ev.block_id.as_str())
  .bind(without_time(ev.data.as_deref()))
  .fetch_one(&mut *conn)
  .await?;
  Ok(seen)
}

/// Event data without its own time ("ms", the knowledge-check answer time), so
/// repeats can be spotted (S3).
fn without_time(data: Option<&str>) -> Option<String> {
  let raw = data?;
  match serde_json::from_str::<Value>(raw) {
    Ok(Value::Object(mut m)) => {
      m.remove("ms");
      Some(Value::Object(m).to_string())
    },
    _ => Some(raw.to_string()),
  }
}

struct Head {
  status: String,
  published_version: Option<i32>,
}

impl Head {
  /// The latest published version, when learners may take the course.
  fn live_version(&self) -> Option<i32> {
    if self.status == "published" {
      self.published_version
    } else {
      None
    }
  }
}

async fn head_of(
  conn: &mut PgConnection,
  ws: Uuid,
  course: Uuid,
) -> Result<Option<Head>, AppError> {
  let row: Option<(String, Option<i32>)> = sqlx::query_as(
    "SELECT status, published_version FROM carehub_course WHERE id = $1 AND workspace_id = $2",
  )
  .bind(course)
  .bind(ws)
  .fetch_optional(&mut *conn)
  .await?;
  Ok(row.map(|r| Head {
    status: r.0,
    published_version: r.1,
  }))
}

async fn content_of(
  conn: &mut PgConnection,
  course: Uuid,
  version: i32,
) -> Result<Option<Value>, AppError> {
  let row: Option<(String,)> = sqlx::query_as(
    "SELECT content::text FROM carehub_course_version WHERE course_id = $1 AND version = $2",
  )
  .bind(course)
  .bind(version)
  .fetch_optional(&mut *conn)
  .await?;
  Ok(
    row
      .and_then(|r| serde_json::from_str::<Value>(&r.0).ok())
      .filter(|v| v.is_object()),
  )
}

async fn enr_of(
  conn: &mut PgConnection,
  course: Uuid,
  uid: i64,
  lock: bool,
) -> Result<Option<Enr>, AppError> {
  let sql = if lock {
    "SELECT version, status, progress::text, score, attempts, time_ms, started_ms, last_ms, \
       completed_ms, ack_ms, awaiting_ack, percent, quiz_passed \
     FROM carehub_enrollment WHERE course_id = $1 AND uid = $2 FOR UPDATE"
  } else {
    "SELECT version, status, progress::text, score, attempts, time_ms, started_ms, last_ms, \
       completed_ms, ack_ms, awaiting_ack, percent, quiz_passed \
     FROM carehub_enrollment WHERE course_id = $1 AND uid = $2"
  };
  let row: Option<EnrRow> = sqlx::query_as(sql)
    .bind(course)
    .bind(uid)
    .fetch_optional(&mut *conn)
    .await?;
  Ok(row.map(Enr::from_row))
}

async fn save_enr(
  conn: &mut PgConnection,
  course: Uuid,
  uid: i64,
  e: &Enr,
) -> Result<(), AppError> {
  sqlx::query(
    "UPDATE carehub_enrollment SET status = $3, progress = $4::jsonb, score = $5, attempts = $6, \
       time_ms = $7, last_ms = $8, completed_ms = $9, ack_ms = $10, awaiting_ack = $11, \
       percent = $12, quiz_passed = $13 \
     WHERE course_id = $1 AND uid = $2",
  )
  .bind(course)
  .bind(uid)
  .bind(e.status.clone())
  .bind(e.prog.to_json().to_string())
  .bind(e.score)
  .bind(e.attempts)
  .bind(e.time_ms)
  .bind(e.last_ms)
  .bind(e.completed_ms)
  .bind(e.ack_ms)
  .bind(e.awaiting_ack)
  .bind(e.percent)
  .bind(e.quiz_passed)
  .execute(&mut *conn)
  .await?;
  Ok(())
}

/// The caller's record for a course, locked for update (use inside a
/// transaction), plus the content of the version it is pinned to. With
/// `start`, someone who hasn't started yet is started on the latest
/// published version. Archived or unpublished courses aren't available.
async fn open_record(
  conn: &mut PgConnection,
  c: &Ctx,
  course: Uuid,
  start: bool,
) -> Result<(Enr, Value), AppError> {
  let head = head_of(&mut *conn, c.ws, course).await?.ok_or_else(gone)?;
  let latest = head.live_version().ok_or_else(gone)?;
  let enr = match enr_of(&mut *conn, course, c.uid(), true).await? {
    Some(e) => e,
    None => {
      if !start {
        return Err(bad(NOT_STARTED_MSG));
      }
      let now = now_ms();
      let done = sqlx::query(
        "INSERT INTO carehub_enrollment (course_id, uid, version, status, progress, started_ms, last_ms) \
         VALUES ($1, $2, $3, 'in_progress', '{}'::jsonb, $4, $4) \
         ON CONFLICT (course_id, uid) DO NOTHING",
      )
      .bind(course)
      .bind(c.uid())
      .bind(latest)
      .bind(now)
      .execute(&mut *conn)
      .await?;
      if done.rows_affected() > 0 {
        log_event(&mut *conn, c.ws, course, c.uid(), Ev::new("start", now)).await?;
      }
      enr_of(&mut *conn, course, c.uid(), true)
        .await?
        .ok_or_else(gone)?
    },
  };
  let content = content_of(&mut *conn, course, enr.version)
    .await?
    .ok_or_else(gone)?;
  Ok((enr, content))
}

// ---------------------------------------------------------------------------
// Quiz helpers
// ---------------------------------------------------------------------------

fn all_lessons_done(lessons: &[Value], prog: &Prog) -> bool {
  lessons.iter().all(|l| prog.lesson_done(txt(l, "id")))
}

fn quiz_lock(
  st: &Settings,
  lessons: &[Value],
  prog: &Prog,
  used: i32,
  passed: bool,
) -> Option<&'static str> {
  if passed {
    return Some(QUIZ_PASSED_MSG);
  }
  if st.max_attempts > 0 && i64::from(used) >= st.max_attempts {
    return Some(QUIZ_NO_ATTEMPTS_MSG);
  }
  if st.completion == "all_lessons_and_quiz" && st.sequential && !all_lessons_done(lessons, prog) {
    return Some(QUIZ_LESSONS_MSG);
  }
  None
}

fn attempts_left(st: &Settings, used: i32) -> Option<i64> {
  if st.max_attempts > 0 {
    Some((st.max_attempts - i64::from(used)).max(0))
  } else {
    None
  }
}

/// The questions for one attempt: a deterministic pool (when pool_size is
/// smaller than the question count) and, if enabled, a deterministic order.
/// Seeded by course, person and attempt number, so Play and grading agree.
fn pick_questions<'a>(
  content: &'a Value,
  st: &Settings,
  course: Uuid,
  uid: i64,
  attempt: i32,
) -> Vec<&'a Value> {
  let all: &'a [Value] = match content.get("quiz") {
    Some(q) => list(q, "questions"),
    None => &[],
  };
  // qbank3: the pool is taken from the quiz's own questions; questions copied
  // from question banks are drawn on top (the same as before without them).
  let mut idx: Vec<usize> = all
    .iter()
    .enumerate()
    .filter(|(_, q)| !qbank::from_bank(q))
    .map(|(i, _)| i)
    .collect();
  if st.pool_size > 0 && st.pool_size < idx.len() {
    shuffle(&mut idx, &format!("{}:{}:{}:pool", course, uid, attempt));
    idx.truncate(st.pool_size);
    idx.sort_unstable();
  }
  idx.extend(qbank::drawn(all, content, course, uid, attempt));
  if st.shuffle {
    shuffle(&mut idx, &format!("{}:{}:{}:order", course, uid, attempt));
  }
  idx.iter().filter_map(|i| all.get(*i)).collect()
}

/// A matching question's right sides as learners get them: text tokens, sorted
/// A-Z by text (S2: no shuffle a browser could undo).
fn sorted_rights(q: &Value, course: Uuid) -> Vec<Value> {
  let pairs = list(q, "pairs");
  let texts = right_texts(q);
  let mut rights: Vec<(String, &str, String, &str)> = text_tokens(course, &texts)
    .into_iter()
    .zip(texts.iter().zip(pairs.iter()))
    .map(|(token, (text, p))| (text.to_lowercase(), *text, token, txt(p, "right")))
    .collect();
  rights.sort();
  rights
    .into_iter()
    .map(|(_, _, token, shown)| json!({ "token": token, "text": shown }))
    .collect()
}

/// A final-quiz question without answers, feedback or explanations.
fn public_question(q: &Value, course: Uuid) -> Value {
  let kind = kind_of(q);
  let classic = matches!(kind, "single" | "multiple" | "true_false" | "matching");
  let options: Vec<Value> = if classic {
    list(q, "options")
      .iter()
      .map(|o| json!({ "id": txt(o, "id"), "text": txt(o, "text") }))
      .collect()
  } else {
    Vec::new()
  };
  let pairs: Vec<Value> = if classic {
    list(q, "pairs")
      .iter()
      .map(|p| json!({ "id": txt(p, "id"), "left": txt(p, "left") }))
      .collect()
  } else {
    Vec::new()
  };
  let rights: Vec<Value> = if kind == "matching" {
    sorted_rights(q, course)
  } else {
    Vec::new()
  };
  let mut out = json!({
    "id": txt(q, "id"),
    "kind": kind,
    "prompt": txt(q, "prompt"),
    "points": points_of(q),
    "options": options,
    "pairs": pairs,
    "rights": rights,
  });
  if let Some(o) = out.as_object_mut() {
    match kind {
      "fill_blank" => {
        o.insert("blank".to_string(), json!(true));
        // qbank3: so the review can say "Capital letters count."
        o.insert("case_sensitive".to_string(), json!(flag(q, "case_sensitive", false)));
      },
      "ordering" => {
        o.insert("items".to_string(), Value::Array(shuffled_items(q, course)));
      },
      "hotspot" => {
        o.insert("image".to_string(), public_image(q));
      },
      _ => {},
    }
  }
  out
}

/// All-or-nothing grading of one answer.
fn is_correct(q: &Value, answer: &Value, course: Uuid) -> bool {
  match kind_of(q) {
    "multiple" => {
      let want: BTreeSet<&str> = list(q, "options")
        .iter()
        .filter(|o| flag(o, "correct", false))
        .map(|o| txt(o, "id"))
        .collect();
      let got: BTreeSet<&str> = match answer.as_array() {
        Some(a) => a.iter().filter_map(|x| x.as_str()).collect(),
        None => BTreeSet::new(),
      };
      !want.is_empty() && want == got
    },
    "matching" => {
      let pairs = list(q, "pairs");
      let chosen = match answer.as_object() {
        Some(o) => o,
        None => return false,
      };
      if pairs.is_empty() {
        return false;
      }
      // S2: each token maps back to its text; right = the pair's own right side.
      let texts = right_texts(q);
      let by_token = text_by_token(course, &texts);
      pairs.iter().all(|p| {
        let want = txt(p, "right").trim();
        chosen
          .get(txt(p, "id"))
          .and_then(|t| t.as_str())
          .and_then(|t| by_token.get(t))
          .map(|got| *got == want)
          .unwrap_or(false)
      })
    },
    "fill_blank" => blank_correct(q, answer),
    "ordering" => ordering_correct(q, answer, course),
    "hotspot" => hotspot_hit(q, answer),
    _ => {
      let pick = answer.as_str().unwrap_or("");
      !pick.is_empty()
        && list(q, "options")
          .iter()
          .any(|o| txt(o, "id") == pick && flag(o, "correct", false))
    },
  }
}

/// The right answer in the same shape the browser sends answers.
fn correct_answer(q: &Value, course: Uuid) -> Value {
  match kind_of(q) {
    "multiple" => Value::Array(
      list(q, "options")
        .iter()
        .filter(|o| flag(o, "correct", false))
        .map(|o| json!(txt(o, "id")))
        .collect(),
    ),
    "matching" => {
      // S2: each pair's own right side, as its text token.
      let texts = right_texts(q);
      let mut m = Map::new();
      for (p, token) in list(q, "pairs").iter().zip(text_tokens(course, &texts)) {
        m.insert(txt(p, "id").to_string(), json!(token));
      }
      Value::Object(m)
    },
    "fill_blank" => Value::Array(blank_answers(q).into_iter().map(|a| json!(a)).collect()),
    "ordering" => Value::Array(
      text_tokens(course, &item_texts(q))
        .into_iter()
        .map(|token| json!(token))
        .collect(),
    ),
    "hotspot" => Value::Array(
      list(q, "spots")
        .iter()
        .filter_map(spot_of)
        .map(|(x, y, w, h)| json!({ "x": x, "y": y, "w": w, "h": h }))
        .collect(),
    ),
    _ => list(q, "options")
      .iter()
      .find(|o| flag(o, "correct", false))
      .map(|o| json!(txt(o, "id")))
      .unwrap_or(Value::Null),
  }
}

/// Why `lesson_complete` can't be accepted yet, if anything.
fn completion_block(st: &Settings, lessons: &[Value], prog: &Prog, idx: usize) -> Option<String> {
  let lesson = match lessons.get(idx) {
    Some(l) => l,
    None => return Some(UNKNOWN_LESSON_MSG.to_string()),
  };
  if st.sequential {
    for earlier in lessons.iter().take(idx) {
      if !prog.lesson_done(txt(earlier, "id")) {
        let title = txt(earlier, "title").trim();
        return Some(if title.is_empty() {
          "Finish the earlier lessons first.".to_string()
        } else {
          format!("Finish \"{}\" first.", title)
        });
      }
    }
  }
  let lp = prog.lessons.get(txt(lesson, "id"));
  let need = required_seconds(st, lesson);
  let active = lp.map(|p| p.active_ms).unwrap_or(0);
  if need > 0 && active < need.saturating_mul(1000).saturating_sub(2000) {
    return Some(format!(
      "Spend at least {} on this lesson first.",
      clock(need)
    ));
  }
  let done_blocks: &[String] = lp.map(|p| p.blocks_done.as_slice()).unwrap_or(&[]);
  for b in list(lesson, "blocks") {
    if txt(b, "type") == "continue" && flag(b, "require_complete", false) {
      let bid = txt(b, "id");
      if !done_blocks.iter().any(|d| d == bid) {
        return Some(CONTINUE_MSG.to_string());
      }
    }
  }
  None
}

// ---------------------------------------------------------------------------
// Learner endpoints
// ---------------------------------------------------------------------------

pub(crate) type CardRow = (
  Uuid,
  String,
  i64,
  i32,
  String,
  String,
  String,
  String,
  i32,
  i32,
  bool,
  bool,
);

pub(crate) type MyRow = (
  Uuid,
  String,
  i32,
  Option<i32>,
  i32,
  i64,
  i64,
  i64,
  Option<i64>,
  Option<i64>,
  bool,
  i32,
);

fn my_row_json(r: &MyRow) -> Value {
  json!({
    "status": r.1,
    "percent": r.2,
    "score": r.3,
    "attempts": r.4,
    "time_ms": r.5,
    "started_ms": r.6,
    "last_ms": r.7,
    "completed_ms": r.8,
    "ack_ms": r.9,
    "awaiting_ack": r.10,
    "version": r.11,
  })
}

pub(crate) fn card_json(r: &CardRow, my: Option<&MyRow>) -> Value {
  json!({
    "id": r.0.to_string(),
    "title": r.4,
    "summary": r.5,
    "cover": r.6,
    "preset": r.7,
    "version": r.3,
    "lessons": r.8,
    "est_minutes": r.9,
    "quiz": r.10,
    "archived": r.1 == "archived",
    "updated_ms": r.2,
    "my": my.map(my_row_json),
  })
}

/// GET /catalog (mine = false) and GET /my (mine = true).
async fn course_cards(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  mine: bool,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  // Card details come from the version the caller is on (or the latest one).
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
       AND (e.uid IS NOT NULL OR c.status = 'published')",
  )
  .bind(c.ws)
  .bind(c.uid())
  .fetch_all(&state.pg_pool)
  .await?;
  let records: Vec<MyRow> = sqlx::query_as(
    "SELECT e.course_id, e.status, e.percent, e.score, e.attempts, e.time_ms, e.started_ms, \
       e.last_ms, e.completed_ms, e.ack_ms, e.awaiting_ack, e.version \
     FROM carehub_enrollment e JOIN carehub_course c ON c.id = e.course_id \
     WHERE c.workspace_id = $1 AND e.uid = $2",
  )
  .bind(c.ws)
  .bind(c.uid())
  .fetch_all(&state.pg_pool)
  .await?;
  let by_course: HashMap<Uuid, &MyRow> = records.iter().map(|r| (r.0, r)).collect();
  let mut picked: Vec<(&CardRow, Option<&MyRow>)> = Vec::new();
  for card in &cards {
    let my = by_course.get(&card.0).copied();
    let archived = card.1 == "archived";
    match my {
      // Archived courses stay visible only to people who finished them.
      Some(m) => {
        if archived && m.1 == "in_progress" {
          continue;
        }
      },
      None => {
        if mine || archived || !card.11 {
          continue;
        }
      },
    }
    picked.push((card, my));
  }
  if mine {
    picked.sort_by(|a, b| {
      let la = a.1.map(|m| m.7).unwrap_or(0);
      let lb = b.1.map(|m| m.7).unwrap_or(0);
      lb.cmp(&la)
    });
  } else {
    picked.sort_by(|a, b| a.0 .4.to_lowercase().cmp(&b.0 .4.to_lowercase()));
  }
  let mut courses: Vec<Value> = picked.iter().map(|(card, my)| card_json(card, *my)).collect();
  // P2 recert
  carehub_learning_assign::add_card_expiry(&state.pg_pool, &mut courses).await;
  Ok(json!({ "courses": courses }))
}

/// GET /courses/{id}/play
async fn play(state: &AppState, user: &UserUuid, ws: &str, id: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let uid = c.uid();
  let mut conn = state.pg_pool.acquire().await?;
  let head = head_of(&mut conn, c.ws, course).await?.ok_or_else(gone)?;
  let latest = head.live_version().ok_or_else(gone)?;
  let enr = enr_of(&mut conn, course, uid, false).await?;
  let version = enr.as_ref().map(|e| e.version).unwrap_or(latest);
  let content = content_of(&mut conn, course, version)
    .await?
    .ok_or_else(gone)?;
  drop(conn);
  let st = settings_of(&content);
  let used = enr.as_ref().map(|e| e.attempts).unwrap_or(0);
  let passed = enr.as_ref().map(|e| e.quiz_passed).unwrap_or(false);
  let blank = Prog::default();
  let prog = enr.as_ref().map(|e| &e.prog).unwrap_or(&blank);
  let locked = if st.quiz_on {
    quiz_lock(&st, list(&content, "lessons"), prog, used, passed)
  } else {
    None
  };
  let questions: Vec<Value> = if st.quiz_on {
    pick_questions(&content, &st, course, uid, used.saturating_add(1))
      .into_iter()
      .map(|q| public_question(q, course))
      .collect()
  } else {
    Vec::new()
  };
  let mut quiz = match content.get("quiz") {
    Some(q) if q.is_object() => q.clone(),
    _ => json!({ "enabled": false }),
  };
  if let Some(o) = quiz.as_object_mut() {
    o.insert("questions".to_string(), Value::Array(questions));
    o.insert("attempts_used".to_string(), json!(used));
    o.insert("attempts_left".to_string(), json!(attempts_left(&st, used)));
    o.insert("locked_reason".to_string(), json!(locked));
    // qbank3: learners don't get the bank rows or the draft checksum.
    o.remove("draws");
    o.remove("draft_md5");
  }
  let mut out = content;
  if let Some(o) = out.as_object_mut() {
    o.insert("quiz".to_string(), quiz);
    o.insert("id".to_string(), json!(course.to_string()));
    o.insert("version".to_string(), json!(version));
  }
  let mut res = Map::new();
  res.insert("course".to_string(), out);
  // P2 recert
  let mut enrollment = enr.as_ref().map(|e| e.full_json()).unwrap_or(Value::Null);
  carehub_learning_assign::add_row_expiry(
    &state.pg_pool,
    std::slice::from_mut(&mut enrollment),
    Some(course),
  )
  .await;
  res.insert("enrollment".to_string(), enrollment);
  // qbank3: the caller's own survey answers ({} if none).
  let surveys = qbank::my_surveys(&state.pg_pool, course, uid).await;
  res.insert("surveys".to_string(), surveys);
  Ok(Value::Object(res))
}

/// POST /courses/{id}/start
async fn start(state: &AppState, user: &UserUuid, ws: &str, id: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let mut tx = state.pg_pool.begin().await?;
  let (enr, _content) = open_record(&mut tx, &c, course, true).await?;
  tx.commit().await?;
  Ok(json!({ "enrollment": enr.full_json() }))
}

/// POST /courses/{id}/progress
async fn progress(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let body = body_json(body)?;
  let events = list(&body, "events");
  let mut tx = state.pg_pool.begin().await?;
  let (mut enr, content) = open_record(&mut tx, &c, course, true).await?;
  let st = settings_of(&content);
  let lessons = list(&content, "lessons");
  let now = now_ms();
  let before = enr.status.clone();
  // Active time can't add up to more than the real time since the last update.
  let mut budget = now
    .saturating_sub(enr.last_ms)
    .max(0)
    .saturating_add(HEARTBEAT_CAP_MS);
  // S1: and all accepted time together can't run ahead of the real time since
  // the start (+60 s), so two open tabs don't count twice. (open_record sets
  // started_ms when it auto-starts the course.)
  let wall = now
    .saturating_sub(enr.started_ms)
    .max(0)
    .saturating_add(HEARTBEAT_CAP_MS);
  budget = budget.min(wall.saturating_sub(enr.time_ms).max(0));
  let mut rejected: Vec<Value> = Vec::new();
  let mut stored: Vec<Ev> = Vec::new();
  for (i, ev) in events.iter().enumerate() {
    if i >= MAX_EVENTS {
      rejected.push(json!({ "i": i, "reason": TOO_MANY_MSG }));
      continue;
    }
    let kind = txt(ev, "kind");
    let lesson_id = txt(ev, "lesson_id");
    let block_id = txt(ev, "block_id");
    let ts = match num(ev, "ts_ms", now) {
      t if t <= 0 || t > now => now,
      t => t,
    };
    let found = lessons
      .iter()
      .enumerate()
      .find(|(_, l)| !lesson_id.is_empty() && txt(l, "id") == lesson_id);
    if kind == "heartbeat" {
      let ms = num(ev, "active_ms", 0)
        .clamp(0, HEARTBEAT_CAP_MS)
        .min(budget)
        .max(0);
      if lesson_id == QUIZ_LESSON {
        enr.prog.quiz_active_ms = enr.prog.quiz_active_ms.saturating_add(ms);
      } else if found.is_some() {
        let lp = enr.prog.lessons.entry(lesson_id.to_string()).or_default();
        lp.active_ms = lp.active_ms.saturating_add(ms);
      } else {
        rejected.push(json!({ "i": i, "reason": UNKNOWN_LESSON_MSG }));
        continue;
      }
      budget = budget.saturating_sub(ms);
      enr.time_ms = enr.time_ms.saturating_add(ms);
      continue;
    }
    if lesson_id == QUIZ_LESSON && kind == "lesson_open" {
      let mut opened = Ev::new(kind, ts);
      opened.lesson_id = QUIZ_LESSON.to_string();
      stored.push(opened);
      continue;
    }
    let (idx, lesson) = match found {
      Some(f) => f,
      None => {
        rejected.push(json!({ "i": i, "reason": UNKNOWN_LESSON_MSG }));
        continue;
      },
    };
    let mut data: Option<String> = None;
    match kind {
      "lesson_open" => {},
      "block_complete" => {
        if !has_block(lesson, block_id) {
          rejected.push(json!({ "i": i, "reason": UNKNOWN_BLOCK_MSG }));
          continue;
        }
        let lp = enr.prog.lessons.entry(lesson_id.to_string()).or_default();
        if !lp.blocks_done.iter().any(|b| b == block_id) {
          lp.blocks_done.push(block_id.to_string());
        }
      },
      "kc_answer" => {
        if !has_block(lesson, block_id) {
          rejected.push(json!({ "i": i, "reason": UNKNOWN_BLOCK_MSG }));
          continue;
        }
        let given = ev.get("data");
        let mut answer = given
          .and_then(|d| d.get("answer"))
          .cloned()
          .unwrap_or(Value::Null);
        if answer.to_string().len() > MAX_KC_BYTES {
          answer = Value::Null;
        }
        let correct = given
          .and_then(|d| d.get("correct"))
          .and_then(|x| x.as_bool())
          .unwrap_or(false);
        let entry = json!({ "answer": answer, "correct": correct, "ms": ts });
        data = Some(entry.to_string());
        enr.prog.kc.insert(block_id.to_string(), entry);
      },
      // qbank3: survey answers are saved in their own table and never go to
      // the event log; a saved set also counts the block as done.
      "survey_answer" => {
        let saved = qbank::save_survey(
          &mut tx,
          &state.pg_pool,
          c.ws,
          course,
          c.uid(),
          enr.version,
          lesson,
          block_id,
          ev.get("data"),
          now,
        )
        .await?;
        if let Some(reason) = saved {
          rejected.push(json!({ "i": i, "reason": reason }));
        } else {
          let lp = enr.prog.lessons.entry(lesson_id.to_string()).or_default();
          if !lp.blocks_done.iter().any(|b| b == block_id) {
            lp.blocks_done.push(block_id.to_string());
          }
        }
        continue;
      },
      "lesson_complete" => {
        if enr.prog.lesson_done(lesson_id) {
          continue;
        }
        if let Some(reason) = completion_block(&st, lessons, &enr.prog, idx) {
          rejected.push(json!({ "i": i, "reason": reason }));
          continue;
        }
        let lp = enr.prog.lessons.entry(lesson_id.to_string()).or_default();
        lp.done = true;
        lp.done_ms = Some(ts);
      },
      _ => {
        rejected.push(json!({ "i": i, "reason": UNKNOWN_EVENT_MSG }));
        continue;
      },
    }
    stored.push(Ev {
      kind: kind.to_string(),
      lesson_id: lesson_id.to_string(),
      block_id: block_id.chars().take(200).collect(),
      data,
      ts,
    });
  }
  if let Some(pos) = body.get("position") {
    let lesson_id = txt(pos, "lesson_id");
    let known = !lesson_id.is_empty()
      && (lesson_id == QUIZ_LESSON || lessons.iter().any(|l| txt(l, "id") == lesson_id));
    if known {
      let block_id = pos
        .get("block_id")
        .and_then(|x| x.as_str())
        .filter(|b| !b.is_empty())
        .map(|b| b.chars().take(200).collect::<String>());
      enr.prog.position = Some((lesson_id.to_string(), block_id));
    }
  }
  enr.last_ms = enr.last_ms.max(now);
  refresh(&mut enr, &st, lessons, now);
  save_enr(&mut tx, course, c.uid(), &enr).await?;
  if let Some(ev) = status_change(&before, &enr, now) {
    stored.push(ev);
  }
  // S3: skip a kc_answer / block_complete / lesson_open that is already stored
  // (same kind, lesson, block and data; time aside), and store nothing past
  // MAX_STORED_EVENTS for this person and course. Progress above still counts.
  let mut logged = if stored.is_empty() {
    0
  } else {
    events_stored(&mut tx, course, c.uid()).await?
  };
  for ev in stored {
    if logged >= MAX_STORED_EVENTS {
      break;
    }
    let repeatable = matches!(ev.kind.as_str(), "kc_answer" | "block_complete" | "lesson_open");
    if repeatable && event_seen(&mut tx, course, c.uid(), &ev).await? {
      continue;
    }
    log_event(&mut tx, c.ws, course, c.uid(), ev).await?;
    logged = logged.saturating_add(1);
  }
  tx.commit().await?;
  Ok(json!({ "enrollment": enr.full_json(), "rejected": rejected }))
}

/// POST /courses/{id}/quiz
async fn submit_quiz(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let uid = c.uid();
  let body = body_json(body)?;
  let no_answers = Map::new();
  let answers: &Map<String, Value> = body
    .get("answers")
    .and_then(|a| a.as_object())
    .unwrap_or(&no_answers);
  let mut tx = state.pg_pool.begin().await?;
  let (mut enr, content) = open_record(&mut tx, &c, course, true).await?;
  let st = settings_of(&content);
  if !st.quiz_on {
    return Err(bad(NO_QUIZ_MSG));
  }
  let lessons = list(&content, "lessons");
  if let Some(reason) = quiz_lock(&st, lessons, &enr.prog, enr.attempts, enr.quiz_passed) {
    return Err(bad(reason));
  }
  let before = enr.status.clone();
  let attempt = enr.attempts.saturating_add(1);
  let questions = pick_questions(&content, &st, course, uid, attempt);
  let null = Value::Null;
  let mut total: i64 = 0;
  let mut earned: i64 = 0;
  let mut graded: Vec<(&Value, bool, i64)> = Vec::new();
  // S3: only this attempt's answers are stored, each at most MAX_KC_BYTES
  // (bigger ones are stored as null), like knowledge-check answers.
  let mut stored_answers = Map::new();
  for q in questions {
    let pts = points_of(q);
    let ok = is_correct(q, answers.get(txt(q, "id")).unwrap_or(&null), course);
    if let Some(a) = answers.get(txt(q, "id")) {
      let keep = if a.to_string().len() > MAX_KC_BYTES {
        Value::Null
      } else {
        a.clone()
      };
      stored_answers.insert(txt(q, "id").to_string(), keep);
    }
    total = total.saturating_add(pts);
    if ok {
      earned = earned.saturating_add(pts);
    }
    graded.push((q, ok, pts));
  }
  let score: i64 = if total > 0 {
    (earned * 200 + total) / (total * 2)
  } else {
    0
  };
  let score32 = i32::try_from(score.clamp(0, 100)).unwrap_or(0);
  let passed = score >= st.pass_mark;
  enr.attempts = attempt;
  if passed {
    enr.quiz_passed = true;
  }
  enr.score = Some(enr.score.map_or(score32, |best| best.max(score32)));
  let left = attempts_left(&st, enr.attempts);
  let reveal = match st.show_answers.as_str() {
    "after_submit" => true,
    "never" => false,
    _ => passed || left == Some(0),
  };
  let mut results: Vec<Value> = Vec::new();
  let mut kept: Vec<Value> = Vec::new();
  for (q, ok, pts) in &graded {
    let got = if *ok { *pts } else { 0 };
    let mut r = Map::new();
    r.insert("question_id".to_string(), json!(txt(q, "id")));
    r.insert("correct".to_string(), json!(*ok));
    r.insert("earned".to_string(), json!(got));
    r.insert("points".to_string(), json!(*pts));
    let feedback = if *ok { "feedback_correct" } else { "feedback_incorrect" };
    r.insert("feedback".to_string(), json!(txt(q, feedback)));
    if reveal {
      r.insert("correct_answer".to_string(), correct_answer(q, course));
      r.insert("explanation".to_string(), json!(txt(q, "explanation")));
      // qbank3: the picked options' own feedback.
      let given = answers.get(txt(q, "id")).unwrap_or(&null);
      if let Some(picked) = qbank::option_feedback(q, given) {
        r.insert("option_feedback".to_string(), picked);
      }
    }
    results.push(Value::Object(r));
    kept.push(json!({ "question_id": txt(q, "id"), "correct": *ok, "earned": got, "points": *pts }));
  }
  let now = now_ms();
  enr.last_ms = enr.last_ms.max(now);
  refresh(&mut enr, &st, lessons, now);
  save_enr(&mut tx, course, uid, &enr).await?;
  sqlx::query(
    "INSERT INTO carehub_quiz_attempt \
       (course_id, uid, version, attempt, answers, results, score, passed, created_ms) \
     VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9)",
  )
  .bind(course)
  .bind(uid)
  .bind(enr.version)
  .bind(attempt)
  .bind(Value::Object(stored_answers).to_string())
  .bind(Value::Array(kept).to_string())
  .bind(score32)
  .bind(passed)
  .bind(now)
  .execute(&mut *tx)
  .await?;
  let mut ev = Ev::new("quiz_submit", now);
  ev.lesson_id = QUIZ_LESSON.to_string();
  ev.data = Some(json!({ "attempt": attempt, "score": score32, "passed": passed }).to_string());
  log_event(&mut tx, c.ws, course, uid, ev).await?;
  if let Some(ev) = status_change(&before, &enr, now) {
    log_event(&mut tx, c.ws, course, uid, ev).await?;
  }
  tx.commit().await?;
  Ok(json!({
    "score": score32,
    "passed": passed,
    "attempts_used": enr.attempts,
    "attempts_left": left,
    "results": results,
    "enrollment": enr.full_json(),
  }))
}

/// POST /courses/{id}/ack
async fn acknowledge(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let mut tx = state.pg_pool.begin().await?;
  let (mut enr, content) = open_record(&mut tx, &c, course, false).await?;
  if enr.ack_ms.is_none() {
    if !enr.awaiting_ack {
      return Err(bad(ACK_EARLY_MSG));
    }
    let now = now_ms();
    let before = enr.status.clone();
    enr.ack_ms = Some(now);
    enr.last_ms = enr.last_ms.max(now);
    let st = settings_of(&content);
    refresh(&mut enr, &st, list(&content, "lessons"), now);
    save_enr(&mut tx, course, c.uid(), &enr).await?;
    log_event(&mut tx, c.ws, course, c.uid(), Ev::new("ack", now)).await?;
    if let Some(ev) = status_change(&before, &enr, now) {
      log_event(&mut tx, c.ws, course, c.uid(), ev).await?;
    }
  }
  tx.commit().await?;
  Ok(json!({ "enrollment": enr.full_json() }))
}

// ---------------------------------------------------------------------------
// Studio (needs learning_build)
// ---------------------------------------------------------------------------

fn preset_of(raw: &str) -> &'static str {
  match raw {
    "scenario_lab" => "scenario_lab",
    "required_read" => "required_read",
    "certification" => "certification",
    "microlearning" => "microlearning",
    _ => "standard",
  }
}

fn short_id() -> String {
  Uuid::new_v4()
    .to_string()
    .chars()
    .filter(|ch| ch.is_ascii_hexdigit())
    .take(6)
    .collect()
}

/// A new course: one empty lesson, quiz off, settings from the preset.
fn default_content(title: &str, preset: &str) -> Value {
  let (sequential, min_seconds, max_attempts, require_ack, completion) = match preset {
    "scenario_lab" => (true, 0, 0, false, "all_lessons"),
    "required_read" => (true, 30, 0, true, "all_lessons"),
    "certification" => (true, 0, 3, false, "all_lessons_and_quiz"),
    _ => (false, 0, 0, false, "all_lessons"),
  };
  json!({
    "title": clean_title(title),
    "summary": "",
    "cover": "",
    "preset": preset,
    "est_minutes": 0,
    "settings": {
      "sequential": sequential,
      "min_seconds_per_lesson": min_seconds,
      "pass_mark": 80,
      "max_attempts": max_attempts,
      "show_answers": "after_pass",
      "require_ack": require_ack,
      "ack_text": "I have read and understood this course.",
      "completion": completion,
      "catalog": true,
    },
    "lessons": [
      { "id": format!("l_{}", short_id()), "title": "Lesson 1", "min_seconds": 0, "blocks": [] }
    ],
    "quiz": { "enabled": false, "title": "Final quiz", "questions": [], "pool_size": 0, "shuffle": true },
  })
}

/// GET /studio
async fn studio_list(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  // qbank3: a version with bank copies is compared through its draft checksum.
  let rows: Vec<(Uuid, String, String, Option<i32>, i64, String, String, bool, i64, i64)> =
    sqlx::query_as(
      "SELECT c.id, c.title, c.status, c.published_version, c.updated_ms, \
         COALESCE(NULLIF(uu.name, ''), split_part(COALESCE(uu.email, ''), '@', 1), ''), \
         COALESCE(NULLIF(cu.name, ''), split_part(COALESCE(cu.email, ''), '@', 1), ''), \
         COALESCE(c.published_version IS NULL OR v.content IS NULL OR CASE \
           WHEN v.content->'quiz'->>'draft_md5' IS NULL THEN c.draft <> v.content \
           ELSE md5(c.draft::text) <> v.content->'quiz'->>'draft_md5' END, TRUE), \
         (SELECT COUNT(*) FROM carehub_enrollment e WHERE e.course_id = c.id)::BIGINT, \
         (SELECT COUNT(*) FROM carehub_enrollment e \
           WHERE e.course_id = c.id AND e.status IN ('completed', 'passed'))::BIGINT \
       FROM carehub_course c \
       LEFT JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
       LEFT JOIN af_user uu ON uu.uid = c.updated_by \
       LEFT JOIN af_user cu ON cu.uid = c.created_by \
       WHERE c.workspace_id = $1 ORDER BY c.updated_ms DESC LIMIT 1000",
    )
    .bind(c.ws)
    .fetch_all(&state.pg_pool)
    .await?;
  let courses: Vec<Value> = rows
    .iter()
    .map(|r| {
      json!({
        "id": r.0.to_string(),
        "title": r.1,
        "status": r.2,
        "published_version": r.3,
        "updated_ms": r.4,
        "updated_by_name": r.5,
        "created_by_name": r.6,
        "has_unpublished_changes": r.7,
        "started": r.8,
        "finished": r.9,
      })
    })
    .collect();
  Ok(json!({ "courses": courses }))
}

/// POST /studio
async fn studio_create(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let body = body_json(body)?;
  let preset = preset_of(txt(&body, "preset"));
  let given_title = txt(&body, "title").trim().to_string();
  let mut content = match body.get("content") {
    Some(v) if v.is_object() => v.clone(),
    _ => default_content(given_title.as_str(), preset),
  };
  if let Some(o) = content.as_object_mut() {
    let has_title = o
      .get("title")
      .and_then(|t| t.as_str())
      .map(|t| !t.trim().is_empty())
      .unwrap_or(false);
    if !given_title.is_empty() || !has_title {
      o.insert("title".to_string(), json!(clean_title(&given_title)));
    }
    if !o.contains_key("preset") {
      o.insert("preset".to_string(), json!(preset));
    }
  }
  let text = content.to_string();
  if text.len() > MAX_CONTENT_BYTES {
    return Err(bad(TOO_BIG_MSG));
  }
  let id = Uuid::new_v4();
  let now = now_ms();
  sqlx::query(
    "INSERT INTO carehub_course \
       (id, workspace_id, title, status, draft, draft_updated_ms, updated_by, created_by, created_ms, updated_ms) \
     VALUES ($1, $2, $3, 'draft', $4::jsonb, $5, $6, $6, $5, $5)",
  )
  .bind(id)
  .bind(c.ws)
  .bind(clean_title(txt(&content, "title")))
  .bind(text)
  .bind(now)
  .bind(c.uid())
  .execute(&state.pg_pool)
  .await?;
  Ok(json!({ "id": id.to_string() }))
}

/// GET /studio/{id}
async fn studio_get(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  // qbank3: a version with bank copies is compared through its draft checksum.
  let row: Option<(String, String, i64, Option<i32>, bool)> = sqlx::query_as(
    "SELECT c.status, c.draft::text, c.draft_updated_ms, c.published_version, \
       COALESCE(c.published_version IS NULL OR v.content IS NULL OR CASE \
         WHEN v.content->'quiz'->>'draft_md5' IS NULL THEN c.draft <> v.content \
         ELSE md5(c.draft::text) <> v.content->'quiz'->>'draft_md5' END, TRUE) \
     FROM carehub_course c \
     LEFT JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.id = $1 AND c.workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (status, draft_text, updated_ms, published_version, changes) = row.ok_or_else(missing)?;
  let versions: Vec<(i32, i64, String, String)> = sqlx::query_as(
    "SELECT v.version, v.published_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), v.note \
     FROM carehub_course_version v LEFT JOIN af_user u ON u.uid = v.published_by \
     WHERE v.course_id = $1 ORDER BY v.version DESC LIMIT 500",
  )
  .bind(course)
  .fetch_all(&state.pg_pool)
  .await?;
  let draft: Value = serde_json::from_str(&draft_text).unwrap_or_else(|_| json!({}));
  let versions: Vec<Value> = versions
    .iter()
    .map(|v| {
      json!({ "version": v.0, "published_ms": v.1, "published_by_name": v.2, "note": v.3 })
    })
    .collect();
  let mut out = Map::new();
  out.insert("id".to_string(), json!(course.to_string()));
  out.insert("status".to_string(), json!(status));
  out.insert("draft".to_string(), draft);
  out.insert("updated_ms".to_string(), json!(updated_ms));
  out.insert("published_version".to_string(), json!(published_version));
  out.insert("has_unpublished_changes".to_string(), json!(changes));
  out.insert("versions".to_string(), Value::Array(versions));
  Ok(Value::Object(out))
}

/// PUT /studio/{id} (autosave)
async fn studio_save(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let body = body_json(body)?;
  let draft = match body.get("draft") {
    Some(d) if d.is_object() => d,
    _ => return Err(bad(DRAFT_SHAPE_MSG)),
  };
  let text = draft.to_string();
  if text.len() > MAX_CONTENT_BYTES {
    return Err(bad(TOO_BIG_MSG));
  }
  let base: Option<i64> = body
    .get("base_updated_ms")
    .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)));
  let row: Option<(i64,)> = sqlx::query_as(
    "UPDATE carehub_course SET draft = $3::jsonb, title = $4, \
       draft_updated_ms = GREATEST($5, draft_updated_ms + 1), \
       updated_ms = GREATEST($5, draft_updated_ms + 1), updated_by = $6 \
     WHERE id = $1 AND workspace_id = $2 AND ($7::BIGINT IS NULL OR draft_updated_ms = $7) \
     RETURNING draft_updated_ms",
  )
  .bind(course)
  .bind(c.ws)
  .bind(text)
  .bind(clean_title(txt(draft, "title")))
  .bind(now_ms())
  .bind(c.uid())
  .bind(base)
  .fetch_optional(&state.pg_pool)
  .await?;
  if let Some((updated_ms,)) = row {
    return Ok(json!({ "updated_ms": updated_ms }));
  }
  let exists: Option<(i64,)> = sqlx::query_as(
    "SELECT draft_updated_ms FROM carehub_course WHERE id = $1 AND workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  if exists.is_some() {
    Err(bad(STALE_MSG))
  } else {
    Err(missing())
  }
}

/// POST /studio/{id}/publish
async fn studio_publish(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let body = body_json(body)?;
  let note: String = txt(&body, "note").trim().chars().take(500).collect();
  let mut tx = state.pg_pool.begin().await?;
  let row: Option<(String,)> = sqlx::query_as(
    "SELECT draft::text FROM carehub_course WHERE id = $1 AND workspace_id = $2 FOR UPDATE",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let (text,) = row.ok_or_else(missing)?;
  let draft: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
  // qbank3: copy the drawn question banks' questions into the version.
  let frozen = qbank::freeze_draws(&mut tx, &state.pg_pool, c.ws, &draft).await?;
  let checked = frozen.content.as_ref().unwrap_or(&draft);
  let mut problems = publish_problems(checked, text.len().max(frozen.text.len()));
  problems.extend(frozen.problems.iter().cloned());
  if !problems.is_empty() {
    return Err(bad(&problem_sentence(&problems)));
  }
  let (next,): (i32,) = sqlx::query_as(
    "SELECT COALESCE(MAX(version), 0) + 1 FROM carehub_course_version WHERE course_id = $1",
  )
  .bind(course)
  .fetch_one(&mut *tx)
  .await?;
  let now = now_ms();
  // qbank3: a version with bank copies is stored from the frozen content.
  if frozen.content.is_some() {
    qbank::insert_frozen(&mut tx, course, next, &note, c.uid(), now, &frozen.text).await?;
  } else {
    sqlx::query(
      "INSERT INTO carehub_course_version (course_id, version, content, note, published_by, published_ms) \
       SELECT id, $2, draft, $3, $4, $5 FROM carehub_course WHERE id = $1",
    )
    .bind(course)
    .bind(next)
    .bind(note)
    .bind(c.uid())
    .bind(now)
    .execute(&mut *tx)
    .await?;
  }
  sqlx::query(
    "UPDATE carehub_course SET status = 'published', published_version = $2, title = $3, \
       updated_ms = $4, updated_by = $5 WHERE id = $1",
  )
  .bind(course)
  .bind(next)
  .bind(clean_title(txt(&draft, "title")))
  .bind(now)
  .bind(c.uid())
  .execute(&mut *tx)
  .await?;
  tx.commit().await?;
  Ok(json!({ "version": next }))
}

/// POST /studio/{id}/duplicate
async fn studio_duplicate(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let row: Option<(String, String)> = sqlx::query_as(
    "SELECT title, draft::text FROM carehub_course WHERE id = $1 AND workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (title, text) = row.ok_or_else(missing)?;
  let mut draft: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({}));
  if !draft.is_object() {
    draft = json!({});
  }
  let new_title = {
    let from_draft = txt(&draft, "title").trim();
    let base = if from_draft.is_empty() {
      title.trim()
    } else {
      from_draft
    };
    clean_title(&format!("Copy of {}", base))
  };
  if let Some(o) = draft.as_object_mut() {
    o.insert("title".to_string(), json!(new_title));
  }
  let text = draft.to_string();
  if text.len() > MAX_CONTENT_BYTES {
    return Err(bad(TOO_BIG_MSG));
  }
  let id = Uuid::new_v4();
  let now = now_ms();
  sqlx::query(
    "INSERT INTO carehub_course \
       (id, workspace_id, title, status, draft, draft_updated_ms, updated_by, created_by, created_ms, updated_ms) \
     VALUES ($1, $2, $3, 'draft', $4::jsonb, $5, $6, $6, $5, $5)",
  )
  .bind(id)
  .bind(c.ws)
  .bind(new_title)
  .bind(text)
  .bind(now)
  .bind(c.uid())
  .execute(&state.pg_pool)
  .await?;
  Ok(json!({ "id": id.to_string() }))
}

/// POST /studio/{id}/archive
async fn studio_archive(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let body = body_json(body)?;
  let archived = flag(&body, "archived", true);
  let done = sqlx::query(
    "UPDATE carehub_course SET status = CASE WHEN $3 THEN 'archived' \
       WHEN published_version IS NULL THEN 'draft' ELSE 'published' END, \
       updated_ms = $4, updated_by = $5 \
     WHERE id = $1 AND workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .bind(archived)
  .bind(now_ms())
  .bind(c.uid())
  .execute(&state.pg_pool)
  .await?;
  if done.rows_affected() == 0 {
    return Err(missing());
  }
  Ok(json!({ "ok": true }))
}

/// POST /studio/{id}/rollback (copies a version into the draft)
async fn studio_rollback(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let body = body_json(body)?;
  let version = i32::try_from(num(&body, "version", 0)).map_err(|_| bad(VERSION_LINK_MSG))?;
  let row: Option<(i64,)> = sqlx::query_as(
    "UPDATE carehub_course AS c SET draft = v.content, \
       title = LEFT(COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), c.title), 200), \
       draft_updated_ms = GREATEST($4, c.draft_updated_ms + 1), \
       updated_ms = GREATEST($4, c.draft_updated_ms + 1), updated_by = $5 \
     FROM carehub_course_version v \
     WHERE c.id = $1 AND c.workspace_id = $2 AND v.course_id = c.id AND v.version = $3 \
     RETURNING c.draft_updated_ms",
  )
  .bind(course)
  .bind(c.ws)
  .bind(version)
  .bind(now_ms())
  .bind(c.uid())
  .fetch_optional(&state.pg_pool)
  .await?;
  match row {
    Some((updated_ms,)) => Ok(json!({ "updated_ms": updated_ms })),
    None => Err(AppError::RecordNotFound(VERSION_MISSING_MSG.to_string())),
  }
}

/// GET /studio/{id}/versions/{version}
async fn studio_version(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  version: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let version: i32 = version.trim().parse().map_err(|_| bad(VERSION_LINK_MSG))?;
  let row: Option<(i32, String, i64, String, String)> = sqlx::query_as(
    "SELECT v.version, v.content::text, v.published_ms, \
       COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), v.note \
     FROM carehub_course_version v JOIN carehub_course c ON c.id = v.course_id \
     LEFT JOIN af_user u ON u.uid = v.published_by \
     WHERE v.course_id = $1 AND c.workspace_id = $2 AND v.version = $3",
  )
  .bind(course)
  .bind(c.ws)
  .bind(version)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (version, text, published_ms, by_name, note) =
    row.ok_or_else(|| AppError::RecordNotFound(VERSION_MISSING_MSG.to_string()))?;
  let content: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({}));
  let mut out = Map::new();
  out.insert("version".to_string(), json!(version));
  out.insert("content".to_string(), content);
  out.insert("published_ms".to_string(), json!(published_ms));
  out.insert("published_by_name".to_string(), json!(by_name));
  out.insert("note".to_string(), json!(note));
  Ok(Value::Object(out))
}

/// DELETE /studio/{id} (only courses that were never published)
async fn studio_delete(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_build()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let mut tx = state.pg_pool.begin().await?;
  let row: Option<(Option<i32>,)> = sqlx::query_as(
    "SELECT published_version FROM carehub_course WHERE id = $1 AND workspace_id = $2 FOR UPDATE",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(&mut *tx)
  .await?;
  let (published,) = row.ok_or_else(missing)?;
  let (versions,): (i64,) = sqlx::query_as(
    "SELECT COUNT(*)::BIGINT FROM carehub_course_version WHERE course_id = $1",
  )
  .bind(course)
  .fetch_one(&mut *tx)
  .await?;
  if published.is_some() || versions > 0 {
    return Err(bad(PUBLISHED_DELETE_MSG));
  }
  for sql in [
    "DELETE FROM carehub_learning_event WHERE course_id = $1",
    "DELETE FROM carehub_quiz_attempt WHERE course_id = $1",
    "DELETE FROM carehub_enrollment WHERE course_id = $1",
    "DELETE FROM carehub_course WHERE id = $1",
  ] {
    sqlx::query(sql).bind(course).execute(&mut *tx).await?;
  }
  tx.commit().await?;
  Ok(json!({ "ok": true }))
}

// ---------------------------------------------------------------------------
// Publish checks
// ---------------------------------------------------------------------------

// qbank3: pub(crate) for the bank module's checks.
pub(crate) fn url_ok(raw: &str) -> bool {
  // S4: no control characters (U+0000-U+001F, U+007F) anywhere. Browsers drop
  // tabs and line breaks, so "/<tab>/host" would open another website.
  if raw.bytes().any(|b| b.is_ascii_control()) {
    return false;
  }
  let url = raw.trim();
  let lower = url.to_ascii_lowercase();
  // S4: "//host" and "/\host" are other websites to a browser, not Care Hub links.
  (lower.starts_with("https://") && lower.len() > "https://".len())
    || lower == "http://localhost"
    || lower.starts_with("http://localhost/")
    || lower.starts_with("http://localhost:")
    || (url.starts_with('/') && !url.starts_with("//") && !url.starts_with("/\\"))
}

/// S5: the only sites an embed block may show (the web app uses the same list).
/// Shown in a frame: Google Docs / Forms, Microsoft Forms, Canva, Genially,
/// Miro, Figma, Scribe, Tango. Video (the web plays these itself): YouTube,
/// Vimeo, Loom.
const EMBED_HOSTS: &[&str] = &[
  "docs.google.com",
  "forms.gle",
  "forms.office.com",
  "forms.microsoft.com",
  "www.canva.com",
  "view.genial.ly",
  "miro.com",
  "www.figma.com",
  "scribehow.com",
  "app.tango.us",
  "youtube.com",
  "www.youtube.com",
  "m.youtube.com",
  "youtu.be",
  "youtube-nocookie.com",
  "www.youtube-nocookie.com",
  "vimeo.com",
  "www.vimeo.com",
  "player.vimeo.com",
  "loom.com",
  "www.loom.com",
];

/// S5: an embed link is https:// to a host in EMBED_HOSTS (the lowercased text
/// up to the first / ? #), with no port and no "@" anywhere. Care Hub and
/// relative links aren't embeds.
fn embed_ok(raw: &str) -> bool {
  let lower = raw.trim().to_ascii_lowercase();
  let rest = match lower.strip_prefix("https://") {
    Some(r) => r,
    None => return false,
  };
  let host = rest
    .split(|c: char| c == '/' || c == '?' || c == '#')
    .next()
    .unwrap_or("");
  !lower.contains('@') && !host.contains(':') && EMBED_HOSTS.contains(&host)
}

// qbank3: pub(crate) for the bank module's link check.
pub(crate) fn lesson_name(index: usize, lesson: &Value) -> String {
  let title = txt(lesson, "title").trim();
  if title.is_empty() {
    format!("Lesson {}", index + 1)
  } else {
    let short: String = title.chars().take(40).collect();
    let dots = if title.chars().count() > 40 { "…" } else { "" };
    format!("Lesson {} (\"{}{}\")", index + 1, short, dots)
  }
}

/// What's wrong with a hotspot / labeled graphic picture link, if anything
/// (same link rule as image blocks).
fn picture_problem(image: &Value, who: &str) -> Option<String> {
  let url = txt(image, "url").trim();
  if url.is_empty() {
    Some(format!("{} needs a picture.", who))
  } else if !url_ok(url) {
    Some(format!(
      "{} needs a picture link that starts with https:// (or a Care Hub link starting with /).",
      who
    ))
  } else {
    None
  }
}

fn check_question(q: &Value, label: &str, problems: &mut Vec<String>) {
  if !q.is_object() {
    problems.push(format!("{} needs a question.", label));
    return;
  }
  if txt(q, "prompt").trim().is_empty() {
    problems.push(format!("{} needs a question.", label));
  }
  let kind = txt(q, "kind");
  match kind {
    "single" | "true_false" | "multiple" => {
      let options = list(q, "options");
      let correct = options
        .iter()
        .filter(|o| flag(o, "correct", false))
        .count();
      let mut ids: HashSet<&str> = HashSet::new();
      let bad_ids = options.iter().any(|o| {
        let id = txt(o, "id").trim();
        id.is_empty() || !ids.insert(id)
      });
      if options.len() < 2 {
        problems.push(format!("{} needs at least two answers.", label));
      } else if options.iter().any(|o| txt(o, "text").trim().is_empty()) {
        problems.push(format!("{} has an empty answer.", label));
      } else if kind == "multiple" && correct == 0 {
        problems.push(format!("{} needs at least one correct answer.", label));
      } else if kind != "multiple" && correct != 1 {
        problems.push(format!("{} needs exactly one correct answer.", label));
      } else if bad_ids {
        problems.push(format!(
          "{} has answers with missing or repeated ids. Try deleting and re-adding them.",
          label
        ));
      }
    },
    "matching" => {
      let pairs = list(q, "pairs");
      let mut ids: HashSet<&str> = HashSet::new();
      let bad_ids = pairs.iter().any(|p| {
        let id = txt(p, "id").trim();
        id.is_empty() || !ids.insert(id)
      });
      if pairs.len() < 2 {
        problems.push(format!("{} needs at least two pairs to match.", label));
      } else if pairs
        .iter()
        .any(|p| txt(p, "left").trim().is_empty() || txt(p, "right").trim().is_empty())
      {
        problems.push(format!("{} has a pair with an empty side.", label));
      } else if bad_ids {
        problems.push(format!(
          "{} has pairs with missing or repeated ids. Try deleting and re-adding them.",
          label
        ));
      }
    },
    "fill_blank" => {
      let accepted = blank_answers(q);
      if accepted.is_empty() {
        problems.push(format!("{} needs at least one accepted answer.", label));
      } else if accepted
        .iter()
        .any(|a| a.chars().count() > MAX_ACCEPTED_CHARS)
      {
        problems.push(format!("{} has an accepted answer that is too long.", label));
      }
    },
    "ordering" => {
      let items = list(q, "items");
      let mut ids: HashSet<&str> = HashSet::new();
      let bad_ids = items.iter().any(|it| {
        let id = txt(it, "id").trim();
        id.is_empty() || !ids.insert(id)
      });
      if items.len() < 2 {
        problems.push(format!("{} needs at least two items to put in order.", label));
      } else if items.iter().any(|it| txt(it, "text").trim().is_empty()) {
        problems.push(format!("{} has an empty item.", label));
      } else if bad_ids {
        problems.push(format!(
          "{} has items with missing or repeated ids. Try deleting and re-adding them.",
          label
        ));
      }
    },
    "hotspot" => {
      let none = Value::Null;
      let image = q.get("image").unwrap_or(&none);
      if let Some(p) = picture_problem(image, label) {
        problems.push(p);
      }
      if txt(image, "alt").trim().is_empty() {
        problems.push(format!("{} needs a picture description.", label));
      }
      let spots = list(q, "spots");
      if spots.is_empty() {
        problems.push(format!("{} needs at least one right spot.", label));
      } else if spots.iter().any(|s| !spot_inside(s)) {
        problems.push(format!("{} has a spot outside the picture.", label));
      }
    },
    _ => problems.push(format!("{} has an unknown question type.", label)),
  }
  // qbank3: feedback for each answer.
  qbank::check_question_extra(q, label, problems);
}

fn check_block(b: &Value, at: &str, problems: &mut Vec<String>) {
  let kind = txt(b, "type");
  match kind {
    "image" | "video" | "audio" | "file" | "embed" => {
      let url = txt(b, "url").trim();
      if url.is_empty() {
        problems.push(format!("The {} in {} needs a link.", kind, at));
      } else if !url_ok(url) {
        problems.push(format!(
          "The {} link in {} must start with https:// (or be a Care Hub link starting with /).",
          kind, at
        ));
      } else if kind == "embed" && !embed_ok(url) {
        // S5: on publish, embeds may only show the sites in EMBED_HOSTS.
        problems.push(format!(
          "The embed in {} can only show Google Docs or Forms, Microsoft Forms, Canva, Genially, \
           Miro, Figma, Scribe, Tango, YouTube, Vimeo or Loom. For other websites, add a link \
           instead.",
          at
        ));
      }
    },
    "knowledge_check" => {
      let label = format!("The knowledge check in {}", at);
      match b.get("question") {
        Some(q) => check_question(q, &label, problems),
        None => problems.push(format!("{} needs a question.", label)),
      }
    },
    "scenario" => {
      let steps = list(b, "steps");
      if steps.is_empty() {
        problems.push(format!("The scenario in {} needs at least one step.", at));
        return;
      }
      let ids: HashSet<&str> = steps
        .iter()
        .map(|step| txt(step, "id"))
        .filter(|id| !id.is_empty())
        .collect();
      let start = txt(b, "start");
      if !start.is_empty() && !ids.contains(start) {
        problems.push(format!(
          "The scenario in {} starts at a step that doesn't exist.",
          at
        ));
      }
      let broken = steps.iter().any(|step| {
        list(step, "choices").iter().any(|choice| {
          let next = txt(choice, "next");
          !next.is_empty() && !ids.contains(next)
        })
      });
      if broken {
        problems.push(format!(
          "The scenario in {} has a choice that leads to a missing step.",
          at
        ));
      }
    },
    "page_link" => {
      if txt(b, "view_id").trim().is_empty() {
        problems.push(format!("The page link in {} needs a page.", at));
      }
    },
    "labeled_graphic" => {
      let who = format!("The labeled graphic in {}", at);
      let none = Value::Null;
      let image = b.get("image").unwrap_or(&none);
      if let Some(p) = picture_problem(image, &who) {
        problems.push(p);
      }
      if txt(image, "alt").trim().is_empty() {
        problems.push(format!("{} needs a picture description.", who));
      }
      let markers = list(b, "markers");
      if markers.is_empty() {
        problems.push(format!("{} needs at least one marker.", who));
      } else if markers.iter().any(|m| txt(m, "title").trim().is_empty()) {
        problems.push(format!("{} has a marker without a title.", who));
      }
    },
    _ => {},
  }
  // qbank3: video captions and transcripts, and the newer block kinds.
  qbank::check_block_extra(b, at, problems);
}

/// Everything that stops a draft from being published, one sentence each.
fn publish_problems(c: &Value, size: usize) -> Vec<String> {
  let mut problems: Vec<String> = Vec::new();
  if !c.is_object() {
    problems.push("The course content looks damaged. Reload the page and try again.".to_string());
    return problems;
  }
  if size > MAX_CONTENT_BYTES {
    problems.push("It's over 4 MB. Use links for big images and videos.".to_string());
  }
  let title = txt(c, "title").trim();
  if title.is_empty() {
    problems.push("Give the course a title.".to_string());
  } else if title.chars().count() > MAX_TITLE_CHARS {
    problems.push("Shorten the course title to 200 characters or fewer.".to_string());
  }
  let cover = txt(c, "cover").trim();
  if !cover.is_empty() && !url_ok(cover) {
    problems.push("The cover image link must start with https://.".to_string());
  }
  let empty = json!({});
  let st = match c.get("settings") {
    Some(v) if v.is_object() => v,
    _ => &empty,
  };
  if !(0_i64..=100).contains(&num(st, "pass_mark", 80)) {
    problems.push("Set the pass mark between 0 and 100.".to_string());
  }
  if !(0_i64..=20).contains(&num(st, "max_attempts", 0)) {
    problems.push("Set quiz attempts between 0 (unlimited) and 20.".to_string());
  }
  // P2 recert
  if let Some(msg) = carehub_learning_assign::recert_problem(st) {
    problems.push(msg.to_string());
  }
  let lessons = list(c, "lessons");
  if lessons.is_empty() {
    problems.push("Add at least one lesson.".to_string());
  }
  let mut lesson_ids: HashSet<&str> = HashSet::new();
  let mut block_ids: HashSet<&str> = HashSet::new();
  for (li, lesson) in lessons.iter().enumerate() {
    let name = lesson_name(li, lesson);
    let lid = txt(lesson, "id").trim();
    if lid.is_empty() || lid == QUIZ_LESSON || !lesson_ids.insert(lid) {
      problems.push(format!(
        "{} has a missing or repeated id. Try duplicating it and deleting the original.",
        name
      ));
    }
    if txt(lesson, "title").trim().is_empty() {
      problems.push(format!("{} needs a title.", name));
    }
    let blocks = list(lesson, "blocks");
    if blocks.is_empty() {
      problems.push(format!("{} needs at least one block.", name));
    }
    for (bi, block) in blocks.iter().enumerate() {
      let at = format!("block {} of {}", bi + 1, name);
      let bid = txt(block, "id").trim();
      if bid.is_empty() || !block_ids.insert(bid) {
        problems.push(format!(
          "Block {} of {} has a missing or repeated id. Try duplicating it and deleting the original.",
          bi + 1,
          name
        ));
      }
      check_block(block, &at, &mut problems);
    }
  }
  // qbank3: buttons and text links must go to a lesson or block of this course.
  qbank::link_problems(c, &mut problems);
  let quiz = match c.get("quiz") {
    Some(v) if v.is_object() => v,
    _ => &empty,
  };
  if flag(quiz, "enabled", false) {
    let questions = list(quiz, "questions");
    if questions.is_empty() {
      problems.push("Add at least one quiz question, or turn the quiz off.".to_string());
    }
    let pool = num(quiz, "pool_size", 0);
    // qbank3: the pool counts the quiz's own questions only (not bank copies).
    let own = questions.iter().filter(|q| !qbank::from_bank(q)).count();
    if pool < 0 || pool > own as i64 {
      problems.push(
        "The quiz can't ask more questions than it has. Lower the number of questions to ask."
          .to_string(),
      );
    }
    let mut question_ids: HashSet<&str> = HashSet::new();
    // qbank3: bank copies are labelled `Question {k} in “{bank}”`.
    let labels = qbank::question_labels(quiz);
    for (qi, q) in questions.iter().enumerate() {
      let label = match labels.get(qi) {
        Some(l) => l.clone(),
        None => format!("Quiz question {}", qi + 1),
      };
      let qid = txt(q, "id").trim();
      if qid.is_empty() || !question_ids.insert(qid) {
        problems.push(format!(
          "{} has a missing or repeated id. Try duplicating it and deleting the original.",
          label
        ));
      }
      check_question(q, &label, &mut problems);
    }
  }
  problems
}

fn problem_sentence(problems: &[String]) -> String {
  let mut out = String::from("This course can't be published yet.");
  for p in problems.iter().take(5) {
    out.push(' ');
    out.push_str(p);
  }
  if problems.len() > 5 {
    out.push_str(&format!(" (And {} more to fix.)", problems.len() - 5));
  }
  out
}

// ---------------------------------------------------------------------------
// Reports (learning_build or learning_analytics; members = non-guests)
// ---------------------------------------------------------------------------

async fn member_count(pool: &PgPool, ws: Uuid) -> Result<i64, AppError> {
  let (n,): (i64,) = sqlx::query_as(
    "SELECT COUNT(*)::BIGINT FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
     WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
       AND COALESCE(u.email, '') NOT LIKE '%.removed-%'",
  )
  .bind(ws)
  .bind(guest_role())
  .fetch_one(pool)
  .await?;
  Ok(n)
}

fn round_opt(v: Option<f64>) -> Option<i64> {
  v.filter(|x| x.is_finite()).map(|x| x.round() as i64)
}

/// GET /reports/courses
async fn report_courses(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_reports()?;
  let members = member_count(&state.pg_pool, c.ws).await?;
  #[allow(clippy::type_complexity)]
  let rows: Vec<(
    Uuid,
    String,
    String,
    i32,
    i64,
    i64,
    i64,
    i64,
    i64,
    Option<f64>,
    f64,
    Option<i64>,
  )> = sqlx::query_as(
    "SELECT c.id, \
       COALESCE((SELECT NULLIF(BTRIM(v.content->>'title'), '') FROM carehub_course_version v \
         WHERE v.course_id = c.id AND v.version = c.published_version), \
         NULLIF(c.title, ''), 'Untitled course'), \
       c.status, COALESCE(c.published_version, 0), \
       COUNT(e.uid)::BIGINT, \
       (COUNT(e.uid) FILTER (WHERE e.status = 'in_progress'))::BIGINT, \
       (COUNT(e.uid) FILTER (WHERE e.status IN ('completed', 'passed')))::BIGINT, \
       (COUNT(e.uid) FILTER (WHERE e.status = 'passed'))::BIGINT, \
       (COUNT(e.uid) FILTER (WHERE e.status = 'failed'))::BIGINT, \
       AVG(e.score)::FLOAT8, COALESCE(AVG(e.time_ms), 0)::FLOAT8, MAX(e.last_ms) \
     FROM carehub_course c \
     LEFT JOIN carehub_enrollment e ON e.course_id = c.id AND e.uid IN ( \
       SELECT m.uid FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
       WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
         AND COALESCE(u.email, '') NOT LIKE '%.removed-%') \
     WHERE c.workspace_id = $1 AND c.published_version IS NOT NULL \
     GROUP BY c.id ORDER BY 2",
  )
  .bind(c.ws)
  .bind(guest_role())
  .fetch_all(&state.pg_pool)
  .await?;
  let courses: Vec<Value> = rows
    .iter()
    .map(|r| {
      json!({
        "id": r.0.to_string(),
        "title": r.1,
        "status": r.2,
        "version": r.3,
        "members": members,
        "started": r.4,
        "in_progress": r.5,
        "finished": r.6,
        "passed": r.7,
        "failed": r.8,
        "not_started": (members - r.4).max(0),
        "avg_score": round_opt(r.9),
        "avg_time_ms": round_opt(Some(r.10)).unwrap_or(0),
        "last_activity_ms": r.11,
      })
    })
    .collect();
  Ok(json!({ "courses": courses }))
}

/// GET /reports/courses/{id}
async fn report_course(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_reports()?;
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let head: Option<(String, Option<i32>, String, String)> = sqlx::query_as(
    "SELECT COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), NULLIF(c.title, ''), 'Untitled course'), \
       c.published_version, \
       COALESCE((v.content->'quiz')::text, '{}'), COALESCE((v.content->'settings')::text, '{}') \
     FROM carehub_course c \
     LEFT JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.id = $1 AND c.workspace_id = $2",
  )
  .bind(course)
  .bind(c.ws)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (title, version, quiz_text, settings_text) = head.ok_or_else(missing)?;
  let mut mini = Map::new();
  mini.insert(
    "quiz".to_string(),
    serde_json::from_str::<Value>(&quiz_text).unwrap_or(Value::Null),
  );
  mini.insert(
    "settings".to_string(),
    serde_json::from_str::<Value>(&settings_text).unwrap_or(Value::Null),
  );
  let mini = Value::Object(mini);
  let st = settings_of(&mini);
  #[allow(clippy::type_complexity)]
  let rows: Vec<(
    i64,
    String,
    String,
    String,
    i32,
    Option<i32>,
    i32,
    i64,
    i64,
    i64,
    Option<i64>,
    Option<i64>,
    bool,
    i32,
  )> = sqlx::query_as(
    "SELECT e.uid, COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), \
       COALESCE(u.email, ''), e.status, e.percent, e.score, e.attempts, e.time_ms, e.started_ms, \
       e.last_ms, e.completed_ms, e.ack_ms, e.awaiting_ack, e.version \
     FROM carehub_enrollment e \
     JOIN af_workspace_member m ON m.uid = e.uid AND m.workspace_id = $2 \
     JOIN af_user u ON u.uid = e.uid \
     WHERE e.course_id = $1 AND m.role_id::INT <> $3 \
       AND COALESCE(u.email, '') NOT LIKE '%.removed-%' \
     ORDER BY e.last_ms DESC",
  )
  .bind(course)
  .bind(c.ws)
  .bind(guest_role())
  .fetch_all(&state.pg_pool)
  .await?;
  let waiting: Vec<(i64, String, String)> = sqlx::query_as(
    "SELECT m.uid, COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), \
       COALESCE(u.email, '') \
     FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
     WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
       AND COALESCE(u.email, '') NOT LIKE '%.removed-%' \
       AND NOT EXISTS (SELECT 1 FROM carehub_enrollment e WHERE e.course_id = $3 AND e.uid = m.uid) \
     ORDER BY 2",
  )
  .bind(c.ws)
  .bind(guest_role())
  .bind(course)
  .fetch_all(&state.pg_pool)
  .await?;
  let stats: Vec<(String, i64, i64)> = match version {
    Some(v) if st.quiz_on => {
      sqlx::query_as(
        "SELECT COALESCE(r.value->>'question_id', ''), COUNT(*)::BIGINT, \
           (COUNT(*) FILTER (WHERE r.value->>'correct' = 'true'))::BIGINT \
         FROM carehub_quiz_attempt a \
         CROSS JOIN LATERAL jsonb_array_elements( \
           CASE WHEN jsonb_typeof(a.results) = 'array' THEN a.results ELSE '[]'::jsonb END) AS r(value) \
         WHERE a.course_id = $1 AND a.version = $2 \
         GROUP BY 1",
      )
      .bind(course)
      .bind(v)
      .fetch_all(&state.pg_pool)
      .await?
    },
    _ => Vec::new(),
  };
  let by_question: HashMap<&str, (i64, i64)> = stats
    .iter()
    .map(|r| (r.0.as_str(), (r.1, r.2)))
    .collect();
  let questions: Vec<Value> = if st.quiz_on {
    match mini.get("quiz") {
      Some(q) => list(q, "questions")
        .iter()
        .map(|q| {
          let (answered, right) = by_question.get(txt(q, "id")).copied().unwrap_or((0, 0));
          let pct = if answered > 0 {
            (right * 100 + answered / 2) / answered
          } else {
            0
          };
          json!({ "id": txt(q, "id"), "prompt": txt(q, "prompt"), "answered": answered, "correct_pct": pct })
        })
        .collect(),
      None => Vec::new(),
    }
  } else {
    Vec::new()
  };
  let mut rows: Vec<Value> = rows
    .iter()
    .map(|r| {
      json!({
        "uid": r.0.to_string(),
        "name": r.1,
        "email": r.2,
        "status": r.3,
        "percent": r.4,
        "score": r.5,
        "attempts": r.6,
        "time_ms": r.7,
        "started_ms": r.8,
        "last_ms": r.9,
        "completed_ms": r.10,
        "ack_ms": r.11,
        "awaiting_ack": r.12,
        "version": r.13,
      })
    })
    .collect();
  // P2 recert
  carehub_learning_assign::add_row_expiry(&state.pg_pool, &mut rows, Some(course)).await;
  let not_started: Vec<Value> = waiting
    .iter()
    .map(|r| json!({ "uid": r.0.to_string(), "name": r.1, "email": r.2 }))
    .collect();
  Ok(json!({
    "course": {
      "id": course.to_string(),
      "title": title,
      "version": version.unwrap_or(0),
      "quiz": st.quiz_on,
      "pass_mark": st.pass_mark,
    },
    "rows": rows,
    "not_started": not_started,
    "questions": questions,
  }))
}

/// GET /reports/people
async fn report_people(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_reports()?;
  #[allow(clippy::type_complexity)]
  let rows: Vec<(i64, String, String, i64, i64, i64, i64, Option<f64>, i64, Option<i64>)> =
    sqlx::query_as(
      "SELECT m.uid, COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), \
         COALESCE(u.email, ''), COALESCE(s.started, 0)::BIGINT, COALESCE(s.finished, 0)::BIGINT, \
         COALESCE(s.in_progress, 0)::BIGINT, COALESCE(s.failed, 0)::BIGINT, s.avg_score, \
         COALESCE(s.time_ms, 0)::BIGINT, s.last_ms \
       FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
       LEFT JOIN ( \
         SELECT e.uid, COUNT(*) AS started, \
           COUNT(*) FILTER (WHERE e.status IN ('completed', 'passed')) AS finished, \
           COUNT(*) FILTER (WHERE e.status = 'in_progress') AS in_progress, \
           COUNT(*) FILTER (WHERE e.status = 'failed') AS failed, \
           AVG(e.score)::FLOAT8 AS avg_score, SUM(e.time_ms)::BIGINT AS time_ms, \
           MAX(e.last_ms) AS last_ms \
         FROM carehub_enrollment e JOIN carehub_course c ON c.id = e.course_id \
         WHERE c.workspace_id = $1 GROUP BY e.uid \
       ) s ON s.uid = m.uid \
       WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
         AND COALESCE(u.email, '') NOT LIKE '%.removed-%' \
       ORDER BY 2",
    )
    .bind(c.ws)
    .bind(guest_role())
    .fetch_all(&state.pg_pool)
    .await?;
  let people: Vec<Value> = rows
    .iter()
    .map(|r| {
      json!({
        "uid": r.0.to_string(),
        "name": r.1,
        "email": r.2,
        "started": r.3,
        "finished": r.4,
        "in_progress": r.5,
        "failed": r.6,
        "avg_score": round_opt(r.7),
        "time_ms": r.8,
        "last_ms": r.9,
      })
    })
    .collect();
  Ok(json!({ "people": people }))
}

/// GET /reports/people/{uid}
async fn report_person(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  person: &str,
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  c.need_reports()?;
  let uid: i64 = person.trim().parse().map_err(|_| bad(PERSON_LINK_MSG))?;
  let who: Option<(i64, String, String)> = sqlx::query_as(
    "SELECT m.uid, COALESCE(NULLIF(u.name, ''), split_part(COALESCE(u.email, ''), '@', 1), ''), \
       COALESCE(u.email, '') \
     FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
     WHERE m.workspace_id = $1 AND m.uid = $2 AND m.role_id::INT <> $3 \
       AND COALESCE(u.email, '') NOT LIKE '%.removed-%'",
  )
  .bind(c.ws)
  .bind(uid)
  .bind(guest_role())
  .fetch_optional(&state.pg_pool)
  .await?;
  let who = who.ok_or_else(|| AppError::RecordNotFound(PERSON_MISSING_MSG.to_string()))?;
  #[allow(clippy::type_complexity)]
  let rows: Vec<(
    Uuid,
    String,
    String,
    i32,
    Option<i32>,
    i32,
    i64,
    i64,
    i64,
    Option<i64>,
    Option<i64>,
  )> = sqlx::query_as(
    "SELECT e.course_id, \
       COALESCE((SELECT NULLIF(BTRIM(v.content->>'title'), '') FROM carehub_course_version v \
         WHERE v.course_id = c.id AND v.version = e.version), NULLIF(c.title, ''), 'Untitled course'), \
       e.status, e.percent, e.score, e.attempts, e.time_ms, e.started_ms, e.last_ms, \
       e.completed_ms, e.ack_ms \
     FROM carehub_enrollment e JOIN carehub_course c ON c.id = e.course_id \
     WHERE c.workspace_id = $1 AND e.uid = $2 \
     ORDER BY e.last_ms DESC",
  )
  .bind(c.ws)
  .bind(uid)
  .fetch_all(&state.pg_pool)
  .await?;
  let mut rows: Vec<Value> = rows
    .iter()
    .map(|r| {
      json!({
        "course_id": r.0.to_string(),
        "title": r.1,
        "status": r.2,
        "percent": r.3,
        "score": r.4,
        "attempts": r.5,
        "time_ms": r.6,
        "started_ms": r.7,
        "last_ms": r.8,
        "completed_ms": r.9,
        "ack_ms": r.10,
      })
    })
    .collect();
  // P2 recert
  carehub_learning_assign::add_row_expiry(&state.pg_pool, &mut rows, None).await;
  Ok(json!({
    "person": { "uid": who.0.to_string(), "name": who.1, "email": who.2 },
    "rows": rows,
  }))
}

/// POST /reports/courses/{id}/reset (learning_build only). Keeps old quiz
/// attempts and events for history.
async fn reset_person(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = ctx(state, user, ws).await?;
  if !c.has("learning_build") {
    return Err(bad(RESET_MSG));
  }
  let course = parse_uuid(id, COURSE_LINK_MSG)?;
  let body = body_json(body)?;
  let uid = uid_of(body.get("uid")).ok_or_else(|| bad(PERSON_LINK_MSG))?;
  let mut tx = state.pg_pool.begin().await?;
  head_of(&mut tx, c.ws, course).await?.ok_or_else(missing)?;
  // P2 recert: shared with renewals (carehub_learning_assign.rs).
  let by = json!({ "by": c.uid().to_string() });
  reset_progress(&mut tx, c.ws, course, uid, by).await?;
  tx.commit().await?;
  Ok(json!({ "ok": true }))
}

/// P2 recert: clears one person's progress on a course (old quiz attempts and
/// events stay) and logs a "reset" event with `data`; false = nothing to clear.
pub(crate) async fn reset_progress(
  conn: &mut PgConnection,
  ws: Uuid,
  course: Uuid,
  uid: i64,
  data: Value,
) -> Result<bool, AppError> {
  let done = sqlx::query("DELETE FROM carehub_enrollment WHERE course_id = $1 AND uid = $2")
    .bind(course)
    .bind(uid)
    .execute(&mut *conn)
    .await?;
  if done.rows_affected() == 0 {
    return Ok(false);
  }
  let mut ev = Ev::new("reset", now_ms());
  ev.data = Some(data.to_string());
  log_event(&mut *conn, ws, course, uid, ev).await?;
  Ok(true)
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

async fn catalog_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  reply(course_cards(&state, &user, &ws, false).await)
}

async fn my_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  reply(course_cards(&state, &user, &ws, true).await)
}

async fn play_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(play(&state, &user, &ws, &id).await)
}

async fn start_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(start(&state, &user, &ws, &id).await)
}

async fn progress_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(progress(&state, &user, &ws, &id, &body).await)
}

async fn quiz_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(submit_quiz(&state, &user, &ws, &id, &body).await)
}

async fn ack_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(acknowledge(&state, &user, &ws, &id).await)
}

async fn studio_list_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
) -> Reply {
  let ws = path.into_inner();
  reply(studio_list(&state, &user, &ws).await)
}

async fn studio_create_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
  body: Bytes,
) -> Reply {
  let ws = path.into_inner();
  reply(studio_create(&state, &user, &ws, &body).await)
}

async fn studio_get_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(studio_get(&state, &user, &ws, &id).await)
}

async fn studio_save_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(studio_save(&state, &user, &ws, &id, &body).await)
}

async fn studio_delete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(studio_delete(&state, &user, &ws, &id).await)
}

async fn studio_publish_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(studio_publish(&state, &user, &ws, &id, &body).await)
}

async fn studio_duplicate_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(studio_duplicate(&state, &user, &ws, &id).await)
}

async fn studio_archive_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(studio_archive(&state, &user, &ws, &id, &body).await)
}

async fn studio_rollback_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(studio_rollback(&state, &user, &ws, &id, &body).await)
}

async fn studio_version_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String, String)>,
) -> Reply {
  let (ws, id, version) = path.into_inner();
  reply(studio_version(&state, &user, &ws, &id, &version).await)
}

async fn report_courses_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
) -> Reply {
  let ws = path.into_inner();
  reply(report_courses(&state, &user, &ws).await)
}

async fn report_course_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(report_course(&state, &user, &ws, &id).await)
}

async fn report_people_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
) -> Reply {
  let ws = path.into_inner();
  reply(report_people(&state, &user, &ws).await)
}

async fn report_person_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, uid) = path.into_inner();
  reply(report_person(&state, &user, &ws, &uid).await)
}

async fn reset_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  reply(reset_person(&state, &user, &ws, &id, &body).await)
}

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/catalog")
        .route(web::get().to(catalog_handler)),
    )
    .service(web::resource("/api/carehub/ws/{ws}/learning/my").route(web::get().to(my_handler)))
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/courses/{id}/play")
        .route(web::get().to(play_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/courses/{id}/start")
        .route(web::post().to(start_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/courses/{id}/progress")
        .app_data(web::PayloadConfig::new(PLAYER_BODY_BYTES))
        .route(web::post().to(progress_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/courses/{id}/quiz")
        .app_data(web::PayloadConfig::new(PLAYER_BODY_BYTES))
        .route(web::post().to(quiz_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/courses/{id}/ack")
        .route(web::post().to(ack_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio")
        .app_data(web::PayloadConfig::new(STUDIO_BODY_BYTES))
        .route(web::get().to(studio_list_handler))
        .route(web::post().to(studio_create_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}")
        .app_data(web::PayloadConfig::new(STUDIO_BODY_BYTES))
        .route(web::get().to(studio_get_handler))
        .route(web::put().to(studio_save_handler))
        .route(web::delete().to(studio_delete_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/publish")
        .route(web::post().to(studio_publish_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/duplicate")
        .route(web::post().to(studio_duplicate_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/archive")
        .route(web::post().to(studio_archive_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/rollback")
        .route(web::post().to(studio_rollback_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/studio/{id}/versions/{version}")
        .route(web::get().to(studio_version_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/courses")
        .route(web::get().to(report_courses_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/courses/{id}")
        .route(web::get().to(report_course_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/courses/{id}/reset")
        .route(web::post().to(reset_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/people")
        .route(web::get().to(report_people_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/people/{uid}")
        .route(web::get().to(report_person_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/reports/overview")
        .route(web::get().to(crate::api::carehub_learning_overview::overview_handler)),
    );
  // qbank3: question banks and survey results.
  qbank::configure(cfg);
  crate::api::carehub_learning_drafts::configure(cfg); // drafts: review links + comments
  // templates: block templates (save blocks and reuse them).
  crate::api::carehub_learning_templates::configure(cfg);
}
