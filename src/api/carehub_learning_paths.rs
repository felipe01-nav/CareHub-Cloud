//! CareHub Learning paths (lane paths3, P3 paths): an ordered set of 2-20
//! courses assigned together. A path's assignments are ordinary assignments
//! (one per course and target, path_id set), so counts, insights, group / role
//! sync and renewals treat them like any other (carehub_learning_assign.rs).
//! This file keeps the paths themselves, their endpoints, GET /paths/mine and
//! what path rows get instead of the per-course messages: one "New learning
//! path" message per person, reminders for their next course only, and one
//! calendar entry per person and path.

use crate::api::carehub_calendar;
use crate::api::carehub_learning_assign::{self as assign, Asg, Board, Group, Reply, ROW_DUE};
use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Bytes, Data};
use app_error::AppError;
use serde_json::{json, Value};
use sqlx::{PgConnection, PgPool};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use uuid::Uuid;

const MAX_TITLE_CHARS: usize = 80;
const MAX_DESCRIPTION_CHARS: usize = 500;
const MIN_COURSES: usize = 2;
const MAX_COURSES: usize = 20;
/// People handled per background run for messages / reminders (the rest wait
/// a minute).
const PATH_BATCH: usize = 200;

const TITLE_MSG: &str = "Give the path a name (up to 80 characters).";
const DESCRIPTION_MSG: &str = "Keep the description to 500 characters or fewer.";
const COURSES_MSG: &str = "A learning path needs 2 to 20 different courses.";
const UNKNOWN_COURSES_MSG: &str =
  "Some of those courses couldn't be found. Please reload the page and try again.";
const ARCHIVED_PATH_MSG: &str = "This path is archived. Unarchive it before assigning it.";
const PATH_LINK_MSG: &str = "That learning path link doesn't look right.";
const PATH_MISSING_MSG: &str = "That learning path couldn't be found. It may have been deleted.";
const NO_TARGETS_MSG: &str = "Pick at least one person, role or group.";

fn path_missing() -> AppError {
  AppError::RecordNotFound(PATH_MISSING_MSG.to_string())
}

// ---------------------------------------------------------------------------
// Paths as stored
// ---------------------------------------------------------------------------

struct PathRec {
  id: Uuid,
  title: String,
  description: String,
  courses: Vec<Uuid>,
  in_order: bool,
  archived: bool,
  created_by: i64,
  created_ms: i64,
  updated_ms: i64,
}

type PathRow = (Uuid, String, String, Vec<Uuid>, bool, bool, i64, i64, i64);

const PATH_COLUMNS: &str =
  "id, title, description, course_ids, in_order, archived, created_by, created_ms, updated_ms";

fn path_rec(r: PathRow) -> PathRec {
  PathRec {
    id: r.0,
    title: r.1,
    description: r.2,
    courses: r.3,
    in_order: r.4,
    archived: r.5,
    created_by: r.6,
    created_ms: r.7,
    updated_ms: r.8,
  }
}

/// The workspace's paths by title (`only`: just that one; `lock`: FOR UPDATE).
async fn paths_of(
  conn: &mut PgConnection,
  ws: Uuid,
  only: Option<Uuid>,
  lock: bool,
) -> Result<Vec<PathRec>, AppError> {
  let sql = format!(
    "SELECT {PATH_COLUMNS} FROM carehub_learning_path \
     WHERE workspace_id = $1 AND ($2::UUID IS NULL OR id = $2) ORDER BY LOWER(title), id{}",
    if lock { " FOR UPDATE" } else { "" }
  );
  let rows: Vec<PathRow> = sqlx::query_as(&sql)
    .bind(ws)
    .bind(only)
    .fetch_all(&mut *conn)
    .await?;
  Ok(rows.into_iter().map(path_rec).collect())
}

async fn one_path(
  conn: &mut PgConnection,
  ws: Uuid,
  id: Uuid,
  lock: bool,
) -> Result<PathRec, AppError> {
  paths_of(conn, ws, Some(id), lock)
    .await?
    .into_iter()
    .next()
    .ok_or_else(path_missing)
}

fn path_title(v: &Value) -> Result<String, AppError> {
  let title = assign::txt(v, "title").trim().to_string();
  if title.is_empty() || title.chars().count() > MAX_TITLE_CHARS {
    return Err(assign::bad(TITLE_MSG));
  }
  Ok(title)
}

/// None = leave as it is; null clears.
fn path_description(v: &Value) -> Result<Option<String>, AppError> {
  match v.get("description") {
    None => Ok(None),
    Some(Value::Null) => Ok(Some(String::new())),
    Some(Value::String(s)) if s.trim().chars().count() <= MAX_DESCRIPTION_CHARS => {
      Ok(Some(s.trim().to_string()))
    },
    Some(_) => Err(assign::bad(DESCRIPTION_MSG)),
  }
}

/// course_ids: 2-20 different course ids, in order.
fn course_list(v: &Value) -> Result<Vec<Uuid>, AppError> {
  let raw = match v.get("course_ids") {
    Some(Value::Array(a)) => a,
    _ => return Err(assign::bad(COURSES_MSG)),
  };
  if raw.len() < MIN_COURSES || raw.len() > MAX_COURSES {
    return Err(assign::bad(COURSES_MSG));
  }
  let mut out: Vec<Uuid> = Vec::with_capacity(raw.len());
  for x in raw {
    let id = x
      .as_str()
      .and_then(|s| Uuid::parse_str(s.trim()).ok())
      .ok_or_else(|| assign::bad(UNKNOWN_COURSES_MSG))?;
    if out.contains(&id) {
      return Err(assign::bad(COURSES_MSG));
    }
    out.push(id);
  }
  Ok(out)
}

/// Every course is in the workspace; the ones new to the path (not in `old`)
/// are published now.
async fn check_courses(
  conn: &mut PgConnection,
  ws: Uuid,
  ids: &[Uuid],
  old: &[Uuid],
) -> Result<(), AppError> {
  let rows: Vec<(Uuid, String, String)> = sqlx::query_as(
    "SELECT c.id, c.status, \
       COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), NULLIF(c.title, ''), 'Untitled course') \
     FROM carehub_course c \
     LEFT JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.workspace_id = $1 AND c.id = ANY($2)",
  )
  .bind(ws)
  .bind(ids.to_vec())
  .fetch_all(&mut *conn)
  .await?;
  let found: HashMap<Uuid, (String, String)> = rows
    .into_iter()
    .map(|(id, status, title)| (id, (status, title)))
    .collect();
  for id in ids {
    let (status, title) = found
      .get(id)
      .ok_or_else(|| assign::bad(UNKNOWN_COURSES_MSG))?;
    if old.contains(id) || status == "published" {
      continue;
    }
    if status == "archived" {
      return Err(assign::bad(&format!(
        "“{}” is archived. Unarchive it before adding it to a path.",
        title
      )));
    }
    return Err(assign::bad(&format!(
      "Publish “{}” before adding it to a path.",
      title
    )));
  }
  Ok(())
}

async fn title_taken(
  conn: &mut PgConnection,
  ws: Uuid,
  title: &str,
  except: Option<Uuid>,
) -> Result<bool, AppError> {
  let found: Option<Uuid> = sqlx::query_scalar(
    "SELECT id FROM carehub_learning_path WHERE workspace_id = $1 AND LOWER(title) = LOWER($2) \
       AND ($3::UUID IS NULL OR id <> $3) LIMIT 1",
  )
  .bind(ws)
  .bind(title)
  .bind(except)
  .fetch_optional(&mut *conn)
  .await?;
  Ok(found.is_some())
}

fn title_clash(title: &str) -> AppError {
  assign::bad(&format!("There's already a path called “{}”.", title))
}

/// Status and live title of courses published at least once.
async fn course_meta(
  pool: &PgPool,
  ids: &[Uuid],
) -> Result<HashMap<Uuid, (String, String)>, AppError> {
  let rows: Vec<(Uuid, String, String)> = sqlx::query_as(
    "SELECT c.id, c.status, \
       COALESCE(NULLIF(BTRIM(v.content->>'title'), ''), NULLIF(c.title, ''), 'Untitled course') \
     FROM carehub_course c \
     JOIN carehub_course_version v ON v.course_id = c.id AND v.version = c.published_version \
     WHERE c.id = ANY($1) AND c.published_version IS NOT NULL",
  )
  .bind(ids.to_vec())
  .fetch_all(pool)
  .await?;
  Ok(
    rows
      .into_iter()
      .map(|(id, status, title)| (id, (status, title)))
      .collect(),
  )
}

// ---------------------------------------------------------------------------
// Where people are on a path
// ---------------------------------------------------------------------------

/// One person on a path's steps (its courses that are published now).
struct Walk {
  /// "not_started" | "in_progress" | "finished"
  status: &'static str,
  /// Floor of the average step percent (a finished step = 100).
  percent: i64,
  /// Finished steps.
  done: usize,
  /// The first unfinished step.
  next: Option<usize>,
}

/// `states`: per step, (status, percent) once they've started it.
fn walk_steps(states: &[Option<(&str, i32)>]) -> Walk {
  let mut done = 0;
  let mut started = false;
  let mut sum: i64 = 0;
  let mut next: Option<usize> = None;
  for (i, s) in states.iter().enumerate() {
    match s {
      Some((status, _)) if assign::finished(status) => {
        done += 1;
        sum += 100;
      },
      _ => {
        if let Some((_, percent)) = s {
          sum += i64::from((*percent).clamp(0, 100));
        }
        next = next.or(Some(i));
      },
    }
    started = started || s.is_some();
  }
  let n = states.len();
  let status = if n > 0 && done == n {
    "finished"
  } else if started {
    "in_progress"
  } else {
    "not_started"
  };
  let percent = if n == 0 { 0 } else { sum / n as i64 };
  Walk {
    status,
    percent,
    done,
    next,
  }
}

/// A current member with a row on a path.
struct Member {
  walk: Walk,
  /// Their earliest due on the next step's course.
  due: Option<i64>,
  overdue: bool,
  assigned_ms: i64,
}

/// One target of a path: its assignments (one per course) and who they cover.
struct Target<'a> {
  asgs: Vec<&'a Asg>,
  uids: BTreeSet<i64>,
}

/// A path as the Board sees it.
struct View<'a> {
  steps: Vec<Uuid>,
  members: BTreeMap<i64, Member>,
  targets: BTreeMap<(String, String), Target<'a>>,
}

fn view<'a>(board: &'a Board, p: &PathRec) -> View<'a> {
  let steps: Vec<Uuid> = p
    .courses
    .iter()
    .copied()
    .filter(|c| {
      board
        .courses
        .get(c)
        .map(|x| x.status == "published")
        .unwrap_or(false)
    })
    .collect();
  let mut targets: BTreeMap<(String, String), Target<'a>> = BTreeMap::new();
  // uid → their rows on the path: (course, assigned_ms, due)
  let mut rows: BTreeMap<i64, Vec<(Uuid, i64, Option<i64>)>> = BTreeMap::new();
  for a in board.asgs.iter().filter(|a| a.path == Some(p.id)) {
    let t = targets
      .entry((a.kind.clone(), a.target.clone()))
      .or_insert_with(|| Target {
        asgs: Vec::new(),
        uids: BTreeSet::new(),
      });
    t.asgs.push(a);
    for uid in board.rows.get(&a.id).into_iter().flatten() {
      t.uids.insert(*uid);
      let (assigned_ms, due) = board
        .seats
        .get(&(a.id, *uid))
        .copied()
        .unwrap_or((0, None));
      rows
        .entry(*uid)
        .or_default()
        .push((a.course, assigned_ms, due));
    }
  }
  let members = rows
    .into_iter()
    .map(|(uid, mine)| (uid, member(board, &steps, uid, &mine)))
    .collect();
  View {
    steps,
    members,
    targets,
  }
}

fn member(board: &Board, steps: &[Uuid], uid: i64, mine: &[(Uuid, i64, Option<i64>)]) -> Member {
  let states: Vec<Option<(&str, i32)>> = steps
    .iter()
    .map(|c| {
      board
        .enr
        .get(&(*c, uid))
        .map(|e| (e.status.as_str(), e.percent))
    })
    .collect();
  let walk = walk_steps(&states);
  let due = walk
    .next
    .and_then(|i| steps.get(i))
    .and_then(|c| mine.iter().filter(|r| r.0 == *c).filter_map(|r| r.2).min());
  let overdue = walk.status != "finished" && due.map(|d| d < board.now).unwrap_or(false);
  Member {
    walk,
    due,
    overdue,
    assigned_ms: mine.iter().map(|r| r.1).min().unwrap_or(0),
  }
}

#[derive(Default)]
struct Count {
  people: i64,
  finished: i64,
  in_progress: i64,
  not_started: i64,
  overdue: i64,
}

impl Count {
  fn add(&mut self, m: &Member) {
    self.people += 1;
    match m.walk.status {
      "finished" => self.finished += 1,
      "in_progress" => self.in_progress += 1,
      _ => self.not_started += 1,
    }
    if m.overdue {
      self.overdue += 1;
    }
  }

  fn put(&self, v: &mut Value) {
    if let Some(o) = v.as_object_mut() {
      o.insert("people".to_string(), json!(self.people));
      o.insert("finished".to_string(), json!(self.finished));
      o.insert("in_progress".to_string(), json!(self.in_progress));
      o.insert("not_started".to_string(), json!(self.not_started));
      o.insert("overdue".to_string(), json!(self.overdue));
    }
  }
}

fn course_title(board: &Board, course: Uuid) -> String {
  board
    .courses
    .get(&course)
    .map(|c| c.title.clone())
    .unwrap_or_else(|| "Untitled course".to_string())
}

fn path_json(p: &PathRec, board: &Board, v: &View, names: &HashMap<i64, String>) -> Value {
  let mut count = Count::default();
  for m in v.members.values() {
    count.add(m);
  }
  // Deleted courses are left out.
  let ids: Vec<String> = p
    .courses
    .iter()
    .filter(|c| board.courses.contains_key(c))
    .map(|c| c.to_string())
    .collect();
  let mut out = json!({
    "id": p.id.to_string(),
    "title": p.title,
    "description": p.description,
    "course_ids": ids,
    "in_order": p.in_order,
    "archived": p.archived,
    "created_ms": p.created_ms,
    "updated_ms": p.updated_ms,
    "created_by_name": names.get(&p.created_by).cloned().unwrap_or_default(),
    "targets": v.targets.len(),
  });
  count.put(&mut out);
  out
}

/// (sort key, PathTarget) with the target's latest settings.
fn target_json(
  board: &Board,
  groups: &[Group],
  t: &Target,
  members: &BTreeMap<i64, Member>,
) -> Option<(String, Value)> {
  let a = t
    .asgs
    .iter()
    .copied()
    .max_by_key(|a| (a.updated_ms, a.created_ms))?;
  let name = board.target_name(a, groups);
  let mut count = Count::default();
  for uid in &t.uids {
    if let Some(m) = members.get(uid) {
      count.add(m);
    }
  }
  let created_ms = t.asgs.iter().map(|x| x.created_ms).min().unwrap_or(0);
  let mut v = json!({
    "target": { "kind": a.kind, "id": a.target, "name": name },
    "due_ms": a.due_ms,
    "due_days": a.due_days,
    "tz_minutes": a.tz,
    "required": a.required,
    "note": a.note,
    "calendar": a.calendar,
    "created_ms": created_ms,
    "updated_ms": a.updated_ms,
  });
  count.put(&mut v);
  Some((name.to_lowercase(), v))
}

/// PathPerson
fn person_json(
  uid: i64,
  p: &assign::Person,
  m: &Member,
  board: &Board,
  steps: &[Uuid],
) -> Value {
  let next = m.walk.next.and_then(|i| steps.get(i)).map(|c| {
    json!({ "course_id": c.to_string(), "title": course_title(board, *c) })
  });
  json!({
    "uid": uid.to_string(),
    "name": p.name,
    "email": p.email,
    "status": m.walk.status,
    "percent": m.walk.percent,
    "finished_courses": m.walk.done,
    "total_courses": steps.len(),
    "next": next,
    "due_ms": m.due,
    "overdue": m.overdue,
    "assigned_ms": m.assigned_ms,
  })
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

/// GET /paths (read): by title, archived ones included.
async fn list_paths(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = assign::ctx(state, user, ws).await?;
  assign::need_read(&c)?;
  let pool = &state.pg_pool;
  let board = Board::load(pool, c.ws, None).await?;
  let mut conn = pool.acquire().await?;
  let paths = paths_of(&mut conn, c.ws, None, false).await?;
  drop(conn);
  let names = assign::names_of(pool, paths.iter().map(|p| p.created_by).collect()).await?;
  let list: Vec<Value> = paths
    .iter()
    .map(|p| path_json(p, &board, &view(&board, p), &names))
    .collect();
  Ok(json!({ "paths": list }))
}

/// GET /paths/{id} (read)
async fn get_path(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = assign::ctx(state, user, ws).await?;
  assign::need_read(&c)?;
  let id = assign::parse_uuid(id, PATH_LINK_MSG)?;
  let pool = &state.pg_pool;
  let mut conn = pool.acquire().await?;
  let p = one_path(&mut conn, c.ws, id, false).await?;
  let groups = assign::groups_of(&mut conn, c.ws).await?;
  drop(conn);
  let board = Board::load(pool, c.ws, None).await?;
  let names = assign::names_of(pool, vec![p.created_by]).await?;
  let v = view(&board, &p);
  let courses: Vec<Value> = p
    .courses
    .iter()
    .filter_map(|cid| {
      let course = board.courses.get(cid)?;
      let status = if course.status == "published" {
        "published"
      } else {
        "archived"
      };
      Some(json!({ "id": cid.to_string(), "title": course.title, "status": status }))
    })
    .collect();
  let mut targets: Vec<(String, Value)> = v
    .targets
    .values()
    .filter_map(|t| target_json(&board, &groups, t, &v.members))
    .collect();
  targets.sort_by(|a, b| a.0.cmp(&b.0));
  let mut people: Vec<(String, Value)> = v
    .members
    .iter()
    .filter_map(|(uid, m)| {
      let person = board.people.get(*uid)?;
      Some((
        person.name.to_lowercase(),
        person_json(*uid, person, m, &board, &v.steps),
      ))
    })
    .collect();
  people.sort_by(|a, b| a.0.cmp(&b.0));
  Ok(json!({
    "path": path_json(&p, &board, &v, &names),
    "courses": courses,
    "targets": targets.into_iter().map(|x| x.1).collect::<Vec<Value>>(),
    "people": people.into_iter().map(|x| x.1).collect::<Vec<Value>>(),
  }))
}

/// (path, course, due_ms, due_days, days_from_ms, tz_minutes, assigned_ms,
/// renew_due_ms, required, note, updated_ms)
type MyRow = (
  Uuid,
  Uuid,
  Option<i64>,
  Option<i32>,
  i64,
  i32,
  i64,
  Option<i64>,
  bool,
  String,
  i64,
);

/// GET /paths/mine: my paths that aren't archived and have a published step.
async fn my_paths(state: &AppState, user: &UserUuid, ws: &str) -> Result<Value, AppError> {
  let c = assign::ctx(state, user, ws).await?;
  let pool = &state.pg_pool;
  let uid = c.uid();
  let rows: Vec<MyRow> = sqlx::query_as(
    "SELECT a.path_id, a.course_id, a.due_ms, a.due_days, a.days_from_ms, a.tz_minutes, \
       s.assigned_ms, s.renew_due_ms, a.required, a.note, a.updated_ms \
     FROM carehub_learning_assignee s \
     JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
     WHERE a.workspace_id = $1 AND s.uid = $2 AND a.path_id IS NOT NULL",
  )
  .bind(c.ws)
  .bind(uid)
  .fetch_all(pool)
  .await?;
  if rows.is_empty() {
    return Ok(json!({ "paths": [] }));
  }
  let mut ids: Vec<Uuid> = rows.iter().map(|r| r.0).collect();
  ids.sort();
  ids.dedup();
  let sql = format!(
    "SELECT {PATH_COLUMNS} FROM carehub_learning_path \
     WHERE workspace_id = $1 AND id = ANY($2) AND NOT archived"
  );
  let recs: Vec<PathRow> = sqlx::query_as(&sql)
    .bind(c.ws)
    .bind(ids)
    .fetch_all(pool)
    .await?;
  let paths: Vec<PathRec> = recs.into_iter().map(path_rec).collect();
  let mut course_ids: Vec<Uuid> = paths
    .iter()
    .flat_map(|p| p.courses.iter().copied())
    .collect();
  course_ids.sort();
  course_ids.dedup();
  let meta = course_meta(pool, &course_ids).await?;
  let enr: Vec<(Uuid, String, i32)> = sqlx::query_as(
    "SELECT course_id, status, percent FROM carehub_enrollment \
     WHERE uid = $1 AND course_id = ANY($2)",
  )
  .bind(uid)
  .bind(course_ids)
  .fetch_all(pool)
  .await?;
  let enr: HashMap<Uuid, (String, i32)> = enr.into_iter().map(|(k, s, p)| (k, (s, p))).collect();
  let now = assign::now_ms();
  let mut out: Vec<((u8, u8, i64, String), Value)> = Vec::new();
  for p in &paths {
    let steps: Vec<Uuid> = p
      .courses
      .iter()
      .copied()
      .filter(|k| meta.get(k).map(|m| m.0 == "published").unwrap_or(false))
      .collect();
    if steps.is_empty() {
      continue;
    }
    let mine: Vec<&MyRow> = rows.iter().filter(|r| r.0 == p.id).collect();
    let states: Vec<Option<(&str, i32)>> = steps
      .iter()
      .map(|k| enr.get(k).map(|e| (e.0.as_str(), e.1)))
      .collect();
    let w = walk_steps(&states);
    let done_all = w.status == "finished";
    let next = w.next.and_then(|i| steps.get(i)).copied();
    // P1 due_days / P2 recert: each row's own due.
    let due = next.and_then(|n| {
      mine
        .iter()
        .filter(|r| r.1 == n)
        .filter_map(|r| assign::seat_due(r.2, r.3, r.4, r.5, r.6, r.7))
        .min()
    });
    let overdue = !done_all && due.map(|d| d < now).unwrap_or(false);
    let note = mine
      .iter()
      .filter(|r| !r.9.trim().is_empty())
      .max_by_key(|r| r.10)
      .map(|r| r.9.clone())
      .unwrap_or_default();
    let title_of = |k: &Uuid| {
      meta
        .get(k)
        .map(|m| m.1.clone())
        .unwrap_or_else(|| "Untitled course".to_string())
    };
    let step_list: Vec<Value> = steps
      .iter()
      .enumerate()
      .map(|(i, k)| {
        let e = enr.get(k);
        let done = e.map(|x| assign::finished(&x.0)).unwrap_or(false);
        let percent = if done {
          100
        } else {
          e.map(|x| x.1.clamp(0, 100)).unwrap_or(0)
        };
        // in_order: not finished and after the next step (the server never
        // blocks opening a course).
        let locked = p.in_order && !done && w.next.map(|n| i > n).unwrap_or(false);
        json!({
          "course_id": k.to_string(),
          "title": title_of(k),
          "status": e.map(|x| x.0.as_str()).unwrap_or("not_started"),
          "percent": percent,
          "locked": locked,
        })
      })
      .collect();
    let next_json = next.map(|n| json!({ "course_id": n.to_string(), "title": title_of(&n) }));
    let v = json!({
      "id": p.id.to_string(),
      "title": p.title,
      "description": p.description,
      "in_order": p.in_order,
      "status": w.status,
      "percent": w.percent,
      "finished_courses": w.done,
      "total_courses": steps.len(),
      "due_ms": due,
      "overdue": overdue,
      "required": mine.iter().any(|r| r.8),
      "note": note,
      "assigned_ms": mine.iter().map(|r| r.6).min().unwrap_or(0),
      "next": next_json,
      "steps": step_list,
    });
    // Unfinished first (overdue, due soonest, no due), then by title; finished last.
    let rank: (u8, u8) = if done_all {
      (1, 0)
    } else if overdue {
      (0, 0)
    } else if due.is_some() {
      (0, 1)
    } else {
      (0, 2)
    };
    out.push((
      (rank.0, rank.1, due.unwrap_or(i64::MAX), p.title.to_lowercase()),
      v,
    ));
  }
  out.sort_by(|a, b| a.0.cmp(&b.0));
  let list: Vec<Value> = out.into_iter().map(|x| x.1).collect();
  Ok(json!({ "paths": list }))
}

/// POST /paths (edit) {title, description?, course_ids, in_order?}
async fn create_path(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = assign::ctx(state, user, ws).await?;
  assign::need_assign(&c)?;
  let v = assign::body_json(body)?;
  let title = path_title(&v)?;
  let description = path_description(&v)?.unwrap_or_default();
  let courses = course_list(&v)?;
  let in_order = assign::flag(&v, "in_order", true);
  let pool = &state.pg_pool;
  let mut tx = pool.begin().await?;
  assign::lock_ws(&mut tx, c.ws).await?;
  if title_taken(&mut tx, c.ws, &title, None).await? {
    return Err(title_clash(&title));
  }
  check_courses(&mut tx, c.ws, &courses, &[]).await?;
  let id = Uuid::new_v4();
  sqlx::query(
    "INSERT INTO carehub_learning_path (id, workspace_id, title, description, course_ids, \
       in_order, archived, created_by, created_ms, updated_ms) \
     VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, $8, $8)",
  )
  .bind(id)
  .bind(c.ws)
  .bind(&title)
  .bind(&description)
  .bind(courses)
  .bind(in_order)
  .bind(c.uid())
  .bind(assign::now_ms())
  .execute(&mut *tx)
  .await?;
  tx.commit().await?;
  Ok(json!({ "id": id.to_string() }))
}

/// Courses added to a path get an assignment for every target it has (with
/// that target's latest settings; people already told get no new message).
async fn add_courses(
  conn: &mut PgConnection,
  ws: Uuid,
  path: Uuid,
  added: &[Uuid],
  people: &assign::People,
  by: i64,
  now: i64,
) -> Result<(), AppError> {
  #[allow(clippy::type_complexity)]
  let targets: Vec<(String, String, Option<i64>, Option<i32>, i32, bool, String, bool, bool)> =
    sqlx::query_as(
      "SELECT DISTINCT ON (target_kind, target_id) target_kind, target_id, due_ms, due_days, \
         tz_minutes, required, note, calendar, notify \
       FROM carehub_learning_assignment WHERE path_id = $1 \
       ORDER BY target_kind, target_id, updated_ms DESC",
    )
    .bind(path)
    .fetch_all(&mut *conn)
    .await?;
  if targets.is_empty() {
    return Ok(());
  }
  let groups = assign::groups_of(&mut *conn, ws).await?;
  for (kind, target, due_ms, due_days, tz, required, note, calendar, notify) in targets {
    let plan = assign::Plan {
      due_ms,
      due_days,
      tz,
      required,
      note,
      calendar,
      notify,
    };
    let want = assign::want_for(&kind, &target, people, &groups);
    for course in added {
      let (asg, _) =
        assign::put_assignment(&mut *conn, ws, *course, Some(path), &kind, &target, &plan, by, now)
          .await?;
      assign::sync_assignment(&mut *conn, asg, *course, ws, notify, &want, now).await?;
    }
  }
  Ok(())
}

/// PUT /paths/{id} (edit) {title?, description?, course_ids?, in_order?,
/// archived?}. Archiving removes the path's assignments (progress is kept);
/// removed courses lose theirs; reordering touches none.
async fn update_path(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = assign::ctx(state, user, ws).await?;
  assign::need_assign(&c)?;
  let id = assign::parse_uuid(id, PATH_LINK_MSG)?;
  let v = assign::body_json(body)?;
  let title = match v.get("title") {
    Some(_) => Some(path_title(&v)?),
    None => None,
  };
  let description = path_description(&v)?;
  let course_ids = match v.get("course_ids") {
    Some(_) => Some(course_list(&v)?),
    None => None,
  };
  let pool = &state.pg_pool;
  let people = assign::people_of(pool, c.ws).await?;
  let mut tx = pool.begin().await?;
  assign::lock_ws(&mut tx, c.ws).await?;
  let p = one_path(&mut tx, c.ws, id, true).await?;
  if let Some(t) = &title {
    if title_taken(&mut tx, c.ws, t, Some(id)).await? {
      return Err(title_clash(t));
    }
  }
  if let Some(list) = &course_ids {
    check_courses(&mut tx, c.ws, list, &p.courses).await?;
  }
  let courses = course_ids.unwrap_or_else(|| p.courses.clone());
  let in_order = assign::flag(&v, "in_order", p.in_order);
  let archived = assign::flag(&v, "archived", p.archived);
  let added: Vec<Uuid> = courses
    .iter()
    .copied()
    .filter(|x| !p.courses.contains(x))
    .collect();
  let removed: Vec<Uuid> = p
    .courses
    .iter()
    .copied()
    .filter(|x| !courses.contains(x))
    .collect();
  let now = assign::now_ms();
  sqlx::query(
    "UPDATE carehub_learning_path SET title = $2, description = $3, course_ids = $4, \
       in_order = $5, archived = $6, updated_ms = $7 WHERE id = $1",
  )
  .bind(id)
  .bind(title.as_deref().unwrap_or(&p.title))
  .bind(description.as_deref().unwrap_or(&p.description))
  .bind(courses)
  .bind(in_order)
  .bind(archived)
  .bind(now)
  .execute(&mut *tx)
  .await?;
  let dropped: Vec<Uuid> = if archived {
    sqlx::query_scalar(
      "DELETE FROM carehub_learning_assignment WHERE path_id = $1 RETURNING course_id",
    )
    .bind(id)
    .fetch_all(&mut *tx)
    .await?
  } else if removed.is_empty() {
    Vec::new()
  } else {
    sqlx::query_scalar(
      "DELETE FROM carehub_learning_assignment WHERE path_id = $1 AND course_id = ANY($2) \
       RETURNING course_id",
    )
    .bind(id)
    .bind(removed.clone())
    .fetch_all(&mut *tx)
    .await?
  };
  let mut touched = dropped.clone();
  if !archived && !added.is_empty() {
    add_courses(&mut tx, c.ws, id, &added, &people, c.uid(), now).await?;
    touched.extend(added.iter().copied());
  }
  tx.commit().await?;
  assign::after_change(pool, touched);
  Ok(json!({
    "ok": true,
    "added": added.len(),
    "removed": removed.len(),
    "assignments_removed": dropped.len(),
  }))
}

/// DELETE /paths/{id} (edit): the path and its assignments (progress is kept).
async fn delete_path(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
) -> Result<Value, AppError> {
  let c = assign::ctx(state, user, ws).await?;
  assign::need_assign(&c)?;
  let id = assign::parse_uuid(id, PATH_LINK_MSG)?;
  let pool = &state.pg_pool;
  let mut tx = pool.begin().await?;
  assign::lock_ws(&mut tx, c.ws).await?;
  one_path(&mut tx, c.ws, id, true).await?;
  let dropped: Vec<Uuid> = sqlx::query_scalar(
    "DELETE FROM carehub_learning_assignment WHERE path_id = $1 RETURNING course_id",
  )
  .bind(id)
  .fetch_all(&mut *tx)
  .await?;
  sqlx::query("DELETE FROM carehub_learning_path WHERE id = $1")
    .bind(id)
    .execute(&mut *tx)
    .await?;
  tx.commit().await?;
  let n = dropped.len();
  assign::after_change(pool, dropped);
  Ok(json!({ "ok": true, "assignments_removed": n }))
}

/// POST /paths/{id}/assign (edit): per target, one assignment per path course
/// (archived ones too: they sleep like any assignment on an archived course).
/// Rules and defaults as POST /assignments; the same target again = update.
async fn assign_path(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = assign::ctx(state, user, ws).await?;
  assign::need_assign(&c)?;
  let id = assign::parse_uuid(id, PATH_LINK_MSG)?;
  let v = assign::body_json(body)?;
  let now = assign::now_ms();
  let plan = assign::plan_of(&v, now)?;
  let pool = &state.pg_pool;
  let people = assign::people_of(pool, c.ws).await?;
  let mut tx = pool.begin().await?;
  assign::lock_ws(&mut tx, c.ws).await?;
  let p = one_path(&mut tx, c.ws, id, true).await?;
  if p.archived {
    return Err(assign::bad(ARCHIVED_PATH_MSG));
  }
  let live: Vec<Uuid> = sqlx::query_scalar(
    "SELECT id FROM carehub_course WHERE workspace_id = $1 AND id = ANY($2) \
       AND published_version IS NOT NULL",
  )
  .bind(c.ws)
  .bind(p.courses.clone())
  .fetch_all(&mut *tx)
  .await?;
  let courses: Vec<Uuid> = p
    .courses
    .iter()
    .copied()
    .filter(|x| live.contains(x))
    .collect();
  if courses.is_empty() {
    return Err(assign::bad(COURSES_MSG));
  }
  let groups = assign::groups_of(&mut tx, c.ws).await?;
  let targets = assign::pick_targets(&v, &people, &groups)?;
  let mut created: i64 = 0;
  let mut updated: i64 = 0;
  let mut covered: HashSet<i64> = HashSet::new();
  for (kind, target, want) in &targets {
    let mut fresh = true;
    for course in &courses {
      let (asg, new) =
        assign::put_assignment(&mut tx, c.ws, *course, Some(id), kind, target, &plan, c.uid(), now)
          .await?;
      fresh = fresh && new;
      assign::sync_assignment(&mut tx, asg, *course, c.ws, plan.notify, want, now).await?;
    }
    if fresh {
      created += 1;
    } else {
      updated += 1;
    }
    covered.extend(want.iter().copied());
  }
  tx.commit().await?;
  assign::after_change(pool, courses);
  Ok(json!({ "created": created, "updated": updated, "people": covered.len() }))
}

/// POST /paths/{id}/unassign (edit) {targets}: their path assignments go
/// (progress is kept). removed = targets that had any.
async fn unassign_path(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  id: &str,
  body: &[u8],
) -> Result<Value, AppError> {
  let c = assign::ctx(state, user, ws).await?;
  assign::need_assign(&c)?;
  let id = assign::parse_uuid(id, PATH_LINK_MSG)?;
  let v = assign::body_json(body)?;
  let raw = assign::list(&v, "targets");
  if raw.is_empty() {
    return Err(assign::bad(NO_TARGETS_MSG));
  }
  let pool = &state.pg_pool;
  let mut tx = pool.begin().await?;
  assign::lock_ws(&mut tx, c.ws).await?;
  one_path(&mut tx, c.ws, id, true).await?;
  let mut seen: HashSet<(String, String)> = HashSet::new();
  let mut removed: i64 = 0;
  let mut touched: Vec<Uuid> = Vec::new();
  for t in raw {
    let kind = assign::txt(t, "kind").trim().to_string();
    let target = match t.get("id") {
      Some(Value::String(s)) => s.trim().to_string(),
      Some(Value::Number(n)) => n.to_string(),
      _ => String::new(),
    };
    if !seen.insert((kind.clone(), target.clone())) {
      continue;
    }
    let gone: Vec<Uuid> = sqlx::query_scalar(
      "DELETE FROM carehub_learning_assignment \
       WHERE path_id = $1 AND target_kind = $2 AND target_id = $3 RETURNING course_id",
    )
    .bind(id)
    .bind(&kind)
    .bind(&target)
    .fetch_all(&mut *tx)
    .await?;
    if !gone.is_empty() {
      removed += 1;
    }
    touched.extend(gone);
  }
  tx.commit().await?;
  assign::after_change(pool, touched);
  Ok(json!({ "ok": true, "removed": removed }))
}

// ---------------------------------------------------------------------------
// Background: path messages, reminders and calendar entries
// ---------------------------------------------------------------------------

/// One path row of a current member.
struct Row {
  course: Uuid,
  due: Option<i64>,
  tz: i32,
  required: bool,
  calendar: bool,
  assigned_ms: i64,
  sent_assigned_ms: i64,
  sent_due_ms: i64,
  sent_overdue_ms: i64,
}

/// (path, uid, workspace, course, row due, tz_minutes, required, calendar,
/// assigned_ms, sent_assigned_ms, sent_due_ms, sent_overdue_ms)
type TickRow = (
  Uuid,
  i64,
  Uuid,
  Uuid,
  Option<i64>,
  i32,
  bool,
  bool,
  i64,
  i64,
  i64,
  i64,
);

/// Marks someone's pending rows on a path (the "New learning path" message).
async fn claim_message(pool: &PgPool, path: Uuid, uid: i64, value: i64) -> Result<u64, AppError> {
  let done = sqlx::query(
    "UPDATE carehub_learning_assignee s SET sent_assigned_ms = $3 \
     FROM carehub_learning_assignment a \
     WHERE a.id = s.assignment_id AND a.path_id = $1 AND s.uid = $2 AND s.sent_assigned_ms = 0",
  )
  .bind(path)
  .bind(uid)
  .bind(value)
  .execute(pool)
  .await?;
  Ok(done.rows_affected())
}

/// Marks the due (or overdue) reminder of someone's next course on a path for
/// one due date. Sending (value > 0) first claims every row of that course with
/// that due, direct ones too, so a course never gets two reminders for one date
/// (false = it was already sent); then the path's other rows with that due, so
/// the courses after it don't get it again.
async fn claim_reminder(
  pool: &PgPool,
  path: Uuid,
  uid: i64,
  course: Uuid,
  due: i64,
  overdue: bool,
  value: i64,
) -> Result<bool, AppError> {
  let column = if overdue {
    "sent_overdue_ms"
  } else {
    "sent_due_ms"
  };
  let mut sent = false;
  if value > 0 {
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
    sent = done.rows_affected() > 0;
  }
  let sql = format!(
    "UPDATE carehub_learning_assignee s SET {column} = $4 \
     FROM carehub_learning_assignment a \
     WHERE a.id = s.assignment_id AND a.path_id = $1 AND s.uid = $2 AND {ROW_DUE} = $3 \
       AND s.{column} = 0"
  );
  sqlx::query(&sql)
    .bind(path)
    .bind(uid)
    .bind(due)
    .bind(value)
    .execute(pool)
    .await?;
  Ok(sent)
}

/// Runs after the per-course reminders (carehub_learning_assign.rs tick) and
/// after assignment changes: per person and live path, the "New learning path"
/// message, the next course's due / overdue reminders (timing as for courses)
/// and the calendar entry.
pub(crate) async fn tick_paths(pool: &PgPool) -> Result<(), AppError> {
  let now = assign::now_ms();
  let sql = format!(
    "SELECT a.path_id, s.uid, a.workspace_id, s.course_id, {ROW_DUE}, a.tz_minutes, \
       a.required, a.calendar, s.assigned_ms, s.sent_assigned_ms, s.sent_due_ms, \
       s.sent_overdue_ms \
     FROM carehub_learning_assignee s \
     JOIN carehub_learning_assignment a ON a.id = s.assignment_id \
     JOIN carehub_learning_path p ON p.id = a.path_id \
     JOIN af_workspace_member m ON m.workspace_id = a.workspace_id AND m.uid = s.uid \
     JOIN af_user u ON u.uid = s.uid \
     WHERE NOT p.archived AND m.role_id::INT <> $1 \
       AND COALESCE(u.email, '') NOT LIKE '%.removed-%'"
  );
  let raw: Vec<TickRow> = sqlx::query_as(&sql)
    .bind(assign::guest_role())
    .fetch_all(pool)
    .await?;
  let mut pairs: BTreeMap<(Uuid, i64), (Uuid, Vec<Row>)> = BTreeMap::new();
  for r in raw {
    pairs
      .entry((r.0, r.1))
      .or_insert_with(|| (r.2, Vec::new()))
      .1
      .push(Row {
        course: r.3,
        due: r.4,
        tz: r.5,
        required: r.6,
        calendar: r.7,
        assigned_ms: r.8,
        sent_assigned_ms: r.9,
        sent_due_ms: r.10,
        sent_overdue_ms: r.11,
      });
  }
  let mut ids: Vec<Uuid> = pairs.keys().map(|k| k.0).collect();
  ids.dedup();
  let found: Vec<(Uuid, String, Vec<Uuid>)> = sqlx::query_as(
    "SELECT id, title, course_ids FROM carehub_learning_path WHERE id = ANY($1) AND NOT archived",
  )
  .bind(ids)
  .fetch_all(pool)
  .await?;
  let paths: HashMap<Uuid, (String, Vec<Uuid>)> = found
    .into_iter()
    .map(|(id, title, courses)| (id, (title, courses)))
    .collect();
  let mut course_ids: Vec<Uuid> = paths
    .values()
    .flat_map(|p| p.1.iter().copied())
    .collect();
  course_ids.sort();
  course_ids.dedup();
  let meta = course_meta(pool, &course_ids).await?;
  let mut uids: Vec<i64> = pairs.keys().map(|k| k.1).collect();
  uids.sort();
  uids.dedup();
  let enr: Vec<(Uuid, i64, String, i32)> = sqlx::query_as(
    "SELECT course_id, uid, status, percent FROM carehub_enrollment \
     WHERE course_id = ANY($1) AND uid = ANY($2)",
  )
  .bind(course_ids)
  .bind(uids)
  .fetch_all(pool)
  .await?;
  let enr: HashMap<(Uuid, i64), (String, i32)> = enr
    .into_iter()
    .map(|(course, uid, status, percent)| ((course, uid), (status, percent)))
    .collect();
  let mut budget = PATH_BATCH;
  // Calendar: (path, uid) → (workspace, local start of the due day); finished
  // people's entries are left alone.
  let mut want: HashMap<(Uuid, i64), (Uuid, i64)> = HashMap::new();
  let mut keep: HashSet<(Uuid, i64)> = HashSet::new();
  for ((path, uid), (ws, rows)) in &pairs {
    let (title, courses) = match paths.get(path) {
      Some(x) => x,
      None => continue,
    };
    let steps: Vec<Uuid> = courses
      .iter()
      .copied()
      .filter(|k| meta.get(k).map(|m| m.0 == "published").unwrap_or(false))
      .collect();
    if steps.is_empty() {
      continue;
    }
    let states: Vec<Option<(&str, i32)>> = steps
      .iter()
      .map(|k| enr.get(&(*k, *uid)).map(|e| (e.0.as_str(), e.1)))
      .collect();
    let w = walk_steps(&states);
    let done_all = w.status == "finished";
    let next = w.next.and_then(|i| steps.get(i)).copied();
    // The earliest due of their rows on the next course (and its time zone).
    let due_of = |calendar_only: bool| {
      next.and_then(|n| {
        rows
          .iter()
          .filter(|r| r.course == n && (r.calendar || !calendar_only))
          .filter_map(|r| r.due.map(|d| (d, r.tz)))
          .min()
      })
    };
    let due = due_of(false);
    if done_all {
      keep.insert((*path, *uid));
    } else if let Some((d, tz)) = due_of(true) {
      want.insert((*path, *uid), (*ws, assign::day_start(assign::local_day(d, tz), tz)));
    }
    if budget == 0 {
      continue;
    }
    // "New learning path": once per person and path.
    if rows.iter().any(|r| r.sent_assigned_ms == 0) {
      budget -= 1;
      let told = rows.iter().any(|r| r.sent_assigned_ms > 0);
      if told || done_all {
        claim_message(pool, *path, *uid, -1).await?;
      } else if claim_message(pool, *path, *uid, now).await? > 0 {
        let n = steps.len();
        let due_part = due
          .map(|(d, tz)| format!(" — due {}", assign::day_label(assign::local_day(d, tz))))
          .unwrap_or_default();
        let required_part = if rows.iter().any(|r| r.required) {
          " (required)"
        } else {
          ""
        };
        let body = format!(
          "📚 New learning path for you: “{}” ({} {}){}{}.\n{}",
          title,
          n,
          if n == 1 { "course" } else { "courses" },
          due_part,
          required_part,
          assign::web_link("/app/learning")
        );
        assign::send(pool, *uid, &body).await;
      }
    }
    // Reminders: the next course only, once per due date on the path.
    let (next, (d, tz)) = match (next, due) {
      (Some(n), Some(x)) if !done_all => (n, x),
      _ => continue,
    };
    let first_ms = rows
      .iter()
      .filter(|r| r.course == next)
      .map(|r| r.assigned_ms)
      .min()
      .unwrap_or(0);
    let d_done = rows.iter().any(|r| r.due == Some(d) && r.sent_due_ms != 0);
    let o_done = rows
      .iter()
      .any(|r| r.due == Some(d) && r.sent_overdue_ms != 0);
    let day = assign::local_day(d, tz);
    let remind_from = assign::day_start(day - 1, tz) + assign::REMIND_HOUR * assign::HOUR_MS;
    let overdue_from = assign::day_start(day + 1, tz) + assign::REMIND_HOUR * assign::HOUR_MS;
    let next_title = meta
      .get(&next)
      .map(|m| m.1.as_str())
      .unwrap_or("Untitled course");
    if !d_done && now >= remind_from {
      budget = budget.saturating_sub(1);
      // Too late (already the due day) or added after the reminder time.
      if now >= assign::day_start(day, tz) || first_ms >= remind_from {
        claim_reminder(pool, *path, *uid, next, d, false, -1).await?;
      } else if claim_reminder(pool, *path, *uid, next, d, false, now).await? {
        let body = format!(
          "⏰ Reminder: “{}” is due tomorrow ({}).\n{}",
          next_title,
          assign::day_label(day),
          assign::course_link(next)
        );
        assign::send(pool, *uid, &body).await;
      }
    }
    if !o_done && now >= overdue_from {
      budget = budget.saturating_sub(1);
      // Added after the due date had passed: no overdue nagging.
      if first_ms >= d {
        claim_reminder(pool, *path, *uid, next, d, true, -1).await?;
      } else if claim_reminder(pool, *path, *uid, next, d, true, now).await? {
        let body = format!(
          "⚠️ “{}” was due {}. Please finish it when you can.\n{}",
          next_title,
          assign::day_label(day),
          assign::course_link(next)
        );
        assign::send(pool, *uid, &body).await;
      }
    }
  }
  sync_path_calendar(pool, &paths, want, &keep, now).await
}

fn path_event_id(path: Uuid, uid: i64) -> Uuid {
  Uuid::new_v5(
    &Uuid::NAMESPACE_OID,
    format!("carehub-learning-path-due:{}:{}", path, uid).as_bytes(),
  )
}

/// One all-day entry per person and path on the local path due (rows that ask
/// for calendar entries): moved when the due changes, removed when they're no
/// longer on the path, left alone once they finish it.
async fn sync_path_calendar(
  pool: &PgPool,
  paths: &HashMap<Uuid, (String, Vec<Uuid>)>,
  want: HashMap<(Uuid, i64), (Uuid, i64)>,
  keep: &HashSet<(Uuid, i64)>,
  now: i64,
) -> Result<(), AppError> {
  carehub_calendar::ensure_tables(pool).await?;
  let have: Vec<(Uuid, i64, Uuid, i64)> =
    sqlx::query_as("SELECT path_id, uid, event_id, start_ms FROM carehub_learning_path_calendar")
      .fetch_all(pool)
      .await?;
  let mut have_start: HashMap<(Uuid, i64), i64> = HashMap::new();
  for (path, uid, event, start) in &have {
    let key = (*path, *uid);
    if want.contains_key(&key) || keep.contains(&key) {
      have_start.insert(key, *start);
      continue;
    }
    sqlx::query("DELETE FROM carehub_calendar_event WHERE id = $1 AND owner = $2")
      .bind(*event)
      .bind(*uid)
      .execute(pool)
      .await?;
    sqlx::query("DELETE FROM carehub_learning_path_calendar WHERE path_id = $1 AND uid = $2")
      .bind(*path)
      .bind(*uid)
      .execute(pool)
      .await?;
  }
  let mut todo: Vec<(Uuid, i64, Uuid, i64)> = want
    .into_iter()
    .filter(|(key, (_, start))| have_start.get(key) != Some(start))
    .map(|((path, uid), (ws, start))| (path, uid, ws, start))
    .collect();
  todo.sort();
  todo.truncate(assign::CALENDAR_BATCH);
  for (path, uid, ws, start) in todo {
    let event = path_event_id(path, uid);
    let title = format!(
      "📚 Due: {}",
      paths
        .get(&path)
        .map(|p| p.0.as_str())
        .unwrap_or("Learning path")
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
    .bind(assign::clip(&title, 500))
    .bind(start)
    .bind(start + assign::DAY_MS)
    .bind("Assigned learning path in Care Hub Learning: /app/learning")
    .execute(pool)
    .await?;
    sqlx::query(
      "INSERT INTO carehub_learning_path_calendar \
         (path_id, uid, workspace_id, event_id, start_ms, updated_ms) \
       VALUES ($1, $2, $3, $4, $5, $6) \
       ON CONFLICT (path_id, uid) DO UPDATE SET event_id = EXCLUDED.event_id, \
         start_ms = EXCLUDED.start_ms, updated_ms = EXCLUDED.updated_ms",
    )
    .bind(path)
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
// HTTP handlers and routes (registered from carehub_learning_assign::configure)
// ---------------------------------------------------------------------------

async fn list_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  assign::reply(list_paths(&state, &user, &ws).await)
}

async fn create_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
  body: Bytes,
) -> Reply {
  let ws = path.into_inner();
  assign::reply(create_path(&state, &user, &ws, &body).await)
}

async fn mine_handler(user: UserUuid, state: Data<AppState>, path: web::Path<String>) -> Reply {
  let ws = path.into_inner();
  assign::reply(my_paths(&state, &user, &ws).await)
}

async fn get_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  assign::reply(get_path(&state, &user, &ws, &id).await)
}

async fn update_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  assign::reply(update_path(&state, &user, &ws, &id, &body).await)
}

async fn delete_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
) -> Reply {
  let (ws, id) = path.into_inner();
  assign::reply(delete_path(&state, &user, &ws, &id).await)
}

async fn assign_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  assign::reply(assign_path(&state, &user, &ws, &id, &body).await)
}

async fn unassign_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  body: Bytes,
) -> Reply {
  let (ws, id) = path.into_inner();
  assign::reply(unassign_path(&state, &user, &ws, &id, &body).await)
}

pub(crate) fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/paths")
        .route(web::get().to(list_handler))
        .route(web::post().to(create_handler)),
    )
    // "mine" first, so it isn't read as a path id.
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/paths/mine").route(web::get().to(mine_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/paths/{id}")
        .route(web::get().to(get_handler))
        .route(web::put().to(update_handler))
        .route(web::delete().to(delete_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/paths/{id}/assign")
        .route(web::post().to(assign_handler)),
    )
    .service(
      web::resource("/api/carehub/ws/{ws}/learning/paths/{id}/unassign")
        .route(web::post().to(unassign_handler)),
    );
}
