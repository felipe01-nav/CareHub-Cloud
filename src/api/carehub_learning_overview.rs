//! CareHub Learning: the Reports "Overview" numbers.
//!
//! GET /api/carehub/ws/{ws}/learning/reports/overview?days=7|30|90&tz_minutes=N
//!
//! Read-only, no tables of its own. Headline numbers for the last `days` local
//! days (today included) and for the same-length period just before it, all
//! counted in SQL over the workspace's current non-guest members (the same
//! "members" as the other reports). Permission: learning_build or
//! learning_analytics, like the other reports.
//!
//! - active: people with any learning activity: a stored event (except
//!   "reset", which an admin or a renewal logs under the learner), a final-quiz
//!   attempt, or a course whose last activity time falls in the period
//!   (heartbeats and repeated steps leave no event row).
//! - finished: "status" events that move a person's course to completed or
//!   passed from anything else (they survive progress resets).
//! - attempts / passed: final-quiz attempts. certificates: certificates issued.

use crate::api::carehub_learning;
use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use database_entity::dto::AFRole;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use std::collections::HashMap;
use uuid::Uuid;

const DAY_MS: i64 = 86_400_000;
const TOP_COURSES: usize = 5;

const REPORTS_MSG: &str =
  "You need the \"See course reports\" permission for this. Ask an admin to update your role.";
const DAYS_MSG: &str = "Pick 7, 30 or 90 days.";
const SERVER_MSG: &str = "Something went wrong on the server. Please try again in a moment.";

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
      tracing::warn!("carehub learning overview: {}", other);
      bad(SERVER_MSG)
    },
  }
}

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

/// 7, 30 or 90; missing (or blank) means 7.
fn parse_days(raw: Option<&str>) -> Result<i64, AppError> {
  match raw.map(str::trim) {
    None | Some("") | Some("7") => Ok(7),
    Some("30") => Ok(30),
    Some("90") => Ok(90),
    Some(_) => Err(bad(DAYS_MSG)),
  }
}

/// Minutes east of UTC, read like assignments do: clamped to ±840, and
/// anything missing or unreadable counts as 0.
fn parse_tz(raw: Option<&str>) -> i32 {
  raw
    .map(str::trim)
    .and_then(|t| {
      t.parse::<i64>()
        .ok()
        .or_else(|| t.parse::<f64>().ok().filter(|f| f.is_finite()).map(|f| f as i64))
    })
    .map(|m| m.clamp(-840, 840) as i32)
    .unwrap_or(0)
}

/// The period shown and the one just before it, in ms since the epoch.
/// Local days use a fixed offset, as assignments do.
struct Period {
  days: i64,
  prev_since: i64,
  since: i64,
  until: i64,
}

impl Period {
  fn new(days: i64, tz: i32, now: i64) -> Period {
    let shift = i64::from(tz) * 60_000;
    let today = (now + shift).div_euclid(DAY_MS);
    let since = (today - days + 1) * DAY_MS - shift;
    let until = (today + 1) * DAY_MS - shift;
    Period {
      days,
      prev_since: since - days * DAY_MS,
      since,
      until,
    }
  }
}

/// GET /reports/overview
async fn overview(
  state: &AppState,
  user: &UserUuid,
  ws: &str,
  query: &HashMap<String, String>,
) -> Result<Value, AppError> {
  let c = carehub_learning::ctx(state, user, ws).await?;
  if !(c.has("learning_build") || c.has("learning_analytics")) {
    return Err(bad(REPORTS_MSG));
  }
  let days = parse_days(query.get("days").map(|s| s.as_str()))?;
  let tz = parse_tz(query.get("tz_minutes").map(|s| s.as_str()));
  let p = Period::new(days, tz, chrono::Utc::now().timestamp_millis());
  let guest: i32 = AFRole::Guest.into();
  let pool = &state.pg_pool;

  // Binds everywhere: $1 workspace, $2 guest role, $3 start of the previous
  // period, $4 start of this period, $5 end of this period (exclusive).
  #[allow(clippy::type_complexity)]
  let totals: (i64, i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
    "WITH mem AS ( \
       SELECT m.uid FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
       WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
         AND COALESCE(u.email, '') NOT LIKE '%.removed-%'), \
     crs AS (SELECT id FROM carehub_course WHERE workspace_id = $1), \
     act AS ( \
       SELECT x.uid, x.course_id, x.t FROM ( \
         SELECT e.uid, e.course_id, e.ts_ms AS t FROM carehub_learning_event e \
         WHERE e.workspace_id = $1 AND e.ts_ms >= $3 AND e.ts_ms < $5 AND e.kind <> 'reset' \
         UNION ALL \
         SELECT a.uid, a.course_id, a.created_ms FROM carehub_quiz_attempt a \
         WHERE a.course_id IN (SELECT id FROM crs) AND a.created_ms >= $3 AND a.created_ms < $5 \
         UNION ALL \
         SELECT n.uid, n.course_id, n.last_ms FROM carehub_enrollment n \
         WHERE n.course_id IN (SELECT id FROM crs) AND n.last_ms >= $3 AND n.last_ms < $5 \
       ) x WHERE x.uid IN (SELECT uid FROM mem)), \
     fin AS ( \
       SELECT e.uid, e.course_id, e.ts_ms AS t FROM carehub_learning_event e \
       WHERE e.workspace_id = $1 AND e.ts_ms >= $3 AND e.ts_ms < $5 AND e.kind = 'status' \
         AND e.data->>'to' IN ('completed', 'passed') \
         AND COALESCE(e.data->>'from', '') NOT IN ('completed', 'passed') \
         AND e.uid IN (SELECT uid FROM mem)), \
     att AS ( \
       SELECT a.created_ms AS t, a.passed FROM carehub_quiz_attempt a \
       WHERE a.course_id IN (SELECT id FROM crs) AND a.created_ms >= $3 AND a.created_ms < $5 \
         AND a.uid IN (SELECT uid FROM mem)) \
     SELECT \
       (SELECT COUNT(DISTINCT uid) FROM act WHERE t >= $4)::BIGINT, \
       (SELECT COUNT(DISTINCT uid) FROM act WHERE t < $4)::BIGINT, \
       (SELECT COUNT(*) FROM fin WHERE t >= $4)::BIGINT, \
       (SELECT COUNT(*) FROM fin WHERE t < $4)::BIGINT, \
       (SELECT COUNT(*) FROM att WHERE t >= $4)::BIGINT, \
       (SELECT COUNT(*) FROM att WHERE t >= $4 AND passed)::BIGINT, \
       (SELECT COUNT(*) FROM att WHERE t < $4)::BIGINT, \
       (SELECT COUNT(*) FROM att WHERE t < $4 AND passed)::BIGINT, \
       (SELECT COUNT(*) FROM mem)::BIGINT",
  )
  .bind(c.ws)
  .bind(guest)
  .bind(p.prev_since)
  .bind(p.since)
  .bind(p.until)
  .fetch_one(pool)
  .await?;

  // Certificates live in the assignments module's table. Until that module
  // has made its tables there are no certificates, so a failure counts as 0.
  let certs: Result<(i64, i64), sqlx::Error> = sqlx::query_as(
    "SELECT (COUNT(*) FILTER (WHERE k.issued_ms >= $4))::BIGINT, \
       (COUNT(*) FILTER (WHERE k.issued_ms < $4))::BIGINT \
     FROM carehub_learning_certificate k \
     WHERE k.workspace_id = $1 AND k.issued_ms >= $3 AND k.issued_ms < $5 \
       AND k.uid IN ( \
         SELECT m.uid FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
         WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
           AND COALESCE(u.email, '') NOT LIKE '%.removed-%')",
  )
  .bind(c.ws)
  .bind(guest)
  .bind(p.prev_since)
  .bind(p.since)
  .bind(p.until)
  .fetch_one(pool)
  .await;
  let (certificates, certificates_prev) = match certs {
    Ok(row) => row,
    Err(err) => {
      tracing::warn!("carehub learning overview: certificates: {}", err);
      (0, 0)
    },
  };

  // Per local day of this period: series 0 = active people, 1 = finishes.
  let per_day: Vec<(i32, i64, i64)> = sqlx::query_as(
    "WITH mem AS ( \
       SELECT m.uid FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
       WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
         AND COALESCE(u.email, '') NOT LIKE '%.removed-%'), \
     crs AS (SELECT id FROM carehub_course WHERE workspace_id = $1), \
     act AS ( \
       SELECT x.uid, x.course_id, x.t FROM ( \
         SELECT e.uid, e.course_id, e.ts_ms AS t FROM carehub_learning_event e \
         WHERE e.workspace_id = $1 AND e.ts_ms >= $3 AND e.ts_ms < $5 AND e.kind <> 'reset' \
         UNION ALL \
         SELECT a.uid, a.course_id, a.created_ms FROM carehub_quiz_attempt a \
         WHERE a.course_id IN (SELECT id FROM crs) AND a.created_ms >= $3 AND a.created_ms < $5 \
         UNION ALL \
         SELECT n.uid, n.course_id, n.last_ms FROM carehub_enrollment n \
         WHERE n.course_id IN (SELECT id FROM crs) AND n.last_ms >= $3 AND n.last_ms < $5 \
       ) x WHERE x.uid IN (SELECT uid FROM mem)), \
     fin AS ( \
       SELECT e.uid, e.course_id, e.ts_ms AS t FROM carehub_learning_event e \
       WHERE e.workspace_id = $1 AND e.ts_ms >= $3 AND e.ts_ms < $5 AND e.kind = 'status' \
         AND e.data->>'to' IN ('completed', 'passed') \
         AND COALESCE(e.data->>'from', '') NOT IN ('completed', 'passed') \
         AND e.uid IN (SELECT uid FROM mem)) \
     SELECT 0, (t - $4) / 86400000, COUNT(DISTINCT uid)::BIGINT FROM act WHERE t >= $4 GROUP BY 2 \
     UNION ALL \
     SELECT 1, (t - $4) / 86400000, COUNT(*)::BIGINT FROM fin WHERE t >= $4 GROUP BY 2",
  )
  .bind(c.ws)
  .bind(guest)
  .bind(p.prev_since)
  .bind(p.since)
  .bind(p.until)
  .fetch_all(pool)
  .await?;

  // Most active courses in this period (archived ones count too).
  let top: Vec<(Uuid, String, String, i64, i64)> = sqlx::query_as(
    "WITH mem AS ( \
       SELECT m.uid FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
       WHERE m.workspace_id = $1 AND m.role_id::INT <> $2 \
         AND COALESCE(u.email, '') NOT LIKE '%.removed-%'), \
     crs AS (SELECT id FROM carehub_course WHERE workspace_id = $1), \
     act AS ( \
       SELECT x.uid, x.course_id, x.t FROM ( \
         SELECT e.uid, e.course_id, e.ts_ms AS t FROM carehub_learning_event e \
         WHERE e.workspace_id = $1 AND e.ts_ms >= $3 AND e.ts_ms < $5 AND e.kind <> 'reset' \
         UNION ALL \
         SELECT a.uid, a.course_id, a.created_ms FROM carehub_quiz_attempt a \
         WHERE a.course_id IN (SELECT id FROM crs) AND a.created_ms >= $3 AND a.created_ms < $5 \
         UNION ALL \
         SELECT n.uid, n.course_id, n.last_ms FROM carehub_enrollment n \
         WHERE n.course_id IN (SELECT id FROM crs) AND n.last_ms >= $3 AND n.last_ms < $5 \
       ) x WHERE x.uid IN (SELECT uid FROM mem)), \
     fin AS ( \
       SELECT e.uid, e.course_id, e.ts_ms AS t FROM carehub_learning_event e \
       WHERE e.workspace_id = $1 AND e.ts_ms >= $3 AND e.ts_ms < $5 AND e.kind = 'status' \
         AND e.data->>'to' IN ('completed', 'passed') \
         AND COALESCE(e.data->>'from', '') NOT IN ('completed', 'passed') \
         AND e.uid IN (SELECT uid FROM mem)), \
     per AS ( \
       SELECT course_id, COUNT(DISTINCT uid) AS active, 0::BIGINT AS finished \
       FROM act WHERE t >= $4 GROUP BY course_id \
       UNION ALL \
       SELECT course_id, 0::BIGINT, COUNT(*) FROM fin WHERE t >= $4 GROUP BY course_id) \
     SELECT c.id, \
       COALESCE((SELECT NULLIF(BTRIM(v.content->>'title'), '') FROM carehub_course_version v \
         WHERE v.course_id = c.id AND v.version = c.published_version), \
         NULLIF(c.title, ''), 'Untitled course'), \
       c.status, SUM(p.active)::BIGINT, SUM(p.finished)::BIGINT \
     FROM per p JOIN carehub_course c ON c.id = p.course_id \
     WHERE c.workspace_id = $1 \
     GROUP BY c.id ORDER BY 4 DESC, 5 DESC, 2, 1 LIMIT 5",
  )
  .bind(c.ws)
  .bind(guest)
  .bind(p.prev_since)
  .bind(p.since)
  .bind(p.until)
  .fetch_all(pool)
  .await?;

  let mut slots: Vec<(i64, i64)> = vec![(0, 0); usize::try_from(p.days).unwrap_or(0)];
  for (series, day, n) in per_day {
    let i = match usize::try_from(day) {
      Ok(i) => i,
      Err(_) => continue,
    };
    if let Some(slot) = slots.get_mut(i) {
      if series == 0 {
        slot.0 = n;
      } else {
        slot.1 = n;
      }
    }
  }
  let by_day: Vec<Value> = slots
    .iter()
    .enumerate()
    .map(|(i, (active, finished))| {
      json!({
        "day_ms": p.since + (i as i64) * DAY_MS,
        "active": active,
        "finished": finished,
      })
    })
    .collect();
  let top_courses: Vec<Value> = top
    .iter()
    .take(TOP_COURSES)
    .map(|r| {
      json!({
        "course_id": r.0.to_string(),
        "title": r.1,
        "status": r.2,
        "active": r.3,
        "finished": r.4,
      })
    })
    .collect();

  Ok(json!({
    "days": p.days,
    "since_ms": p.since,
    "until_ms": p.until,
    "members": totals.8,
    "active": totals.0,
    "active_prev": totals.1,
    "finished": totals.2,
    "finished_prev": totals.3,
    "attempts": totals.4,
    "passed": totals.5,
    "attempts_prev": totals.6,
    "passed_prev": totals.7,
    "certificates": certificates,
    "certificates_prev": certificates_prev,
    "by_day": by_day,
    "top_courses": top_courses,
  }))
}

pub(crate) async fn overview_handler(
  user: UserUuid,
  state: Data<AppState>,
  path: web::Path<String>,
  query: web::Query<HashMap<String, String>>,
) -> Reply {
  let ws = path.into_inner();
  reply(overview(&state, &user, &ws, &query).await)
}
