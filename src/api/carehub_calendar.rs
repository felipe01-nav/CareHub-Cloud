//! CareHub: personal calendar (the "Calendar" page in the sidebar).
//!
//! * Own events: stored per person in `carehub_calendar_event`. Only the owner
//!   can see or change them.
//! * Subscribed calendars (read-only): a person can add the private "iCal"
//!   link of an iCloud, Google or Outlook calendar. The link is stored on the
//!   server and never sent back to the browser; the server downloads the
//!   calendar file on request so the browser can show the events.

use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use reqwest::Url;
use serde::Deserialize;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const MAX_ICS_BYTES: usize = 15 * 1024 * 1024;
const MAX_EVENTS: i64 = 5000;
const MAX_FEEDS: i64 = 20;

static TABLES_READY: AtomicBool = AtomicBool::new(false);

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/calendar/events")
        .route(web::get().to(list_events_handler))
        .route(web::post().to(create_event_handler)),
    )
    .service(
      web::resource("/api/carehub/calendar/events/{event_id}")
        .route(web::put().to(update_event_handler))
        .route(web::delete().to(delete_event_handler)),
    )
    .service(
      web::resource("/api/carehub/calendar/feeds")
        .route(web::get().to(list_feeds_handler))
        .route(web::post().to(create_feed_handler)),
    )
    .service(
      web::resource("/api/carehub/calendar/feeds/{feed_id}/ics")
        .route(web::get().to(feed_ics_handler)),
    )
    .service(
      web::resource("/api/carehub/calendar/feeds/{feed_id}")
        .route(web::delete().to(delete_feed_handler)),
    );
}

#[derive(Debug, Deserialize)]
struct RangeQuery {
  from: Option<i64>,
  to: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct EventBody {
  #[serde(default)]
  title: String,
  start: i64,
  end: i64,
  #[serde(default)]
  all_day: bool,
  #[serde(default)]
  location: String,
  #[serde(default)]
  notes: String,
  #[serde(default)]
  color: String,
}

#[derive(Debug, Deserialize)]
struct FeedBody {
  #[serde(default)]
  name: String,
  url: String,
  #[serde(default)]
  color: String,
}

fn internal(msg: String) -> AppError {
  AppError::Internal(anyhow::anyhow!(msg))
}

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn clip(s: &str, max: usize) -> String {
  s.chars().take(max).collect()
}

async fn ensure_tables(pool: &sqlx::PgPool) -> Result<(), AppError> {
  if TABLES_READY.load(Ordering::Relaxed) {
    return Ok(());
  }
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_calendar_event (\
       id UUID PRIMARY KEY,\
       owner BIGINT NOT NULL,\
       title TEXT NOT NULL DEFAULT '',\
       start_ms BIGINT NOT NULL,\
       end_ms BIGINT NOT NULL,\
       all_day BOOLEAN NOT NULL DEFAULT FALSE,\
       location TEXT NOT NULL DEFAULT '',\
       notes TEXT NOT NULL DEFAULT '',\
       color TEXT NOT NULL DEFAULT '',\
       created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\
       updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    "CREATE INDEX IF NOT EXISTS carehub_calendar_event_owner_start \
     ON carehub_calendar_event (owner, start_ms)",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_calendar_feed (\
       id UUID PRIMARY KEY,\
       owner BIGINT NOT NULL,\
       name TEXT NOT NULL DEFAULT '',\
       url TEXT NOT NULL,\
       color TEXT NOT NULL DEFAULT '',\
       created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
  )
  .execute(pool)
  .await?;
  TABLES_READY.store(true, Ordering::Relaxed);
  Ok(())
}

async fn owner_uid(state: &AppState, user_uuid: &UserUuid) -> Result<i64, AppError> {
  ensure_tables(&state.pg_pool).await?;
  let uid = state.user_cache.get_user_uid(user_uuid).await?;
  Ok(uid)
}

fn check_event(body: &EventBody) -> Result<(), AppError> {
  // Roughly the years 1970 - 2200.
  let max = 7_258_118_400_000_i64;
  if body.start < 0 || body.end < 0 || body.start > max || body.end > max {
    return Err(bad("The date is out of range"));
  }
  if body.end <= body.start {
    return Err(bad("The end must be after the start"));
  }
  Ok(())
}

type EventRow = (Uuid, String, i64, i64, bool, String, String, String);

fn event_json(row: &EventRow) -> Value {
  json!({
    "id": row.0,
    "title": row.1,
    "start": row.2,
    "end": row.3,
    "all_day": row.4,
    "location": row.5,
    "notes": row.6,
    "color": row.7,
  })
}

/// GET /api/carehub/calendar/events?from=&to= (milliseconds)
async fn list_events_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  query: web::Query<RangeQuery>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let from = query.from.unwrap_or(0);
  let to = query.to.unwrap_or(i64::MAX);
  let rows: Vec<EventRow> = sqlx::query_as(
    "SELECT id, title, start_ms, end_ms, all_day, location, notes, color \
     FROM carehub_calendar_event \
     WHERE owner = $1 AND start_ms < $3 AND end_ms > $2 \
     ORDER BY start_ms LIMIT $4",
  )
  .bind(uid)
  .bind(from)
  .bind(to)
  .bind(MAX_EVENTS)
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  let events: Vec<Value> = rows.iter().map(event_json).collect();
  Ok(Json(AppResponse::Ok().with_data(json!({ "events": events }))))
}

/// POST /api/carehub/calendar/events
async fn create_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<EventBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  check_event(&body)?;
  let id = Uuid::new_v4();
  let row: EventRow = sqlx::query_as(
    "INSERT INTO carehub_calendar_event \
       (id, owner, title, start_ms, end_ms, all_day, location, notes, color) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
     RETURNING id, title, start_ms, end_ms, all_day, location, notes, color",
  )
  .bind(id)
  .bind(uid)
  .bind(clip(body.title.trim(), 500))
  .bind(body.start)
  .bind(body.end)
  .bind(body.all_day)
  .bind(clip(body.location.trim(), 1000))
  .bind(clip(&body.notes, 20000))
  .bind(clip(body.color.trim(), 32))
  .fetch_one(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(event_json(&row))))
}

/// PUT /api/carehub/calendar/events/{event_id}
async fn update_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
  body: Json<EventBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  check_event(&body)?;
  let event_id = path.into_inner();
  let row: Option<EventRow> = sqlx::query_as(
    "UPDATE carehub_calendar_event SET \
       title = $3, start_ms = $4, end_ms = $5, all_day = $6, location = $7, \
       notes = $8, color = $9, updated_at = NOW() \
     WHERE id = $1 AND owner = $2 \
     RETURNING id, title, start_ms, end_ms, all_day, location, notes, color",
  )
  .bind(event_id)
  .bind(uid)
  .bind(clip(body.title.trim(), 500))
  .bind(body.start)
  .bind(body.end)
  .bind(body.all_day)
  .bind(clip(body.location.trim(), 1000))
  .bind(clip(&body.notes, 20000))
  .bind(clip(body.color.trim(), 32))
  .fetch_optional(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  match row {
    Some(row) => Ok(Json(AppResponse::Ok().with_data(event_json(&row)))),
    None => Err(AppError::RecordNotFound("Event not found".to_string()).into()),
  }
}

/// DELETE /api/carehub/calendar/events/{event_id}
async fn delete_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let event_id = path.into_inner();
  sqlx::query("DELETE FROM carehub_calendar_event WHERE id = $1 AND owner = $2")
    .bind(event_id)
    .bind(uid)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "deleted": true }))))
}

// ---------------------------------------------------------------------------
// Subscribed (read-only) calendars
// ---------------------------------------------------------------------------

fn allowed_host(host: &str) -> bool {
  let host = host.trim_end_matches('.').to_lowercase();
  host == "icloud.com"
    || host.ends_with(".icloud.com")
    || host == "calendar.google.com"
    || host == "outlook.office365.com"
    || host == "outlook.live.com"
    || host == "outlook.office.com"
}

/// Turns what the person pasted into a safe https link, or explains the problem.
fn parse_feed_url(raw: &str) -> Result<Url, AppError> {
  let mut text = raw.trim().to_string();
  if text.to_lowercase().starts_with("webcal://") {
    text = format!("https://{}", &text[9..]);
  }
  let url = Url::parse(&text).map_err(|_| bad("This doesn't look like a calendar link"))?;
  if url.scheme() != "https" {
    return Err(bad("The calendar link must start with https:// or webcal://"));
  }
  let host = url.host_str().unwrap_or_default();
  if !allowed_host(host) {
    return Err(bad(
      "For now only iCloud, Google and Outlook calendar links are supported",
    ));
  }
  if url.port().is_some() {
    return Err(bad("This doesn't look like a calendar link"));
  }
  Ok(url)
}

async fn download_ics(url: &Url) -> Result<String, AppError> {
  let policy = reqwest::redirect::Policy::custom(|attempt| {
    if attempt.previous().len() >= 5 {
      return attempt.error("too many redirects");
    }
    let ok = attempt.url().scheme() == "https"
      && attempt.url().port().is_none()
      && attempt.url().host_str().map(allowed_host).unwrap_or(false);
    if ok {
      attempt.follow()
    } else {
      attempt.stop()
    }
  });
  let client = reqwest::Client::builder()
    .timeout(Duration::from_secs(20))
    .redirect(policy)
    .user_agent("CareHub-Calendar/1.0")
    .build()
    .map_err(|e| internal(format!("calendar client: {}", e)))?;
  let mut resp = client
    .get(url.clone())
    .header("Accept", "text/calendar, */*")
    .send()
    .await
    .map_err(|_| bad("Could not reach the calendar. Please check the link."))?;
  if !resp.status().is_success() {
    return Err(bad(&format!(
      "The calendar service answered with an error ({}). Please check the link.",
      resp.status().as_u16()
    )));
  }
  if let Some(len) = resp.content_length() {
    if len as usize > MAX_ICS_BYTES {
      return Err(bad("This calendar is too big to show"));
    }
  }
  let mut data: Vec<u8> = Vec::new();
  while let Some(chunk) = resp
    .chunk()
    .await
    .map_err(|_| bad("Could not download the calendar. Please try again."))?
  {
    if data.len() + chunk.len() > MAX_ICS_BYTES {
      return Err(bad("This calendar is too big to show"));
    }
    data.extend_from_slice(&chunk);
  }
  let text = String::from_utf8_lossy(&data).to_string();
  if !text.contains("BEGIN:VCALENDAR") {
    return Err(bad(
      "This link doesn't give a calendar. Please copy the public/iCal link again.",
    ));
  }
  Ok(text)
}

fn host_of(url: &str) -> String {
  Url::parse(url)
    .ok()
    .and_then(|u| u.host_str().map(|h| h.to_string()))
    .unwrap_or_default()
}

/// GET /api/carehub/calendar/feeds -> { feeds: [{ id, name, color, host }] }
async fn list_feeds_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let rows: Vec<(Uuid, String, String, String)> = sqlx::query_as(
    "SELECT id, name, url, color FROM carehub_calendar_feed WHERE owner = $1 ORDER BY created_at",
  )
  .bind(uid)
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  let feeds: Vec<Value> = rows
    .iter()
    .map(|(id, name, url, color)| {
      json!({ "id": id, "name": name, "color": color, "host": host_of(url) })
    })
    .collect();
  Ok(Json(AppResponse::Ok().with_data(json!({ "feeds": feeds }))))
}

/// POST /api/carehub/calendar/feeds { name, url, color }
async fn create_feed_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<FeedBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM carehub_calendar_feed WHERE owner = $1")
    .bind(uid)
    .fetch_one(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  if count >= MAX_FEEDS {
    return Err(bad("You have reached the maximum number of calendars").into());
  }
  let url = parse_feed_url(&body.url)?;
  // Check the link works before saving it.
  download_ics(&url).await?;
  let name = {
    let n = clip(body.name.trim(), 200);
    if n.is_empty() {
      "My calendar".to_string()
    } else {
      n
    }
  };
  let id = Uuid::new_v4();
  sqlx::query(
    "INSERT INTO carehub_calendar_feed (id, owner, name, url, color) VALUES ($1, $2, $3, $4, $5)",
  )
  .bind(id)
  .bind(uid)
  .bind(&name)
  .bind(url.as_str())
  .bind(clip(body.color.trim(), 32))
  .execute(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "id": id,
    "name": name,
    "color": clip(body.color.trim(), 32),
    "host": url.host_str().unwrap_or_default(),
  }))))
}

/// GET /api/carehub/calendar/feeds/{feed_id}/ics -> { ics: "BEGIN:VCALENDAR..." }
async fn feed_ics_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let feed_id = path.into_inner();
  let stored: Option<String> =
    sqlx::query_scalar("SELECT url FROM carehub_calendar_feed WHERE id = $1 AND owner = $2")
      .bind(feed_id)
      .bind(uid)
      .fetch_optional(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
  let stored = stored.ok_or_else(|| AppError::RecordNotFound("Calendar not found".to_string()))?;
  let url = parse_feed_url(&stored)?;
  let ics = download_ics(&url).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "ics": ics }))))
}

/// DELETE /api/carehub/calendar/feeds/{feed_id}
async fn delete_feed_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let feed_id = path.into_inner();
  sqlx::query("DELETE FROM carehub_calendar_feed WHERE id = $1 AND owner = $2")
    .bind(feed_id)
    .bind(uid)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "deleted": true }))))
}
