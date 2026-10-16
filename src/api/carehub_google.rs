//! CareHub: private Google calendars (two-way), the way Notion Calendar does it.
//!
//! The person clicks "Connect Google Calendar", signs in on Google's own page
//! and allows Care Hub to use their calendar. Google then sends them back here
//! and the server keeps a "refresh token" so it can read and change events.
//! The calendar does not need to be public.
//!
//! * Needs a Google OAuth client (CAREHUB_GOOGLE_CLIENT_ID and
//!   CAREHUB_GOOGLE_CLIENT_SECRET) and CAREHUB_SECRET_KEY.
//! * The refresh token is stored encrypted (pgcrypto, CAREHUB_SECRET_KEY) and
//!   is never sent to the browser.
//! * Requests only go to Google's calendar API over https.
//! * Each person can only see and change their own connection.
//! * The browser gets the same shapes as the iCloud routes (events as iCalendar
//!   text), so the Calendar page shows both the same way.

use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::{HttpResponse, Result as ActixResult};
use app_error::AppError;
use reqwest::Url;
use serde::Deserialize;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use uuid::Uuid;

const AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const REVOKE_URL: &str = "https://oauth2.googleapis.com/revoke";
const API_BASE: &str = "https://www.googleapis.com/calendar/v3/";
const CALENDARS_BASE: &str = "https://www.googleapis.com/calendar/v3/calendars/";
const SCOPE: &str = "https://www.googleapis.com/auth/calendar";
const CALLBACK_PATH: &str = "/api/carehub/calendar/google/callback";
const STATE_TTL_MS: i64 = 15 * 60 * 1000;
const MAX_CALENDARS: usize = 30;
const MAX_EVENTS: usize = 5000;

static TABLE_READY: AtomicBool = AtomicBool::new(false);

/// Short-lived access tokens, kept in memory: owner -> (token, expires at ms).
fn token_cache() -> &'static Mutex<HashMap<i64, (String, i64)>> {
  static CACHE: OnceLock<Mutex<HashMap<i64, (String, i64)>>> = OnceLock::new();
  CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/calendar/google")
        .route(web::get().to(status_handler))
        .route(web::delete().to(disconnect_handler)),
    )
    .service(web::resource("/api/carehub/calendar/google/connect").route(web::post().to(connect_handler)))
    .service(web::resource(CALLBACK_PATH).route(web::get().to(callback_handler)))
    .service(
      web::resource("/api/carehub/calendar/google/events")
        .route(web::get().to(list_events_handler))
        .route(web::post().to(create_event_handler))
        .route(web::put().to(update_event_handler)),
    )
    .service(
      web::resource("/api/carehub/calendar/google/events/delete").route(web::post().to(delete_event_handler)),
    );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn now_ms() -> i64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map(|d| d.as_millis() as i64)
    .unwrap_or(0)
}

fn env(name: &str) -> Option<String> {
  std::env::var(name)
    .ok()
    .map(|v| v.trim().to_string())
    .filter(|v| !v.is_empty())
}

struct Settings {
  client_id: String,
  client_secret: String,
  key: String,
}

fn settings() -> Option<Settings> {
  let key = env("CAREHUB_SECRET_KEY")?;
  if key.len() < 32 {
    return None;
  }
  Some(Settings {
    client_id: env("CAREHUB_GOOGLE_CLIENT_ID")?,
    client_secret: env("CAREHUB_GOOGLE_CLIENT_SECRET")?,
    key,
  })
}

fn require_settings() -> Result<Settings, AppError> {
  settings().ok_or_else(|| {
    bad("Google Calendar is not switched on for this Care Hub yet (the Google sign-in settings are missing).")
  })
}

fn client() -> reqwest::Client {
  reqwest::Client::builder()
    .timeout(Duration::from_secs(25))
    .user_agent("CareHub-Calendar/1.0")
    .build()
    .unwrap_or_else(|_| reqwest::Client::new())
}

async fn owner_uid(state: &AppState, user_uuid: &UserUuid) -> Result<i64, AppError> {
  ensure_tables(&state.pg_pool).await?;
  Ok(state.user_cache.get_user_uid(user_uuid).await?)
}

async fn ensure_tables(pool: &sqlx::PgPool) -> Result<(), AppError> {
  if TABLE_READY.load(Ordering::Relaxed) {
    return Ok(());
  }
  sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto").execute(pool).await?;
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_google_account (\
       owner BIGINT PRIMARY KEY,\
       email TEXT NOT NULL DEFAULT '',\
       secret BYTEA NOT NULL,\
       created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_google_oauth_state (\
       state TEXT PRIMARY KEY,\
       owner BIGINT NOT NULL,\
       origin TEXT NOT NULL,\
       created_ms BIGINT NOT NULL)",
  )
  .execute(pool)
  .await?;
  TABLE_READY.store(true, Ordering::Relaxed);
  Ok(())
}

/// The address Care Hub is opened on, e.g. https://name.tailnet.ts.net
/// (http is only allowed for this computer itself).
fn clean_origin(raw: &str) -> Option<String> {
  let url = Url::parse(raw.trim()).ok()?;
  let host = url.host_str()?.to_ascii_lowercase();
  match url.scheme() {
    "https" => {},
    "http" if host == "localhost" || host == "127.0.0.1" => {},
    _ => return None,
  }
  if !url.username().is_empty() || url.password().is_some() {
    return None;
  }
  let mut origin = format!("{}://{}", url.scheme(), host);
  if let Some(port) = url.port() {
    origin.push_str(&format!(":{}", port));
  }
  Some(origin)
}

fn redirect_uri(origin: &str) -> String {
  format!("{}{}", origin, CALLBACK_PATH)
}

fn google_error(status: u16, body: &Value) -> AppError {
  let detail = body["error"]["message"]
    .as_str()
    .or_else(|| body["error_description"].as_str())
    .unwrap_or("")
    .to_string();
  match status {
    401 => bad("Google did not accept the connection. Please connect Google Calendar again."),
    403 => {
      if detail.is_empty() {
        bad("Google refused this change (the calendar may be read-only).")
      } else {
        bad(&format!("Google refused this change: {}", detail))
      }
    },
    404 | 410 => bad("This event no longer exists in Google Calendar."),
    412 => bad("The event was changed somewhere else. Please reload and try again."),
    429 => bad("Google Calendar is busy. Please try again in a minute."),
    _ => {
      if detail.is_empty() {
        bad(&format!("Google Calendar error {}.", status))
      } else {
        bad(&format!("Google Calendar error {}: {}", status, detail))
      }
    },
  }
}

// ---------------------------------------------------------------------------
// tokens
// ---------------------------------------------------------------------------

async fn load_refresh_token(state: &AppState, uid: i64, key: &str) -> Result<Option<(String, String)>, AppError> {
  let row: Option<(String, String)> = sqlx::query_as(
    "SELECT email, pgp_sym_decrypt(secret, $2) FROM carehub_google_account WHERE owner = $1",
  )
  .bind(uid)
  .bind(key)
  .fetch_optional(&state.pg_pool)
  .await
  .map_err(|_| bad("Could not read the Google connection. Please connect Google Calendar again."))?;
  Ok(row)
}

struct Account {
  email: String,
  token: String,
}

/// A working access token for this person (refreshed when needed).
async fn account(state: &AppState, uid: i64, force_refresh: bool) -> Result<Option<Account>, AppError> {
  let s = require_settings()?;
  let (email, refresh) = match load_refresh_token(state, uid, &s.key).await? {
    Some(row) => row,
    None => return Ok(None),
  };
  if !force_refresh {
    if let Ok(cache) = token_cache().lock() {
      if let Some((token, expires)) = cache.get(&uid) {
        if *expires > now_ms() + 60_000 {
          return Ok(Some(Account { email, token: token.clone() }));
        }
      }
    }
  }
  let resp = client()
    .post(TOKEN_URL)
    .form(&[
      ("client_id", s.client_id.as_str()),
      ("client_secret", s.client_secret.as_str()),
      ("refresh_token", refresh.as_str()),
      ("grant_type", "refresh_token"),
    ])
    .send()
    .await
    .map_err(|e| bad(&format!("Google could not be reached: {}", e)))?;
  let status = resp.status().as_u16();
  let body: Value = resp.json().await.unwrap_or(Value::Null);
  if !(200..300).contains(&status) {
    if body["error"].as_str() == Some("invalid_grant") {
      return Err(bad(
        "Google access was removed or has expired. Please disconnect and connect Google Calendar again.",
      ));
    }
    return Err(google_error(status, &body));
  }
  let token = body["access_token"].as_str().unwrap_or_default().to_string();
  if token.is_empty() {
    return Err(bad("Google did not send an access token. Please connect Google Calendar again."));
  }
  let expires = now_ms() + body["expires_in"].as_i64().unwrap_or(3000) * 1000;
  if let Ok(mut cache) = token_cache().lock() {
    cache.insert(uid, (token.clone(), expires));
  }
  Ok(Some(Account { email, token }))
}

async fn require_account(state: &AppState, uid: i64) -> Result<Account, AppError> {
  account(state, uid, false)
    .await?
    .ok_or_else(|| bad("Google Calendar is not connected."))
}

/// Calls Google with the person's token; retries once with a fresh token.
async fn google(
  state: &AppState,
  uid: i64,
  method: reqwest::Method,
  url: Url,
  body: Option<Value>,
  if_match: &str,
) -> Result<(u16, Value), AppError> {
  let mut acct = require_account(state, uid).await?;
  for attempt in 0..2 {
    let mut req = client().request(method.clone(), url.clone()).bearer_auth(&acct.token);
    if !if_match.is_empty() {
      req = req.header("If-Match", if_match);
    }
    if let Some(b) = &body {
      req = req.json(b);
    }
    let resp = req
      .send()
      .await
      .map_err(|e| bad(&format!("Google could not be reached: {}", e)))?;
    let status = resp.status().as_u16();
    if status == 401 && attempt == 0 {
      acct = account(state, uid, true)
        .await?
        .ok_or_else(|| bad("Google Calendar is not connected."))?;
      continue;
    }
    let text = resp.text().await.unwrap_or_default();
    let value: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    return Ok((status, value));
  }
  Err(bad("Google did not accept the connection. Please connect Google Calendar again."))
}

fn calendar_events_url(calendar_id: &str) -> Result<Url, AppError> {
  let mut url = Url::parse(CALENDARS_BASE).map_err(|_| bad("calendar url"))?;
  url
    .path_segments_mut()
    .map_err(|_| bad("calendar url"))?
    .pop_if_empty()
    .push(calendar_id)
    .push("events");
  Ok(url)
}

fn event_url(calendar_id: &str, event_id: &str) -> Result<Url, AppError> {
  let mut url = calendar_events_url(calendar_id)?;
  url.path_segments_mut().map_err(|_| bad("event url"))?.push(event_id);
  Ok(url)
}

/// `href` must be an event address on Google's calendar API.
fn checked_event_url(href: &str) -> Result<Url, AppError> {
  let url = Url::parse(href).map_err(|_| bad("Unknown event"))?;
  if url.scheme() != "https"
    || url.host_str() != Some("www.googleapis.com")
    || !url.as_str().starts_with(CALENDARS_BASE)
    || url.path().contains("..")
    || url.query().is_some()
  {
    return Err(bad("Unknown event"));
  }
  let segments: Vec<&str> = url.path_segments().map(|s| s.collect()).unwrap_or_default();
  // calendar / v3 / calendars / {calendar} / events / {event}
  if segments.len() != 6 || segments[4] != "events" || segments[5].is_empty() {
    return Err(bad("Unknown event"));
  }
  Ok(url)
}

async fn list_calendars(state: &AppState, uid: i64) -> Result<Vec<Value>, AppError> {
  let mut url = Url::parse(API_BASE)
    .and_then(|u| u.join("users/me/calendarList"))
    .map_err(|_| bad("calendar url"))?;
  url.query_pairs_mut().append_pair("maxResults", "100");
  let (status, body) = google(state, uid, reqwest::Method::GET, url, None, "").await?;
  if !(200..300).contains(&status) {
    return Err(google_error(status, &body));
  }
  let mut out = Vec::new();
  for item in body["items"].as_array().cloned().unwrap_or_default() {
    let id = item["id"].as_str().unwrap_or_default().to_string();
    if id.is_empty() || item["deleted"].as_bool() == Some(true) {
      continue;
    }
    let role = item["accessRole"].as_str().unwrap_or("reader");
    if role == "freeBusyReader" {
      continue;
    }
    let name = item["summaryOverride"]
      .as_str()
      .or_else(|| item["summary"].as_str())
      .unwrap_or("Google Calendar")
      .to_string();
    out.push(json!({
      "url": id,
      "name": name,
      "color": item["backgroundColor"].as_str().unwrap_or(""),
      "read_only": !(role == "owner" || role == "writer"),
      "primary": item["primary"].as_bool().unwrap_or(false),
    }));
    if out.len() >= MAX_CALENDARS {
      break;
    }
  }
  // The main calendar first.
  out.sort_by_key(|c| !c["primary"].as_bool().unwrap_or(false));
  Ok(out)
}

// ---------------------------------------------------------------------------
// iCalendar text for the browser
// ---------------------------------------------------------------------------

fn ics_text(s: &str) -> String {
  let clipped: String = s.chars().take(5000).collect();
  clipped
    .replace('\\', "\\\\")
    .replace(';', "\\;")
    .replace(',', "\\,")
    .replace("\r\n", "\\n")
    .replace(['\n', '\r'], "\\n")
}

/// Google descriptions can contain simple HTML; turn it into plain text.
fn plain_text(html: &str) -> String {
  let mut s = html
    .replace("<br>", "\n")
    .replace("<br/>", "\n")
    .replace("<br />", "\n")
    .replace("</p>", "\n")
    .replace("</li>", "\n")
    .replace("</div>", "\n");
  let mut out = String::with_capacity(s.len());
  let mut in_tag = false;
  for c in s.drain(..) {
    match c {
      '<' => in_tag = true,
      '>' if in_tag => in_tag = false,
      _ if !in_tag => out.push(c),
      _ => {},
    }
  }
  out
    .replace("&nbsp;", " ")
    .replace("&lt;", "<")
    .replace("&gt;", ">")
    .replace("&quot;", "\"")
    .replace("&#39;", "'")
    .replace("&amp;", "&")
    .trim()
    .to_string()
}

fn utc_stamp(rfc3339: &str) -> Option<String> {
  chrono::DateTime::parse_from_rfc3339(rfc3339)
    .ok()
    .map(|d| d.with_timezone(&chrono::Utc).format("%Y%m%dT%H%M%SZ").to_string())
}

fn day_stamp(date: &str) -> Option<String> {
  let d = date.replace('-', "");
  if d.len() == 8 && d.chars().all(|c| c.is_ascii_digit()) {
    Some(d)
  } else {
    None
  }
}

/// One Google event as iCalendar text, or None if it should not be shown.
fn event_ics(ev: &Value) -> Option<String> {
  if ev["status"].as_str() == Some("cancelled") {
    return None;
  }
  if let Some(attendees) = ev["attendees"].as_array() {
    let declined = attendees
      .iter()
      .any(|a| a["self"].as_bool() == Some(true) && a["responseStatus"].as_str() == Some("declined"));
    if declined {
      return None;
    }
  }
  let id = ev["id"].as_str()?;
  let (dtstart, dtend) = if let Some(date) = ev["start"]["date"].as_str() {
    let start = day_stamp(date)?;
    let end = ev["end"]["date"].as_str().and_then(day_stamp).unwrap_or_else(|| start.clone());
    (format!("DTSTART;VALUE=DATE:{}", start), format!("DTEND;VALUE=DATE:{}", end))
  } else {
    let start = utc_stamp(ev["start"]["dateTime"].as_str()?)?;
    let end = ev["end"]["dateTime"].as_str().and_then(utc_stamp).unwrap_or_else(|| start.clone());
    (format!("DTSTART:{}", start), format!("DTEND:{}", end))
  };
  let mut lines = vec![
    "BEGIN:VCALENDAR".to_string(),
    "VERSION:2.0".to_string(),
    "PRODID:-//Care Hub//Google//EN".to_string(),
    "BEGIN:VEVENT".to_string(),
    format!("UID:{}", ics_text(id)),
    dtstart,
    dtend,
    format!("SUMMARY:{}", ics_text(ev["summary"].as_str().unwrap_or(""))),
  ];
  if let Some(location) = ev["location"].as_str().filter(|l| !l.trim().is_empty()) {
    lines.push(format!("LOCATION:{}", ics_text(location.trim())));
  }
  let mut notes = plain_text(ev["description"].as_str().unwrap_or(""));
  let meet = ev["hangoutLink"].as_str().unwrap_or("").to_string();
  if !meet.is_empty() && !notes.contains(&meet) {
    if !notes.is_empty() {
      notes.push_str("\n\n");
    }
    notes.push_str(&format!("Google Meet: {}", meet));
  }
  if !notes.is_empty() {
    lines.push(format!("DESCRIPTION:{}", ics_text(&notes)));
  }
  let link = if meet.is_empty() { ev["htmlLink"].as_str().unwrap_or("").to_string() } else { meet };
  if link.starts_with("https://") {
    lines.push(format!("URL:{}", link));
  }
  lines.push("END:VEVENT".to_string());
  lines.push("END:VCALENDAR".to_string());
  Some(lines.join("\r\n") + "\r\n")
}

// ---------------------------------------------------------------------------
// connection
// ---------------------------------------------------------------------------

/// GET /api/carehub/calendar/google -> { enabled, connected, apple_id (email), calendars, error? }
async fn status_handler(user_uuid: UserUuid, state: Data<AppState>) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let s = match settings() {
    Some(s) => s,
    None => return Ok(Json(AppResponse::Ok().with_data(json!({ "enabled": false, "connected": false })))),
  };
  let email = match load_refresh_token(&state, uid, &s.key).await {
    Ok(Some((email, _))) => email,
    Ok(None) => return Ok(Json(AppResponse::Ok().with_data(json!({ "enabled": true, "connected": false })))),
    Err(e) => {
      return Ok(Json(AppResponse::Ok().with_data(json!({
        "enabled": true, "connected": true, "apple_id": "", "calendars": [], "error": e.to_string(),
      }))))
    },
  };
  match list_calendars(&state, uid).await {
    Ok(calendars) => Ok(Json(AppResponse::Ok().with_data(json!({
      "enabled": true,
      "connected": true,
      "apple_id": email,
      "calendars": calendars,
    })))),
    Err(e) => Ok(Json(AppResponse::Ok().with_data(json!({
      "enabled": true,
      "connected": true,
      "apple_id": email,
      "calendars": [],
      "error": e.to_string(),
    })))),
  }
}

#[derive(Debug, Deserialize)]
struct ConnectBody {
  origin: String,
}

/// POST /api/carehub/calendar/google/connect { origin } -> { url } (Google's sign-in page)
async fn connect_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<ConnectBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let s = require_settings()?;
  let origin = clean_origin(&body.origin).ok_or_else(|| {
    bad("Google sign-in only works when Care Hub is opened with https:// (or on this computer as localhost).")
  })?;
  let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
  let now = now_ms();
  sqlx::query("DELETE FROM carehub_google_oauth_state WHERE created_ms < $1")
    .bind(now - STATE_TTL_MS)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  sqlx::query("INSERT INTO carehub_google_oauth_state (state, owner, origin, created_ms) VALUES ($1, $2, $3, $4)")
    .bind(&token)
    .bind(uid)
    .bind(&origin)
    .bind(now)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  let mut url = Url::parse(AUTH_URL).map_err(|_| bad("auth url"))?;
  url
    .query_pairs_mut()
    .append_pair("client_id", &s.client_id)
    .append_pair("redirect_uri", &redirect_uri(&origin))
    .append_pair("response_type", "code")
    .append_pair("scope", SCOPE)
    .append_pair("access_type", "offline")
    .append_pair("prompt", "consent")
    .append_pair("include_granted_scopes", "true")
    .append_pair("state", &token);
  Ok(Json(AppResponse::Ok().with_data(json!({ "url": url.as_str() }))))
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
  #[serde(default)]
  code: String,
  #[serde(default)]
  state: String,
  #[serde(default)]
  error: String,
}

fn back_to_calendar(origin: &str, error: Option<&str>) -> HttpResponse {
  let base = format!("{}/app/calendar", origin);
  let mut url = match Url::parse(&base) {
    Ok(u) => u,
    Err(_) => return HttpResponse::BadRequest().body("Unknown Care Hub address"),
  };
  match error {
    None => {
      url.query_pairs_mut().append_pair("google", "connected");
    },
    Some(e) => {
      url.query_pairs_mut().append_pair("google_error", e);
    },
  }
  HttpResponse::Found()
    .insert_header(("Location", url.as_str().to_string()))
    .insert_header(("Cache-Control", "no-store"))
    .finish()
}

/// GET /api/carehub/calendar/google/callback?code=&state= (Google sends the person here)
async fn callback_handler(state: Data<AppState>, query: web::Query<CallbackQuery>) -> HttpResponse {
  if ensure_tables(&state.pg_pool).await.is_err() {
    return HttpResponse::InternalServerError().body("Care Hub could not finish the Google sign-in.");
  }
  let row: Option<(i64, String, i64)> =
    sqlx::query_as("DELETE FROM carehub_google_oauth_state WHERE state = $1 RETURNING owner, origin, created_ms")
      .bind(&query.state)
      .fetch_optional(&state.pg_pool)
      .await
      .unwrap_or(None);
  let (uid, origin, created) = match row {
    Some(r) => r,
    None => {
      return HttpResponse::BadRequest()
        .content_type("text/plain; charset=utf-8")
        .body("This Google sign-in link has expired. Please go back to Care Hub and click \"Connect Google Calendar\" again.")
    },
  };
  if now_ms() - created > STATE_TTL_MS {
    return back_to_calendar(&origin, Some("The sign-in took too long. Please try again."));
  }
  if !query.error.is_empty() {
    let msg = if query.error == "access_denied" {
      "Google sign-in was cancelled, or your organisation does not allow this app.".to_string()
    } else {
      format!("Google sign-in failed ({}).", query.error)
    };
    return back_to_calendar(&origin, Some(&msg));
  }
  let s = match settings() {
    Some(s) => s,
    None => return back_to_calendar(&origin, Some("Google Calendar is not switched on for this Care Hub.")),
  };
  let sent = client()
    .post(TOKEN_URL)
    .form(&[
      ("code", query.code.as_str()),
      ("client_id", s.client_id.as_str()),
      ("client_secret", s.client_secret.as_str()),
      ("redirect_uri", redirect_uri(&origin).as_str()),
      ("grant_type", "authorization_code"),
    ])
    .send()
    .await;
  let resp = match sent {
    Ok(r) => r,
    Err(_) => return back_to_calendar(&origin, Some("Google could not be reached. Please try again.")),
  };
  let status = resp.status().as_u16();
  let body: Value = resp.json().await.unwrap_or(Value::Null);
  if !(200..300).contains(&status) {
    let detail = body["error_description"].as_str().or_else(|| body["error"].as_str()).unwrap_or("unknown error");
    return back_to_calendar(&origin, Some(&format!("Google sign-in failed: {}", detail)));
  }
  let refresh = body["refresh_token"].as_str().unwrap_or_default().to_string();
  let access = body["access_token"].as_str().unwrap_or_default().to_string();
  if refresh.is_empty() || access.is_empty() {
    return back_to_calendar(
      &origin,
      Some("Google did not allow offline access. Please try again and allow access to your calendar."),
    );
  }
  // The main calendar's id is the person's Google email address.
  let mut email = String::new();
  if let Ok(url) = Url::parse(CALENDARS_BASE).and_then(|u| u.join("primary")) {
    if let Ok(r) = client().get(url).bearer_auth(&access).send().await {
      let v: Value = r.json().await.unwrap_or(Value::Null);
      email = v["id"].as_str().unwrap_or_default().to_string();
    }
  }
  let saved = sqlx::query(
    "INSERT INTO carehub_google_account (owner, email, secret) VALUES ($1, $2, pgp_sym_encrypt($3, $4)) \
     ON CONFLICT (owner) DO UPDATE SET email = EXCLUDED.email, secret = EXCLUDED.secret, created_at = NOW()",
  )
  .bind(uid)
  .bind(&email)
  .bind(&refresh)
  .bind(&s.key)
  .execute(&state.pg_pool)
  .await;
  if saved.is_err() {
    return back_to_calendar(&origin, Some("Care Hub could not save the Google connection."));
  }
  let expires = now_ms() + body["expires_in"].as_i64().unwrap_or(3000) * 1000;
  if let Ok(mut cache) = token_cache().lock() {
    cache.insert(uid, (access, expires));
  }
  back_to_calendar(&origin, None)
}

/// DELETE /api/carehub/calendar/google
async fn disconnect_handler(user_uuid: UserUuid, state: Data<AppState>) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  if let Some(s) = settings() {
    if let Ok(Some((_, refresh))) = load_refresh_token(&state, uid, &s.key).await {
      // Also tell Google, so Care Hub disappears from the Google account's app list.
      let _ = client().post(REVOKE_URL).form(&[("token", refresh.as_str())]).send().await;
    }
  }
  sqlx::query("DELETE FROM carehub_google_account WHERE owner = $1")
    .bind(uid)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  if let Ok(mut cache) = token_cache().lock() {
    cache.remove(&uid);
  }
  Ok(Json(AppResponse::Ok().with_data(json!({ "connected": false }))))
}

// ---------------------------------------------------------------------------
// events
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RangeQuery {
  from: i64,
  to: i64,
}

fn rfc3339(ms: i64) -> Result<String, AppError> {
  chrono::DateTime::from_timestamp_millis(ms)
    .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
    .ok_or_else(|| bad("The date is out of range"))
}

/// GET /api/carehub/calendar/google/events?from=&to= -> { events: [{ calendar, href, etag, ics }], errors }
async fn list_events_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  query: web::Query<RangeQuery>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  if query.to <= query.from || query.to - query.from > 400 * 86_400_000 {
    return Err(bad("Please choose a shorter date range").into());
  }
  let time_min = rfc3339(query.from)?;
  let time_max = rfc3339(query.to)?;
  let calendars = list_calendars(&state, uid).await?;
  let mut events = Vec::new();
  let mut errors = Vec::new();
  'calendars: for cal in calendars {
    let cal_id = cal["url"].as_str().unwrap_or_default().to_string();
    let mut page_token = String::new();
    for _page in 0..5 {
      let mut url = calendar_events_url(&cal_id)?;
      {
        let mut q = url.query_pairs_mut();
        q.append_pair("timeMin", &time_min)
          .append_pair("timeMax", &time_max)
          .append_pair("singleEvents", "true")
          .append_pair("maxResults", "2500")
          .append_pair("showDeleted", "false");
        if !page_token.is_empty() {
          q.append_pair("pageToken", &page_token);
        }
      }
      let (status, body) = match google(&state, uid, reqwest::Method::GET, url, None, "").await {
        Ok(r) => r,
        Err(e) => {
          errors.push(json!({ "calendar": cal_id, "error": e.to_string() }));
          continue 'calendars;
        },
      };
      if !(200..300).contains(&status) {
        errors.push(json!({ "calendar": cal_id, "error": google_error(status, &body).to_string() }));
        continue 'calendars;
      }
      for ev in body["items"].as_array().cloned().unwrap_or_default() {
        let ics = match event_ics(&ev) {
          Some(ics) => ics,
          None => continue,
        };
        let id = ev["id"].as_str().unwrap_or_default();
        let href = match event_url(&cal_id, id) {
          Ok(u) => u,
          Err(_) => continue,
        };
        events.push(json!({
          "calendar": cal_id,
          "href": href.as_str(),
          "etag": ev["etag"].as_str().unwrap_or(""),
          "ics": ics,
        }));
        if events.len() >= MAX_EVENTS {
          break 'calendars;
        }
      }
      page_token = body["nextPageToken"].as_str().unwrap_or_default().to_string();
      if page_token.is_empty() {
        break;
      }
    }
  }
  Ok(Json(AppResponse::Ok().with_data(json!({ "events": events, "errors": errors }))))
}

#[derive(Debug, Deserialize)]
struct EventBody {
  #[serde(default)]
  calendar: String,
  #[serde(default)]
  href: String,
  #[serde(default)]
  etag: String,
  #[serde(default)]
  title: String,
  start: i64,
  end: i64,
  #[serde(default)]
  all_day: bool,
  /// For all-day events: first and last+1 day as YYYYMMDD (the person's own dates).
  #[serde(default)]
  start_day: String,
  #[serde(default)]
  end_day: String,
  #[serde(default)]
  location: String,
  #[serde(default)]
  notes: String,
}

fn iso_day(d: &str) -> Option<String> {
  if d.len() == 8 && d.chars().all(|c| c.is_ascii_digit()) {
    Some(format!("{}-{}-{}", &d[0..4], &d[4..6], &d[6..8]))
  } else {
    None
  }
}

fn google_event(body: &EventBody) -> Result<Value, AppError> {
  if body.end <= body.start {
    return Err(bad("The end must be after the start"));
  }
  let (start, end) = if body.all_day {
    let s = iso_day(&body.start_day).ok_or_else(|| bad("Please enter valid dates"))?;
    let e = iso_day(&body.end_day).ok_or_else(|| bad("Please enter valid dates"))?;
    if e <= s {
      return Err(bad("Please enter valid dates"));
    }
    (json!({ "date": s, "dateTime": null }), json!({ "date": e, "dateTime": null }))
  } else {
    (
      json!({ "dateTime": rfc3339(body.start)?, "date": null }),
      json!({ "dateTime": rfc3339(body.end)?, "date": null }),
    )
  };
  let title: String = body.title.trim().chars().take(1000).collect();
  let location: String = body.location.trim().chars().take(1000).collect();
  let notes: String = body.notes.chars().take(8000).collect();
  Ok(json!({
    "summary": title,
    "location": location,
    "description": notes,
    "start": start,
    "end": end,
  }))
}

/// POST /api/carehub/calendar/google/events { calendar, title, start, end, all_day, ... }
async fn create_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<EventBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  if body.calendar.trim().is_empty() {
    return Err(bad("Please choose a Google calendar").into());
  }
  let event = google_event(&body)?;
  let url = calendar_events_url(body.calendar.trim())?;
  let (status, created) = google(&state, uid, reqwest::Method::POST, url, Some(event), "").await?;
  if !(200..300).contains(&status) {
    return Err(google_error(status, &created).into());
  }
  let id = created["id"].as_str().unwrap_or_default();
  let href = event_url(body.calendar.trim(), id)?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "href": href.as_str(),
    "etag": created["etag"].as_str().unwrap_or(""),
    "uid": id,
  }))))
}

/// PUT /api/carehub/calendar/google/events { href, etag, title, start, end, ... }
async fn update_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<EventBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let url = checked_event_url(&body.href)?;
  let event = google_event(&body)?;
  let (status, updated) = google(&state, uid, reqwest::Method::PATCH, url.clone(), Some(event), &body.etag).await?;
  if !(200..300).contains(&status) {
    return Err(google_error(status, &updated).into());
  }
  Ok(Json(AppResponse::Ok().with_data(json!({
    "href": url.as_str(),
    "etag": updated["etag"].as_str().unwrap_or(""),
  }))))
}

#[derive(Debug, Deserialize)]
struct DeleteBody {
  href: String,
  #[serde(default)]
  etag: String,
}

/// POST /api/carehub/calendar/google/events/delete { href, etag }
async fn delete_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<DeleteBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let url = checked_event_url(&body.href)?;
  let (status, resp) = google(&state, uid, reqwest::Method::DELETE, url, None, &body.etag).await?;
  if !(200..300).contains(&status) && status != 404 && status != 410 {
    return Err(google_error(status, &resp).into());
  }
  Ok(Json(AppResponse::Ok().with_data(json!({ "deleted": true }))))
}
