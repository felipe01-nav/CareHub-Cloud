//! CareHub: private iCloud calendars (two-way), the way Notion Calendar does it.
//!
//! The person creates an "app-specific password" for their Apple ID and enters
//! it once in Care Hub. The server talks to iCloud's calendar service (CalDAV)
//! with it, so the calendar does not need to be public.
//!
//! * The password is stored encrypted in the database (pgcrypto, key from the
//!   CAREHUB_SECRET_KEY setting) and is never sent back to the browser.
//! * Requests only go to *.icloud.com over https, and only to addresses inside
//!   the person's own iCloud calendar home.
//! * Each person can only see and change their own connection.

use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const ICLOUD_START: &str = "https://caldav.icloud.com/";
const MAX_RESPONSE_BYTES: usize = 20 * 1024 * 1024;
const MAX_CALENDARS: usize = 30;

static TABLE_READY: AtomicBool = AtomicBool::new(false);

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/carehub/calendar/icloud")
        .route(web::get().to(status_handler))
        .route(web::post().to(connect_handler))
        .route(web::delete().to(disconnect_handler)),
    )
    .service(
      web::resource("/api/carehub/calendar/icloud/events")
        .route(web::get().to(list_events_handler))
        .route(web::post().to(create_event_handler))
        .route(web::put().to(update_event_handler)),
    )
    .service(
      web::resource("/api/carehub/calendar/icloud/events/delete")
        .route(web::post().to(delete_event_handler)),
    );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn internal(msg: String) -> AppError {
  AppError::Internal(anyhow::anyhow!(msg))
}

fn secret_key() -> Result<String, AppError> {
  let key = std::env::var("CAREHUB_SECRET_KEY").unwrap_or_default();
  if key.trim().len() < 32 {
    return Err(bad(
      "iCloud calendars are not switched on for this Care Hub yet (CAREHUB_SECRET_KEY is missing).",
    ));
  }
  Ok(key.trim().to_string())
}

async fn owner_uid(state: &AppState, user_uuid: &UserUuid) -> Result<i64, AppError> {
  if !TABLE_READY.load(Ordering::Relaxed) {
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto")
      .execute(&state.pg_pool)
      .await?;
    sqlx::query(
      "CREATE TABLE IF NOT EXISTS carehub_icloud_account (\
         owner BIGINT PRIMARY KEY,\
         apple_id TEXT NOT NULL,\
         secret BYTEA NOT NULL,\
         home_url TEXT NOT NULL,\
         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    )
    .execute(&state.pg_pool)
    .await?;
    TABLE_READY.store(true, Ordering::Relaxed);
  }
  Ok(state.user_cache.get_user_uid(user_uuid).await?)
}

fn icloud_url(raw: &str) -> Option<Url> {
  let url = Url::parse(raw).ok()?;
  let host = url.host_str()?.to_ascii_lowercase();
  if url.scheme() != "https" || !(host == "icloud.com" || host.ends_with(".icloud.com")) {
    return None;
  }
  if !url.username().is_empty() || url.password().is_some() {
    return None;
  }
  Some(url)
}

/// `target` must be an iCloud address inside the person's calendar home.
fn inside_home(home: &Url, target: &str) -> Result<Url, AppError> {
  let url = icloud_url(target).ok_or_else(|| bad("Unknown calendar address"))?;
  if url.host_str() != home.host_str() || url.port_or_known_default() != home.port_or_known_default() {
    return Err(bad("Unknown calendar address"));
  }
  if !url.path().starts_with(home.path()) || url.path().contains("..") {
    return Err(bad("Unknown calendar address"));
  }
  Ok(url)
}

struct Creds {
  apple_id: String,
  password: String,
}

fn client() -> Result<reqwest::Client, AppError> {
  reqwest::Client::builder()
    .timeout(Duration::from_secs(25))
    // Redirects are followed by hand so the login is only sent to iCloud.
    .redirect(reqwest::redirect::Policy::none())
    .user_agent("CareHub-Calendar/1.0")
    .build()
    .map_err(|e| internal(format!("icloud client: {}", e)))
}

struct DavResponse {
  status: StatusCode,
  url: Url,
  etag: Option<String>,
  body: String,
}

async fn dav(
  creds: &Creds,
  method: &str,
  url: Url,
  depth: Option<&str>,
  body: Option<String>,
  content_type: &str,
  extra: &[(&str, String)],
) -> Result<DavResponse, AppError> {
  let http = client()?;
  let method = Method::from_bytes(method.as_bytes()).map_err(|_| internal("bad method".into()))?;
  let mut current = url;
  for _ in 0..4 {
    let mut req = http
      .request(method.clone(), current.clone())
      .basic_auth(&creds.apple_id, Some(&creds.password));
    if let Some(d) = depth {
      req = req.header("Depth", d);
    }
    if let Some(b) = body.clone() {
      req = req.header("Content-Type", content_type).body(b);
    }
    for (k, v) in extra {
      req = req.header(*k, v.as_str());
    }
    let mut resp = req
      .send()
      .await
      .map_err(|_| bad("Could not reach iCloud. Please try again."))?;
    let status = resp.status();
    if status.is_redirection() {
      let next = resp
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .and_then(|loc| current.join(loc).ok())
        .and_then(|u| icloud_url(u.as_str()));
      match next {
        Some(u) => {
          current = u;
          continue;
        },
        None => return Err(bad("iCloud sent an unexpected answer.")),
      }
    }
    let etag = resp
      .headers()
      .get("etag")
      .and_then(|v| v.to_str().ok())
      .map(|s| s.to_string());
    let mut data: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
      .chunk()
      .await
      .map_err(|_| bad("Could not read the answer from iCloud."))?
    {
      if data.len() + chunk.len() > MAX_RESPONSE_BYTES {
        return Err(bad("The iCloud answer is too big."));
      }
      data.extend_from_slice(&chunk);
    }
    if status == StatusCode::UNAUTHORIZED {
      return Err(bad(
        "iCloud did not accept the Apple ID or app-specific password. Please check them.",
      ));
    }
    return Ok(DavResponse {
      status,
      url: current,
      etag,
      body: String::from_utf8_lossy(&data).to_string(),
    });
  }
  Err(bad("iCloud redirected too many times."))
}

// ---- tiny XML reader (namespace prefixes are ignored) ----

/// Inner text of every element named `local` (any prefix).
fn elements(xml: &str, local: &str) -> Vec<String> {
  let mut out = Vec::new();
  let bytes = xml.as_bytes();
  let mut i = 0;
  while let Some(pos) = xml[i..].find('<') {
    let start = i + pos;
    i = start + 1;
    if i >= xml.len() {
      break;
    }
    let c = bytes[i];
    if c == b'/' || c == b'?' || c == b'!' {
      continue;
    }
    let name_end = xml[i..]
      .find(|ch: char| ch.is_whitespace() || ch == '>' || ch == '/')
      .map(|p| i + p)
      .unwrap_or(xml.len());
    let qname = &xml[i..name_end];
    let name = qname.rsplit(':').next().unwrap_or(qname);
    if !name.eq_ignore_ascii_case(local) {
      continue;
    }
    let tag_end = match xml[name_end..].find('>') {
      Some(p) => name_end + p,
      None => break,
    };
    if bytes[tag_end - 1] == b'/' {
      out.push(String::new());
      i = tag_end + 1;
      continue;
    }
    let close = format!("</{}>", qname);
    match xml[tag_end + 1..].find(&close) {
      Some(p) => {
        out.push(xml[tag_end + 1..tag_end + 1 + p].to_string());
        i = tag_end + 1 + p + close.len();
      },
      None => break,
    }
  }
  out
}

fn first(xml: &str, local: &str) -> Option<String> {
  elements(xml, local).into_iter().next()
}

fn unescape(s: &str) -> String {
  let t = s.trim();
  let t = if let (Some(a), Some(b)) = (t.find("<![CDATA["), t.rfind("]]>")) {
    if b >= a + 9 { t[a + 9..b].to_string() } else { t.to_string() }
  } else {
    t.to_string()
  };
  t.replace("&lt;", "<")
    .replace("&gt;", ">")
    .replace("&quot;", "\"")
    .replace("&apos;", "'")
    .replace("&#13;", "\r")
    .replace("&#xD;", "\r")
    .replace("&#xd;", "\r")
    .replace("&#10;", "\n")
    .replace("&#xA;", "\n")
    .replace("&#xa;", "\n")
    .replace("&amp;", "&")
}

fn first_href(xml: &str) -> Option<String> {
  first(xml, "href").map(|h| unescape(&h))
}

// ---- discovery ----

async fn find_home(creds: &Creds) -> Result<Url, AppError> {
  let start = Url::parse(ICLOUD_START).map_err(|_| internal("start url".into()))?;
  let body = r#"<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:current-user-principal/></d:prop></d:propfind>"#;
  let r = dav(creds, "PROPFIND", start, Some("0"), Some(body.into()), "application/xml; charset=utf-8", &[]).await?;
  if r.status.as_u16() != 207 {
    return Err(bad("iCloud did not accept the sign-in. Please check the Apple ID and password."));
  }
  let principal_href = first(&r.body, "current-user-principal")
    .and_then(|p| first_href(&p))
    .ok_or_else(|| bad("Could not find your iCloud calendars."))?;
  let principal = r
    .url
    .join(&principal_href)
    .ok()
    .and_then(|u| icloud_url(u.as_str()))
    .ok_or_else(|| bad("Could not find your iCloud calendars."))?;
  let body = r#"<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav"><d:prop><c:calendar-home-set/></d:prop></d:propfind>"#;
  let r = dav(creds, "PROPFIND", principal, Some("0"), Some(body.into()), "application/xml; charset=utf-8", &[]).await?;
  let home_href = first(&r.body, "calendar-home-set")
    .and_then(|p| first_href(&p))
    .ok_or_else(|| bad("Could not find your iCloud calendars."))?;
  let mut home = r
    .url
    .join(&home_href)
    .ok()
    .and_then(|u| icloud_url(u.as_str()))
    .ok_or_else(|| bad("Could not find your iCloud calendars."))?;
  if !home.path().ends_with('/') {
    let p = format!("{}/", home.path());
    home.set_path(&p);
  }
  Ok(home)
}

async fn list_calendars(creds: &Creds, home: &Url) -> Result<Vec<Value>, AppError> {
  let body = r#"<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav" xmlns:ic="http://apple.com/ns/ical/"><d:prop><d:displayname/><d:resourcetype/><ic:calendar-color/><c:supported-calendar-component-set/><d:current-user-privilege-set/></d:prop></d:propfind>"#;
  let r = dav(creds, "PROPFIND", home.clone(), Some("1"), Some(body.into()), "application/xml; charset=utf-8", &[]).await?;
  if r.status.as_u16() != 207 {
    return Err(bad("Could not load your iCloud calendars."));
  }
  let mut out = Vec::new();
  for resp in elements(&r.body, "response") {
    let rtype = first(&resp, "resourcetype").unwrap_or_default();
    if elements(&rtype, "calendar").is_empty() {
      continue;
    }
    if let Some(comps) = first(&resp, "supported-calendar-component-set") {
      if !comps.to_ascii_uppercase().contains("VEVENT") {
        continue;
      }
    }
    let href = match first_href(&resp) {
      Some(h) => h,
      None => continue,
    };
    let url = match home.join(&href).ok().and_then(|u| inside_home(home, u.as_str()).ok()) {
      Some(u) => u,
      None => continue,
    };
    let name = first(&resp, "displayname").map(|n| unescape(&n)).unwrap_or_default();
    let mut color = first(&resp, "calendar-color").map(|c| unescape(&c)).unwrap_or_default();
    if color.len() == 9 && color.starts_with('#') {
      color.truncate(7);
    }
    let privileges = first(&resp, "current-user-privilege-set").unwrap_or_default();
    let read_only = !privileges.is_empty()
      && elements(&privileges, "write").is_empty()
      && elements(&privileges, "write-content").is_empty()
      && elements(&privileges, "all").is_empty();
    let display = if name.trim().is_empty() {
      "iCloud".to_string()
    } else {
      name.trim().to_string()
    };
    out.push(json!({
      "url": url.as_str(),
      "name": display,
      "color": color,
      "read_only": read_only,
    }));
    if out.len() >= MAX_CALENDARS {
      break;
    }
  }
  Ok(out)
}

async fn load_account(state: &AppState, uid: i64) -> Result<Option<(Creds, Url)>, AppError> {
  let key = secret_key()?;
  let row: Option<(String, String, String)> = sqlx::query_as(
    "SELECT apple_id, pgp_sym_decrypt(secret, $2), home_url FROM carehub_icloud_account WHERE owner = $1",
  )
  .bind(uid)
  .bind(&key)
  .fetch_optional(&state.pg_pool)
  .await
  .map_err(|_| bad("Could not read the iCloud connection. Please connect iCloud again."))?;
  match row {
    None => Ok(None),
    Some((apple_id, password, home)) => {
      let home = icloud_url(&home).ok_or_else(|| bad("Please connect iCloud again."))?;
      Ok(Some((Creds { apple_id, password }, home)))
    },
  }
}

async fn require_account(state: &AppState, uid: i64) -> Result<(Creds, Url), AppError> {
  load_account(state, uid)
    .await?
    .ok_or_else(|| bad("iCloud is not connected."))
}

// ---------------------------------------------------------------------------
// connection
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ConnectBody {
  apple_id: String,
  password: String,
}

/// GET /api/carehub/calendar/icloud -> { connected, apple_id, calendars, error? }
async fn status_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let enabled = secret_key().is_ok();
  if !enabled {
    return Ok(Json(AppResponse::Ok().with_data(json!({ "enabled": false, "connected": false }))));
  }
  let account = load_account(&state, uid).await?;
  let (creds, home) = match account {
    None => {
      return Ok(Json(AppResponse::Ok().with_data(json!({ "enabled": true, "connected": false }))))
    },
    Some(a) => a,
  };
  match list_calendars(&creds, &home).await {
    Ok(calendars) => Ok(Json(AppResponse::Ok().with_data(json!({
      "enabled": true,
      "connected": true,
      "apple_id": creds.apple_id,
      "calendars": calendars,
    })))),
    Err(e) => Ok(Json(AppResponse::Ok().with_data(json!({
      "enabled": true,
      "connected": true,
      "apple_id": creds.apple_id,
      "calendars": [],
      "error": e.to_string(),
    })))),
  }
}

/// POST /api/carehub/calendar/icloud { apple_id, password }
async fn connect_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<ConnectBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let key = secret_key()?;
  let apple_id = body.apple_id.trim().to_string();
  // App-specific passwords look like xxxx-xxxx-xxxx-xxxx; spaces are removed.
  let password: String = body.password.chars().filter(|c| !c.is_whitespace()).collect();
  if !apple_id.contains('@') || apple_id.len() > 200 {
    return Err(bad("Please enter the email address of your Apple ID.").into());
  }
  if password.len() < 8 || password.len() > 100 {
    return Err(bad("Please paste the app-specific password from Apple (xxxx-xxxx-xxxx-xxxx).").into());
  }
  let creds = Creds { apple_id: apple_id.clone(), password: password.clone() };
  let home = find_home(&creds).await?;
  let calendars = list_calendars(&creds, &home).await?;
  sqlx::query(
    "INSERT INTO carehub_icloud_account (owner, apple_id, secret, home_url) \
     VALUES ($1, $2, pgp_sym_encrypt($3, $4), $5) \
     ON CONFLICT (owner) DO UPDATE SET apple_id = EXCLUDED.apple_id, secret = EXCLUDED.secret, \
     home_url = EXCLUDED.home_url, created_at = NOW()",
  )
  .bind(uid)
  .bind(&apple_id)
  .bind(&password)
  .bind(&key)
  .bind(home.as_str())
  .execute(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "enabled": true,
    "connected": true,
    "apple_id": apple_id,
    "calendars": calendars,
  }))))
}

/// DELETE /api/carehub/calendar/icloud
async fn disconnect_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  sqlx::query("DELETE FROM carehub_icloud_account WHERE owner = $1")
    .bind(uid)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
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

fn dav_time(ms: i64) -> Result<String, AppError> {
  chrono::DateTime::from_timestamp_millis(ms)
    .map(|d| d.format("%Y%m%dT%H%M%SZ").to_string())
    .ok_or_else(|| bad("The date is out of range"))
}

/// GET /api/carehub/calendar/icloud/events?from=&to= -> { events: [{ calendar, href, etag, ics }] }
async fn list_events_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  query: web::Query<RangeQuery>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let (creds, home) = require_account(&state, uid).await?;
  if query.to <= query.from || query.to - query.from > 400 * 86_400_000 {
    return Err(bad("Please choose a shorter date range").into());
  }
  let start = dav_time(query.from)?;
  let end = dav_time(query.to)?;
  let calendars = list_calendars(&creds, &home).await?;
  let report = format!(
    r#"<?xml version="1.0" encoding="utf-8"?><c:calendar-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav"><d:prop><d:getetag/><c:calendar-data/></d:prop><c:filter><c:comp-filter name="VCALENDAR"><c:comp-filter name="VEVENT"><c:time-range start="{}" end="{}"/></c:comp-filter></c:comp-filter></c:filter></c:calendar-query>"#,
    start, end
  );
  let mut events = Vec::new();
  let mut errors = Vec::new();
  for cal in calendars {
    let cal_url = cal["url"].as_str().unwrap_or_default().to_string();
    let url = match inside_home(&home, &cal_url) {
      Ok(u) => u,
      Err(_) => continue,
    };
    let r = match dav(&creds, "REPORT", url.clone(), Some("1"), Some(report.clone()), "application/xml; charset=utf-8", &[]).await {
      Ok(r) => r,
      Err(e) => {
        errors.push(json!({ "calendar": cal_url, "error": e.to_string() }));
        continue;
      },
    };
    if r.status.as_u16() != 207 {
      errors.push(json!({ "calendar": cal_url, "error": format!("iCloud error {}", r.status.as_u16()) }));
      continue;
    }
    for resp in elements(&r.body, "response") {
      let data = match first(&resp, "calendar-data") {
        Some(d) => unescape(&d),
        None => continue,
      };
      if !data.contains("BEGIN:VCALENDAR") {
        continue;
      }
      let href = match first_href(&resp).and_then(|h| url.join(&h).ok()) {
        Some(h) => h,
        None => continue,
      };
      let etag = first(&resp, "getetag").map(|e| unescape(&e)).unwrap_or_default();
      events.push(json!({ "calendar": cal_url, "href": href.as_str(), "etag": etag, "ics": data }));
      if events.len() >= 5000 {
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
  uid: String,
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

fn ics_text(s: &str) -> String {
  let clipped: String = s.chars().take(5000).collect();
  clipped
    .replace('\\', "\\\\")
    .replace(';', "\\;")
    .replace(',', "\\,")
    .replace("\r\n", "\\n")
    .replace(['\n', '\r'], "\\n")
}

fn is_day(s: &str) -> bool {
  s.len() == 8 && s.chars().all(|c| c.is_ascii_digit())
}

fn build_ics(uid: &str, body: &EventBody) -> Result<String, AppError> {
  if body.end <= body.start {
    return Err(bad("The end must be after the start"));
  }
  let (dtstart, dtend) = if body.all_day {
    if !is_day(&body.start_day) || !is_day(&body.end_day) || body.end_day <= body.start_day {
      return Err(bad("Please enter valid dates"));
    }
    (
      format!("DTSTART;VALUE=DATE:{}", body.start_day),
      format!("DTEND;VALUE=DATE:{}", body.end_day),
    )
  } else {
    (format!("DTSTART:{}", dav_time(body.start)?), format!("DTEND:{}", dav_time(body.end)?))
  };
  let now = dav_time(chrono::Utc::now().timestamp_millis())?;
  let mut lines = vec![
    "BEGIN:VCALENDAR".to_string(),
    "VERSION:2.0".to_string(),
    "PRODID:-//Care Hub//Calendar//EN".to_string(),
    "CALSCALE:GREGORIAN".to_string(),
    "BEGIN:VEVENT".to_string(),
    format!("UID:{}", ics_text(uid)),
    format!("DTSTAMP:{}", now),
    format!("LAST-MODIFIED:{}", now),
    dtstart,
    dtend,
    format!("SUMMARY:{}", ics_text(body.title.trim())),
  ];
  if !body.location.trim().is_empty() {
    lines.push(format!("LOCATION:{}", ics_text(body.location.trim())));
  }
  if !body.notes.trim().is_empty() {
    lines.push(format!("DESCRIPTION:{}", ics_text(&body.notes)));
  }
  lines.push("END:VEVENT".to_string());
  lines.push("END:VCALENDAR".to_string());
  Ok(lines.join("\r\n") + "\r\n")
}

fn check_write(r: &DavResponse) -> Result<(), AppError> {
  match r.status.as_u16() {
    200..=299 => Ok(()),
    403 => Err(bad("This iCloud calendar can't be changed (it is read-only).")),
    404 => Err(bad("This event no longer exists in iCloud.")),
    412 => Err(bad("The event was changed somewhere else. Please reload and try again.")),
    code => Err(bad(&format!("iCloud could not save the change (error {}).", code))),
  }
}

/// POST /api/carehub/calendar/icloud/events { calendar, title, start, end, all_day, ... }
async fn create_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<EventBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let (creds, home) = require_account(&state, uid).await?;
  let mut cal = inside_home(&home, &body.calendar)?;
  if !cal.path().ends_with('/') {
    let p = format!("{}/", cal.path());
    cal.set_path(&p);
  }
  let event_uid = format!("{}@carehub", Uuid::new_v4());
  let ics = build_ics(&event_uid, &body)?;
  let target = cal
    .join(&format!("{}.ics", Uuid::new_v4()))
    .map_err(|_| internal("event url".into()))?;
  let r = dav(
    &creds,
    "PUT",
    target.clone(),
    None,
    Some(ics),
    "text/calendar; charset=utf-8",
    &[("If-None-Match", "*".to_string())],
  )
  .await?;
  check_write(&r)?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "href": target.as_str(),
    "etag": r.etag.unwrap_or_default(),
    "uid": event_uid,
  }))))
}

/// PUT /api/carehub/calendar/icloud/events { href, etag, uid, title, start, end, ... }
async fn update_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<EventBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let (creds, home) = require_account(&state, uid).await?;
  let target = inside_home(&home, &body.href)?;
  if body.uid.trim().is_empty() {
    return Err(bad("This event can't be changed here").into());
  }
  let ics = build_ics(body.uid.trim(), &body)?;
  let mut extra = Vec::new();
  if !body.etag.is_empty() {
    extra.push(("If-Match", body.etag.clone()));
  }
  let r = dav(&creds, "PUT", target.clone(), None, Some(ics), "text/calendar; charset=utf-8", &extra).await?;
  check_write(&r)?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "href": target.as_str(),
    "etag": r.etag.unwrap_or_default(),
  }))))
}

#[derive(Debug, Deserialize)]
struct DeleteBody {
  href: String,
  #[serde(default)]
  etag: String,
}

/// POST /api/carehub/calendar/icloud/events/delete { href, etag }
async fn delete_event_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<DeleteBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = owner_uid(&state, &user_uuid).await?;
  let (creds, home) = require_account(&state, uid).await?;
  let target = inside_home(&home, &body.href)?;
  if !target.path().ends_with(".ics") {
    return Err(bad("Unknown event").into());
  }
  let mut extra = Vec::new();
  if !body.etag.is_empty() {
    extra.push(("If-Match", body.etag.clone()));
  }
  let r = dav(&creds, "DELETE", target, None, None, "", &extra).await?;
  if r.status.as_u16() != 404 {
    check_write(&r)?;
  }
  Ok(Json(AppResponse::Ok().with_data(json!({ "deleted": true }))))
}
