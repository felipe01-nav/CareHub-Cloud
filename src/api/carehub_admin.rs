//! CareHub: "User accounts" admin screen (Settings > User accounts).
//!
//! Lets the CareHub administrator list sign-in accounts, add an account with a
//! password, set a new password and remove an account - without email.
//! Only people whose email is listed in the CAREHUB_ADMIN_EMAILS setting
//! (comma separated) may use these routes. Everyone else gets "no permission".
//!
//! The accounts live in the sign-in service (GoTrue); these handlers call its
//! admin API with the server's service key.

use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use database_entity::dto::AFRole;
use reqwest::Method;
use serde::Deserialize;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use uuid::Uuid;

const MIN_PASSWORD_LEN: usize = 6;
/// Longest name accepted when an account is made (counted in characters).
const MAX_NAME_CHARS: usize = 100;

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(web::resource("/api/carehub/admin/me").route(web::get().to(admin_me_handler)))
    .service(
      web::resource("/api/carehub/admin/users")
        .route(web::get().to(list_users_handler))
        .route(web::post().to(create_user_handler)),
    )
    .service(
      web::resource("/api/carehub/admin/users/{user_id}/password")
        .route(web::put().to(set_password_handler)),
    )
    .service(
      web::resource("/api/carehub/admin/users/{user_id}/workspace")
        .route(web::post().to(join_workspace_handler))
        .route(web::delete().to(leave_workspace_handler)),
    )
    .service(
      web::resource("/api/carehub/admin/users/{user_id}")
        .route(web::delete().to(delete_user_handler)),
    )
    .service(
      web::resource("/api/carehub/account/password-status")
        .route(web::get().to(password_status_handler)),
    )
    .service(
      web::resource("/api/carehub/account/password")
        .route(web::post().to(change_own_password_handler)),
    );
}

#[derive(Debug, Deserialize)]
struct CreateUserBody {
  email: String,
  password: String,
  /// Optional: the person's name. It becomes their name in Care Hub at their first sign-in.
  #[serde(default)]
  name: Option<String>,
  /// Optional: also make the new person a member of this workspace.
  workspace_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
struct WorkspaceBody {
  workspace_id: Uuid,
}

#[derive(Debug, Deserialize)]
struct WorkspaceQuery {
  workspace_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
struct SetPasswordBody {
  password: String,
}

fn internal(msg: String) -> AppError {
  AppError::Internal(anyhow::anyhow!(msg))
}

fn admin_emails() -> Vec<String> {
  std::env::var("CAREHUB_ADMIN_EMAILS")
    .unwrap_or_default()
    .split(',')
    .map(|s| s.trim().to_lowercase())
    .filter(|s| !s.is_empty())
    .collect()
}

/// Calls the sign-in service's admin API and returns the JSON answer.
async fn gotrue_admin_call(
  state: &AppState,
  method: Method,
  path: &str,
  body: Option<Value>,
) -> Result<Value, AppError> {
  let token = state.gotrue_admin.token().await?;
  let url = format!("{}{}", state.gotrue_client.base_url, path);
  let mut req = state
    .gotrue_client
    .http_client_with_auth(method, &url, &token);
  if let Some(body) = body {
    req = req.json(&body);
  }
  let resp = req
    .send()
    .await
    .map_err(|err| internal(format!("sign-in service unreachable: {}", err)))?;
  let status = resp.status();
  let text = resp
    .text()
    .await
    .map_err(|err| internal(format!("sign-in service answer unreadable: {}", err)))?;
  let value: Value = if text.trim().is_empty() {
    Value::Null
  } else {
    serde_json::from_str(&text).unwrap_or_else(|_| json!({ "msg": text }))
  };
  if status.is_success() {
    return Ok(value);
  }
  let msg = value
    .get("msg")
    .or_else(|| value.get("message"))
    .or_else(|| value.get("error_description"))
    .and_then(|v| v.as_str())
    .unwrap_or("request failed")
    .to_string();
  let code = status.as_u16();
  if code == 404 {
    return Err(AppError::RecordNotFound(msg));
  }
  if code == 422 || code == 400 {
    if msg.to_lowercase().contains("already") {
      return Err(AppError::UserAlreadyRegistered(
        "An account with this email already exists".to_string(),
      ));
    }
    return Err(AppError::InvalidRequest(msg));
  }
  Err(internal(format!("sign-in service error {}: {}", code, msg)))
}

/// The signed-in person's email, looked up in the sign-in service.
async fn caller_email(state: &AppState, user_uuid: &UserUuid) -> Result<String, AppError> {
  let id: Uuid = **user_uuid;
  let user = gotrue_admin_call(state, Method::GET, &format!("/admin/users/{}", id), None).await?;
  Ok(
    user
      .get("email")
      .and_then(|v| v.as_str())
      .unwrap_or_default()
      .trim()
      .to_lowercase(),
  )
}

async fn is_admin(state: &AppState, user_uuid: &UserUuid) -> Result<bool, AppError> {
  let admins = admin_emails();
  let email = caller_email(state, user_uuid).await?;
  if email.is_empty() {
    return Ok(false);
  }
  if admins.contains(&email) {
    return Ok(true);
  }
  // CareHub roles: people given the Admin role in Settings > Roles & permissions.
  Ok(super::carehub_extras::is_assigned_admin(&state.pg_pool, &email).await)
}

async fn require_admin(state: &AppState, user_uuid: &UserUuid) -> Result<(), AppError> {
  if is_admin(state, user_uuid).await? {
    Ok(())
  } else {
    Err(AppError::NotEnoughPermissions)
  }
}

fn check_password(password: &str) -> Result<(), AppError> {
  if password.chars().count() < MIN_PASSWORD_LEN {
    return Err(AppError::InvalidPassword(format!(
      "Password must be at least {} characters",
      MIN_PASSWORD_LEN
    )));
  }
  Ok(())
}

/// Characters that flip the direction of the text around them (they can make a name
/// look different from what it is), so they are left out of names.
fn is_bidi_control(c: char) -> bool {
  matches!(c, '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}')
}

/// The name sent when an account is made: control characters removed, spaces trimmed
/// and runs of spaces (or tabs and line breaks) made into one space.
/// `None` when no name was given or nothing is left, which means "no name", as before.
fn clean_name(raw: Option<&str>) -> Result<Option<String>, AppError> {
  let raw = match raw {
    Some(raw) => raw,
    None => return Ok(None),
  };
  let kept: String = raw
    .chars()
    .filter(|c| c.is_whitespace() || !(c.is_control() || is_bidi_control(*c)))
    .collect();
  let name = kept.split_whitespace().collect::<Vec<_>>().join(" ");
  if name.is_empty() {
    return Ok(None);
  }
  if name.chars().count() > MAX_NAME_CHARS {
    return Err(AppError::InvalidRequest(format!(
      "The name can be at most {} characters.",
      MAX_NAME_CHARS
    )));
  }
  Ok(Some(name))
}

/// The rules people see in the app: 8+ characters, upper and lower case, a symbol.
fn check_strong_password(password: &str) -> Result<(), AppError> {
  fn bad(msg: &str) -> Result<(), AppError> {
    Err(AppError::InvalidPassword(msg.to_string()))
  }
  if password.chars().count() < 8 {
    return bad("The password needs at least 8 characters.");
  }
  if !password.chars().any(|c| c.is_uppercase()) {
    return bad("The password needs an uppercase letter.");
  }
  if !password.chars().any(|c| c.is_lowercase()) {
    return bad("The password needs a lowercase letter.");
  }
  if !password
    .chars()
    .any(|c| !c.is_alphanumeric() && !c.is_whitespace())
  {
    return bad("The password needs a symbol (for example ! ? # or -).");
  }
  Ok(())
}

// ---------------------------------------------------------------------------
// "Choose a new password" at first sign-in
//
// Accounts made in Settings > User accounts, and accounts whose password the
// administrator set, are listed in `carehub_password_change`. While listed, the
// app asks the person to choose their own password before they can continue.
// ---------------------------------------------------------------------------

async fn ensure_password_change_table(pool: &sqlx::PgPool) -> Result<(), AppError> {
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_password_change (\
       user_id UUID PRIMARY KEY,\
       created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
  )
  .execute(pool)
  .await?;
  Ok(())
}

async fn require_password_change(pool: &sqlx::PgPool, user_id: &Uuid) {
  let result = async {
    ensure_password_change_table(pool).await?;
    sqlx::query(
      "INSERT INTO carehub_password_change (user_id) VALUES ($1) \
       ON CONFLICT (user_id) DO UPDATE SET created_at = NOW()",
    )
    .bind(user_id)
    .execute(pool)
    .await?;
    Ok::<(), AppError>(())
  }
  .await;
  if let Err(err) = result {
    tracing::warn!("carehub_admin: could not mark password change: {}", err);
  }
}

async fn clear_password_change(pool: &sqlx::PgPool, user_id: &Uuid) {
  let result = async {
    ensure_password_change_table(pool).await?;
    sqlx::query("DELETE FROM carehub_password_change WHERE user_id = $1")
      .bind(user_id)
      .execute(pool)
      .await?;
    Ok::<(), AppError>(())
  }
  .await;
  if let Err(err) = result {
    tracing::warn!("carehub_admin: could not clear password change: {}", err);
  }
}

/// GET /api/carehub/account/password-status -> { "must_change": bool }
async fn password_status_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let me: Uuid = *user_uuid;
  ensure_password_change_table(&state.pg_pool).await?;
  let must_change: bool = sqlx::query_scalar(
    "SELECT EXISTS (SELECT 1 FROM carehub_password_change WHERE user_id = $1)",
  )
  .bind(me)
  .fetch_one(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "must_change": must_change }))))
}

/// POST /api/carehub/account/password  body: { "password" }
/// The signed-in person chooses their own new password.
async fn change_own_password_handler(
  user_uuid: UserUuid,
  body: Json<SetPasswordBody>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<()>>> {
  let me: Uuid = *user_uuid;
  let body = body.into_inner();
  check_strong_password(&body.password)?;
  gotrue_admin_call(
    &state,
    Method::PUT,
    &format!("/admin/users/{}", me),
    Some(json!({ "password": body.password })),
  )
  .await?;
  clear_password_change(&state.pg_pool, &me).await;
  Ok(Json(AppResponse::Ok()))
}

// ---------------------------------------------------------------------------
// Workspace membership without email invitations
//
// A person can only be added to a workspace after their first sign-in (that is
// when the server creates their profile). Until then the join is remembered in
// `carehub_pending_member` and applied automatically at the first sign-in.
// ---------------------------------------------------------------------------

async fn ensure_pending_table(pool: &sqlx::PgPool) -> Result<(), AppError> {
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_pending_member (\
       email TEXT NOT NULL,\
       workspace_id UUID NOT NULL,\
       created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\
       PRIMARY KEY (email, workspace_id))",
  )
  .execute(pool)
  .await?;
  Ok(())
}

/// Puts the name on the person's Care Hub profile, if they already have one.
/// (A new account usually doesn't: the profile is made at the first sign-in,
/// from the name saved with the sign-in account.)
async fn set_profile_name(pool: &sqlx::PgPool, id: &Uuid, name: &str) {
  if let Err(err) = sqlx::query("UPDATE af_user SET name = $1 WHERE uuid = $2")
    .bind(name)
    .bind(id)
    .execute(pool)
    .await
  {
    tracing::warn!("carehub_admin: could not save the name on the profile: {}", err);
  }
}

/// The sign-in id -> server profile (uid, email), if the person has signed in before.
async fn profile_for(pool: &sqlx::PgPool, id: &Uuid) -> Result<Option<(i64, String)>, AppError> {
  let row = sqlx::query_as::<_, (i64, String)>("SELECT uid, email FROM af_user WHERE uuid = $1")
    .bind(id)
    .fetch_optional(pool)
    .await?;
  Ok(row)
}

async fn add_member(state: &AppState, uid: i64, workspace_id: &Uuid) -> Result<(), AppError> {
  let role_id: i32 = AFRole::Member.into();
  sqlx::query(
    "INSERT INTO af_workspace_member (workspace_id, uid, role_id) VALUES ($1, $2, $3) \
     ON CONFLICT (workspace_id, uid) DO NOTHING",
  )
  .bind(workspace_id)
  .bind(uid)
  .bind(role_id)
  .execute(&state.pg_pool)
  .await?;
  state
    .workspace_access_control
    .insert_role(&uid, workspace_id, AFRole::Member)
    .await?;
  Ok(())
}

async fn remember_join(state: &AppState, email: &str, workspace_id: &Uuid) -> Result<(), AppError> {
  ensure_pending_table(&state.pg_pool).await?;
  sqlx::query(
    "INSERT INTO carehub_pending_member (email, workspace_id) VALUES ($1, $2) \
     ON CONFLICT (email, workspace_id) DO NOTHING",
  )
  .bind(email.trim().to_lowercase())
  .bind(workspace_id)
  .execute(&state.pg_pool)
  .await?;
  Ok(())
}

/// Called right after a person's first sign-in: applies any remembered joins.
pub async fn apply_pending_workspace_joins(state: &AppState, uid: i64, email: &str) {
  if let Err(err) = apply_pending_inner(state, uid, email).await {
    tracing::warn!("carehub_admin: could not apply pending workspace join: {}", err);
  }
}

async fn apply_pending_inner(state: &AppState, uid: i64, email: &str) -> Result<(), AppError> {
  ensure_pending_table(&state.pg_pool).await?;
  let workspaces: Vec<Uuid> = sqlx::query_scalar(
    "DELETE FROM carehub_pending_member WHERE email = $1 RETURNING workspace_id",
  )
  .bind(email.trim().to_lowercase())
  .fetch_all(&state.pg_pool)
  .await?;
  for workspace_id in workspaces {
    add_member(state, uid, &workspace_id).await?;
  }
  Ok(())
}

/// The administrator must own the workspace they add people to.
async fn require_owner(
  state: &AppState,
  user_uuid: &UserUuid,
  workspace_id: &Uuid,
) -> Result<(), AppError> {
  let uid = state.user_cache.get_user_uid(user_uuid).await?;
  state
    .workspace_access_control
    .enforce_role_weak(&uid, workspace_id, AFRole::Owner)
    .await?;
  Ok(())
}

fn slim_user(user: &Value) -> Value {
  json!({
    "id": user.get("id").cloned().unwrap_or(Value::Null),
    "email": user.get("email").cloned().unwrap_or(Value::Null),
    "created_at": user.get("created_at").cloned().unwrap_or(Value::Null),
    "last_sign_in_at": user.get("last_sign_in_at").cloned().unwrap_or(Value::Null),
  })
}

/// The name saved with the sign-in account when it was made (user_metadata.name),
/// tidied like when it was saved. Empty when there is none.
fn saved_name(user: &Value) -> String {
  let raw = user
    .get("user_metadata")
    .and_then(|m| m.get("name"))
    .and_then(|v| v.as_str());
  clean_name(raw).ok().flatten().unwrap_or_default()
}

/// GET /api/carehub/admin/me -> { "is_admin": bool }
async fn admin_me_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let admin = match is_admin(&state, &user_uuid).await {
    Ok(admin) => admin,
    Err(err) => {
      tracing::warn!("carehub_admin: admin check failed: {}", err);
      false
    },
  };
  Ok(Json(AppResponse::Ok().with_data(json!({ "is_admin": admin }))))
}

/// GET /api/carehub/admin/users -> { "users": [...] }
/// "name" is the profile name; before the first sign-in it is the name saved when the account was made.
async fn list_users_handler(
  user_uuid: UserUuid,
  query: web::Query<WorkspaceQuery>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  require_admin(&state, &user_uuid).await?;
  let mut users: Vec<Value> = Vec::new();
  for page in 1..=20 {
    let answer = gotrue_admin_call(
      &state,
      Method::GET,
      &format!("/admin/users?page={}&per_page=100", page),
      None,
    )
    .await?;
    let batch = answer
      .get("users")
      .and_then(|v| v.as_array())
      .cloned()
      .unwrap_or_default();
    let count = batch.len();
    users.extend(batch.iter().map(|user| {
      let mut slim = slim_user(user);
      // Shown until the first sign-in; after that the profile name is used (below).
      slim["name"] = json!(saved_name(user));
      slim
    }));
    if count < 100 {
      break;
    }
  }
  // Mark who has signed in, who is in the current workspace, who will join.
  // Profile name and picture come from the user's own profile settings.
  let profiles: Vec<(Uuid, String, String)> = sqlx::query_as(
    "SELECT uuid, COALESCE(name, ''), COALESCE(metadata->>'icon_url', '') FROM af_user",
  )
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  let signed_in: Vec<Uuid> = profiles.iter().map(|(id, _, _)| *id).collect();
  let (members, pending): (Vec<Uuid>, Vec<String>) = match query.workspace_id {
    Some(workspace_id) => {
      let members: Vec<Uuid> = sqlx::query_scalar(
        "SELECT u.uuid FROM af_workspace_member m JOIN af_user u ON u.uid = m.uid \
         WHERE m.workspace_id = $1",
      )
      .bind(workspace_id)
      .fetch_all(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
      ensure_pending_table(&state.pg_pool).await?;
      let pending: Vec<String> =
        sqlx::query_scalar("SELECT email FROM carehub_pending_member WHERE workspace_id = $1")
          .bind(workspace_id)
          .fetch_all(&state.pg_pool)
          .await
          .map_err(AppError::from)?;
      (members, pending)
    },
    None => (Vec::new(), Vec::new()),
  };
  for user in users.iter_mut() {
    let id = user
      .get("id")
      .and_then(|v| v.as_str())
      .and_then(|v| Uuid::parse_str(v).ok());
    let email = user
      .get("email")
      .and_then(|v| v.as_str())
      .unwrap_or_default()
      .to_lowercase();
    let has_signed_in = id.map(|id| signed_in.contains(&id)).unwrap_or(false);
    let in_workspace = id.map(|id| members.contains(&id)).unwrap_or(false);
    user["has_signed_in"] = json!(has_signed_in);
    user["in_workspace"] = json!(in_workspace);
    user["joins_workspace"] = json!(!in_workspace && pending.contains(&email));
    let profile = id.and_then(|id| profiles.iter().find(|(uuid, _, _)| *uuid == id));
    if let Some((_, name, _)) = profile {
      user["name"] = json!(name.trim());
    }
    user["avatar"] = json!(profile.map(|(_, _, icon)| icon.clone()).unwrap_or_default());
  }
  users.sort_by(|a, b| {
    let ea = a.get("email").and_then(|v| v.as_str()).unwrap_or_default();
    let eb = b.get("email").and_then(|v| v.as_str()).unwrap_or_default();
    ea.cmp(eb)
  });
  Ok(Json(AppResponse::Ok().with_data(json!({ "users": users }))))
}

/// POST /api/carehub/admin/users  body: { "email", "password", "name"?, "workspace_id"? }
/// "name" (optional, up to 100 characters) is saved with the sign-in account as
/// user_metadata.name, so it becomes the person's name at their first sign-in.
async fn create_user_handler(
  user_uuid: UserUuid,
  body: Json<CreateUserBody>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  require_admin(&state, &user_uuid).await?;
  let body = body.into_inner();
  let email = body.email.trim().to_lowercase();
  if email.is_empty() || !email.contains('@') || email.contains(' ') {
    return Err(AppError::InvalidEmail("Please enter a valid email address".to_string()).into());
  }
  check_password(&body.password)?;
  let name = clean_name(body.name.as_deref())?;
  let mut new_account = json!({
    "email": email,
    "password": body.password,
    "email_confirm": true,
  });
  if let Some(name) = &name {
    new_account["user_metadata"] = json!({ "name": name });
  }
  let user = gotrue_admin_call(&state, Method::POST, "/admin/users", Some(new_account)).await?;
  if let Some(new_id) = user
    .get("id")
    .and_then(|v| v.as_str())
    .and_then(|v| Uuid::parse_str(v).ok())
  {
    require_password_change(&state.pg_pool, &new_id).await;
    if let Some(name) = &name {
      set_profile_name(&state.pg_pool, &new_id, name).await;
    }
  }
  if let Some(workspace_id) = body.workspace_id {
    require_owner(&state, &user_uuid, &workspace_id).await?;
    remember_join(&state, &email, &workspace_id).await?;
  }
  Ok(Json(AppResponse::Ok().with_data(slim_user(&user))))
}

/// POST /api/carehub/admin/users/{user_id}/workspace  body: { "workspace_id" }
/// Adds the person to the workspace now, or at their first sign-in.
async fn join_workspace_handler(
  user_uuid: UserUuid,
  path: web::Path<Uuid>,
  body: Json<WorkspaceBody>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  require_admin(&state, &user_uuid).await?;
  let target = path.into_inner();
  let workspace_id = body.into_inner().workspace_id;
  require_owner(&state, &user_uuid, &workspace_id).await?;
  if let Some((uid, _email)) = profile_for(&state.pg_pool, &target).await? {
    add_member(&state, uid, &workspace_id).await?;
    return Ok(Json(AppResponse::Ok().with_data(json!({ "joined": true }))));
  }
  let user = gotrue_admin_call(&state, Method::GET, &format!("/admin/users/{}", target), None).await?;
  let email = user
    .get("email")
    .and_then(|v| v.as_str())
    .unwrap_or_default()
    .to_string();
  if email.is_empty() {
    return Err(AppError::RecordNotFound("Account not found".to_string()).into());
  }
  remember_join(&state, &email, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "joined": false }))))
}

/// DELETE /api/carehub/admin/users/{user_id}/workspace?workspace_id=...
/// Takes the person out of the workspace (their account stays).
async fn leave_workspace_handler(
  user_uuid: UserUuid,
  path: web::Path<Uuid>,
  query: web::Query<WorkspaceQuery>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<()>>> {
  require_admin(&state, &user_uuid).await?;
  let target = path.into_inner();
  let workspace_id = query
    .workspace_id
    .ok_or_else(|| AppError::InvalidRequest("workspace_id is missing".to_string()))?;
  require_owner(&state, &user_uuid, &workspace_id).await?;
  let me: Uuid = *user_uuid;
  if target == me {
    return Err(AppError::InvalidRequest("You can't remove yourself from your workspace".to_string()).into());
  }
  if let Some((uid, email)) = profile_for(&state.pg_pool, &target).await? {
    sqlx::query(
      "DELETE FROM af_workspace_member WHERE workspace_id = $1 AND uid = $2 \
       AND uid <> (SELECT owner_uid FROM af_workspace WHERE workspace_id = $1)",
    )
    .bind(workspace_id)
    .bind(uid)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
    state
      .workspace_access_control
      .remove_user_from_workspace(&uid, &workspace_id)
      .await?;
    ensure_pending_table(&state.pg_pool).await?;
    sqlx::query("DELETE FROM carehub_pending_member WHERE email = $1 AND workspace_id = $2")
      .bind(email.trim().to_lowercase())
      .bind(workspace_id)
      .execute(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
  } else {
    let user = gotrue_admin_call(&state, Method::GET, &format!("/admin/users/{}", target), None).await?;
    let email = user.get("email").and_then(|v| v.as_str()).unwrap_or_default();
    ensure_pending_table(&state.pg_pool).await?;
    sqlx::query("DELETE FROM carehub_pending_member WHERE email = $1 AND workspace_id = $2")
      .bind(email.trim().to_lowercase())
      .bind(workspace_id)
      .execute(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
  }
  Ok(Json(AppResponse::Ok()))
}

/// PUT /api/carehub/admin/users/{user_id}/password  body: { "password" }
async fn set_password_handler(
  user_uuid: UserUuid,
  path: web::Path<Uuid>,
  body: Json<SetPasswordBody>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<()>>> {
  require_admin(&state, &user_uuid).await?;
  let target = path.into_inner();
  let body = body.into_inner();
  check_password(&body.password)?;
  gotrue_admin_call(
    &state,
    Method::PUT,
    &format!("/admin/users/{}", target),
    Some(json!({ "password": body.password })),
  )
  .await?;
  // Someone else's password: they choose their own at the next sign-in.
  let me: Uuid = *user_uuid;
  if target == me {
    clear_password_change(&state.pg_pool, &me).await;
  } else {
    require_password_change(&state.pg_pool, &target).await;
  }
  Ok(Json(AppResponse::Ok()))
}

/// DELETE /api/carehub/admin/users/{user_id}
/// Removes the sign-in account. Pages the person created stay in the workspace.
async fn delete_user_handler(
  user_uuid: UserUuid,
  path: web::Path<Uuid>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<()>>> {
  require_admin(&state, &user_uuid).await?;
  let target = path.into_inner();
  let me: Uuid = *user_uuid;
  if target == me {
    return Err(AppError::InvalidRequest("You can't remove your own account".to_string()).into());
  }
  gotrue_admin_call(
    &state,
    Method::DELETE,
    &format!("/admin/users/{}", target),
    Some(json!({ "should_soft_delete": false })),
  )
  .await?;
  clear_password_change(&state.pg_pool, &target).await;
  // Free the email so the same address can get a new account later.
  // The person's pages and their name on them are kept.
  if let Err(err) = sqlx::query(
    "UPDATE af_user SET email = email || '.removed-' || uid::text WHERE uuid = $1",
  )
  .bind(target)
  .execute(&state.pg_pool)
  .await
  {
    tracing::warn!("carehub_admin: could not free email of removed account: {}", err);
  }
  Ok(Json(AppResponse::Ok()))
}

#[cfg(test)]
mod tests {
  use super::{clean_name, saved_name};
  use serde_json::json;

  fn clean(raw: &str) -> Option<String> {
    clean_name(Some(raw)).ok().flatten()
  }

  #[test]
  fn names_are_tidied() {
    assert_eq!(clean_name(None).ok().flatten(), None);
    assert_eq!(clean("  Jordan   Lee "), Some("Jordan Lee".to_string()));
    assert_eq!(clean("Jordan\tLee\r\n"), Some("Jordan Lee".to_string()));
    assert_eq!(clean("Jor\u{0}dan\u{7} Lee\u{1b}"), Some("Jordan Lee".to_string()));
    assert_eq!(clean("\u{202E}Jordan Lee\u{2066}"), Some("Jordan Lee".to_string()));
    assert_eq!(clean("José Núñez-O'Brien"), Some("José Núñez-O'Brien".to_string()));
    assert_eq!(clean("   "), None);
    assert_eq!(clean("\u{0}\u{1f}"), None);
  }

  #[test]
  fn names_up_to_100_characters() {
    assert_eq!(clean(&"é".repeat(100)).map(|n| n.chars().count()), Some(100));
    assert!(clean_name(Some("a".repeat(101).as_str())).is_err());
    assert_eq!(clean(&format!("  {}  ", "a".repeat(100))).map(|n| n.len()), Some(100));
  }

  #[test]
  fn saved_name_from_the_sign_in_account() {
    assert_eq!(saved_name(&json!({ "user_metadata": { "name": "  Jordan   Lee " } })), "Jordan Lee");
    assert_eq!(saved_name(&json!({ "user_metadata": {} })), "");
    assert_eq!(saved_name(&json!({ "id": "x" })), "");
    assert_eq!(saved_name(&json!({ "user_metadata": { "name": 7 } })), "");
    assert_eq!(saved_name(&json!({ "user_metadata": { "name": "a".repeat(101) } })), "");
  }
}
