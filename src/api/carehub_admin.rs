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
    );
}

#[derive(Debug, Deserialize)]
struct CreateUserBody {
  email: String,
  password: String,
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
    users.extend(batch.iter().map(slim_user));
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
    user["name"] = json!(profile.map(|(_, name, _)| name.trim().to_string()).unwrap_or_default());
    user["avatar"] = json!(profile.map(|(_, _, icon)| icon.clone()).unwrap_or_default());
  }
  users.sort_by(|a, b| {
    let ea = a.get("email").and_then(|v| v.as_str()).unwrap_or_default();
    let eb = b.get("email").and_then(|v| v.as_str()).unwrap_or_default();
    ea.cmp(eb)
  });
  Ok(Json(AppResponse::Ok().with_data(json!({ "users": users }))))
}

/// POST /api/carehub/admin/users  body: { "email", "password" }
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
  let user = gotrue_admin_call(
    &state,
    Method::POST,
    "/admin/users",
    Some(json!({
      "email": email,
      "password": body.password,
      "email_confirm": true,
    })),
  )
  .await?;
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
