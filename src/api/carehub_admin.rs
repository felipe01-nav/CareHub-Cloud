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
      web::resource("/api/carehub/admin/users/{user_id}")
        .route(web::delete().to(delete_user_handler)),
    );
}

#[derive(Debug, Deserialize)]
struct CreateUserBody {
  email: String,
  password: String,
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
  if admins.is_empty() {
    return Ok(false);
  }
  let email = caller_email(state, user_uuid).await?;
  Ok(!email.is_empty() && admins.contains(&email))
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
  Ok(Json(AppResponse::Ok().with_data(slim_user(&user))))
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
