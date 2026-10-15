//! CareHub compatibility routes.
//!
//! CareHub runs the open-source (AGPL) AppFlowy-Cloud server together with the
//! newer open-source AppFlowy-Web client. The web client calls a few routes that
//! this older server does not have. The handlers below answer those requests by
//! reusing the server's existing logic, so no data formats are changed.
//!
//! Every handler requires a signed-in user and keeps the same workspace access
//! checks as the original routes.

use crate::api::util::realtime_user_for_web_request;
use crate::biz;
use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::HttpRequest;
use actix_web::Result as ActixResult;
use app_error::AppError;
use database_entity::dto::AFRole;
use serde::Deserialize;
use serde_json::{json, Value};
use shared_entity::dto::workspace_dto::FolderView;
use shared_entity::response::AppResponse;
use uuid::Uuid;

/// The server refuses folder requests deeper than 10 levels.
const MAX_FOLDER_DEPTH: u32 = 10;
const DEFAULT_DATABASE_PAGE_SIZE: usize = 200;

/// Registers the compatibility routes. Must be registered before
/// `workspace_scope()` so these exact paths are matched first.
pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(
      web::resource("/api/workspace/{workspace_id}/view/{view_id}")
        .route(web::get().to(get_view_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/views")
        .route(web::get().to(get_views_by_query_handler))
        .route(web::post().to(get_views_by_body_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/database")
        .route(web::get().to(list_database_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/database-view-creation-status")
        .route(web::get().to(database_view_creation_status_handler)),
    );
}

#[derive(Debug, Deserialize)]
struct DepthQuery {
  depth: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ViewsQuery {
  view_ids: Option<String>,
  depth: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ViewsBody {
  view_ids: Vec<Uuid>,
  depth: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct DatabaseListQuery {
  offset: Option<usize>,
  limit: Option<usize>,
}

fn clamp_depth(depth: Option<u32>, default_depth: u32) -> u32 {
  depth.unwrap_or(default_depth).min(MAX_FOLDER_DEPTH)
}

/// Loads one view (and its children up to `depth`) for a workspace member.
async fn load_view(
  state: &AppState,
  req: &HttpRequest,
  user_uuid: &UserUuid,
  workspace_id: Uuid,
  view_id: Uuid,
  depth: u32,
) -> Result<FolderView, AppError> {
  let uid = state.user_cache.get_user_uid(user_uuid).await?;
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  state
    .workspace_access_control
    .enforce_role_weak(&uid, &workspace_id, AFRole::Member)
    .await?;
  biz::collab::ops::get_user_workspace_structure(state, user, workspace_id, depth, &view_id).await
}

/// GET /api/workspace/{workspace_id}/view/{view_id}?depth=N
/// Newer web clients load the sidebar (outline) and single views through this route.
async fn get_view_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  query: web::Query<DepthQuery>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<FolderView>>> {
  let (workspace_id, view_id) = path.into_inner();
  let depth = clamp_depth(query.depth, 1);
  let view = load_view(&state, &req, &user_uuid, workspace_id, view_id, depth).await?;
  Ok(Json(AppResponse::Ok().with_data(view)))
}

async fn views_response(
  state: &AppState,
  req: &HttpRequest,
  user_uuid: &UserUuid,
  workspace_id: Uuid,
  view_ids: Vec<Uuid>,
  depth: u32,
) -> ActixResult<Json<AppResponse<Value>>> {
  let mut views: Vec<FolderView> = Vec::with_capacity(view_ids.len());
  for view_id in view_ids {
    match load_view(state, req, user_uuid, workspace_id, view_id, depth).await {
      Ok(view) => views.push(view),
      Err(err) => {
        tracing::warn!("carehub_compat: skipping view {}: {}", view_id, err);
      },
    }
  }
  Ok(Json(AppResponse::Ok().with_data(json!({ "views": views }))))
}

/// GET /api/workspace/{workspace_id}/views?view_ids=a,b,c&depth=N
async fn get_views_by_query_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  query: web::Query<ViewsQuery>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let mut view_ids = Vec::new();
  if let Some(raw) = query.view_ids.as_deref() {
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
      let id = Uuid::parse_str(part)
        .map_err(|_| AppError::InvalidRequest(format!("Invalid view id: {}", part)))?;
      view_ids.push(id);
    }
  }
  let depth = clamp_depth(query.depth, 2);
  views_response(
    &state,
    &req,
    &user_uuid,
    workspace_id.into_inner(),
    view_ids,
    depth,
  )
  .await
}

/// POST /api/workspace/{workspace_id}/views  body: { "view_ids": [...], "depth": N }
async fn get_views_by_body_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  body: Json<ViewsBody>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let body = body.into_inner();
  let depth = clamp_depth(body.depth, 2);
  views_response(
    &state,
    &req,
    &user_uuid,
    workspace_id.into_inner(),
    body.view_ids,
    depth,
  )
  .await
}

/// GET /api/workspace/{workspace_id}/database[?offset=&limit=]
///
/// Without paging parameters this returns the original (legacy) list.
/// With paging parameters it returns the newer paged shape that the web
/// client uses for the relation/rollup database picker:
/// `{ "databases": [{ "database_id", "views": [...] }], "has_more": bool }`.
async fn list_database_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  query: web::Query<DatabaseListQuery>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let workspace_id = workspace_id.into_inner();
  state
    .workspace_access_control
    .enforce_role_weak(&uid, &workspace_id, AFRole::Member)
    .await?;
  let databases = biz::collab::ops::list_database(
    &state.pg_pool,
    &state.ws_server,
    &state.collab_storage,
    uid,
    workspace_id,
  )
  .await?;

  if query.offset.is_none() && query.limit.is_none() {
    return Ok(Json(AppResponse::Ok().with_data(json!(databases))));
  }

  let total = databases.len();
  let offset = query.offset.unwrap_or(0);
  let limit = query.limit.unwrap_or(DEFAULT_DATABASE_PAGE_SIZE).max(1);
  let page: Vec<Value> = databases
    .into_iter()
    .skip(offset)
    .take(limit)
    .map(|database| {
      let views: Vec<Value> = database
        .views
        .into_iter()
        .map(|view| {
          json!({
            "view_id": view.view_id,
            "name": view.name,
            "icon": view.icon,
            "layout": view.layout,
            "is_container": false,
            "parent_view_id": Value::Null,
          })
        })
        .collect();
      json!({ "database_id": database.id, "views": views })
    })
    .collect();
  let has_more = offset.saturating_add(limit) < total;
  Ok(Json(AppResponse::Ok().with_data(
    json!({ "databases": page, "has_more": has_more }),
  )))
}

/// GET /api/workspace/{workspace_id}/database-view-creation-status
/// Form and chart views need server features this server does not have.
async fn database_view_creation_status_handler(
  _user_uuid: UserUuid,
  _workspace_id: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  Ok(Json(AppResponse::Ok().with_data(json!({
    "can_create_form": false,
    "can_create_chart": false,
  }))))
}
