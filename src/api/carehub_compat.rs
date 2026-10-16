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
use crate::biz::workspace::ops::get_workspace_mentionable_person;
use crate::biz::workspace::page_view::update_space;
use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::HttpRequest;
use actix_web::Result as ActixResult;
use app_error::AppError;
use database_entity::dto::AFRole;
use serde::Deserialize;
use serde_json::{json, Value};
use database::collab::select_collab_id_exists;
use shared_entity::dto::workspace_dto::{FolderView, SpacePermission};
use shared_entity::response::AppResponse;
use uuid::Uuid;

/// The server refuses folder requests deeper than 10 levels.
const MAX_FOLDER_DEPTH: u32 = 10;
const DEFAULT_DATABASE_PAGE_SIZE: usize = 200;

/// Registers the compatibility routes. Must be registered before
/// `workspace_scope()` so these exact paths are matched first.
pub fn configure(cfg: &mut web::ServiceConfig) {
  // CareHub forms (and adding any database view type) – see carehub_forms.rs
  super::carehub_forms::configure(cfg);
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
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/collab/{object_id}/permission")
        .route(web::get().to(collab_permission_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/spaces/{space_id}/permission")
        .route(web::get().to(space_permission_handler)),
    )
    // --- batch 2 ---
    .service(
      web::resource("/api/workspace/{workspace_id}/collab/{object_id}/collab-exists")
        .route(web::get().to(collab_exists_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/workspace-profile")
        .route(web::get().to(workspace_profile_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/view/{view_id}/navigation")
        .route(web::get().to(view_navigation_handler)),
    )
    .service(
      web::resource("/api/sharing/workspace/{workspace_id}/view/{view_id}")
        .route(web::get().to(shared_with_me_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/document/{view_id}/inline-comment")
        .route(web::get().to(inline_comments_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/document/{view_id}/inline-comment/reaction")
        .route(web::get().to(inline_comment_reactions_handler)),
    )
    .service(
      web::resource(
        "/api/workspace/{workspace_id}/database/{view_id}/row/{row_id}/comment/notification",
      )
      .route(web::post().to(row_comment_notification_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/notifications")
        .route(web::get().to(notifications_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/notifications/unread-count")
        .route(web::get().to(notifications_unread_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/notifications/{action}")
        .route(web::post().to(notifications_action_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/spaces/{space_id}")
        .route(web::patch().to(update_structured_space_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/spaces/{space_id}/members")
        .route(web::get().to(space_members_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/groups")
        .route(web::get().to(workspace_groups_handler)),
    )
    .service(
      web::resource("/api/workspace/{workspace_id}/page-view/{view_id}/upgrade-database-container")
        .route(web::get().to(upgrade_database_container_status_handler)),
    )
    .service(web::resource("/api/server-info").route(web::get().to(server_info_handler)))
    .service(
      web::resource("/api/integrations/connections")
        .route(web::get().to(integration_connections_handler)),
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
      let mut views: Vec<Value> = database
        .views
        .into_iter()
        .map(|view| {
          json!({
            "view_id": view.view_id,
            "name": view.name,
            "icon": view.icon,
            "layout": view.layout,
            "is_container": false,
            "embedded": false,
            "parent_view_id": Value::Null,
          })
        })
        .collect();
      // Databases on this server have no separate "container" page. The web
      // client only offers databases with a container in the relation picker,
      // so the first view also stands in as the container.
      if let Some(first) = views.first().cloned() {
        let mut container = first;
        container["is_container"] = json!(true);
        views.insert(0, container);
      }
      json!({ "database_id": database.id, "views": views })
    })
    .collect();
  let has_more = offset.saturating_add(limit) < total;
  Ok(Json(AppResponse::Ok().with_data(
    json!({ "databases": page, "has_more": has_more }),
  )))
}

/// GET /api/workspace/{workspace_id}/database-view-creation-status
/// Forms are provided by carehub_forms.rs; charts are not available yet.
async fn database_view_creation_status_handler(
  _user_uuid: UserUuid,
  _workspace_id: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  Ok(Json(AppResponse::Ok().with_data(json!({
    "can_create_form": true,
    "can_create_chart": false,
  }))))
}

// ---------------------------------------------------------------------------
// Permission answers
//
// The web client asks the server what the signed-in user may do with a page
// before it unlocks the editor. This server has no per-page permissions, so
// the answer follows the user's workspace role, exactly like the rest of the
// server: owners and members can edit, guests can only read, and anyone
// outside the workspace is refused (403).
// ---------------------------------------------------------------------------

/// Access levels used by the web client.
const ACCESS_READ_ONLY: i32 = 10;
const ACCESS_READ_AND_WRITE: i32 = 30;
const ACCESS_FULL: i32 = 50;

#[derive(Debug, Deserialize)]
struct CollabPermissionQuery {
  collab_type: Option<i32>,
}

/// Returns the user's role in the workspace, or an error if they are not in it.
async fn workspace_role(
  state: &AppState,
  user_uuid: &UserUuid,
  workspace_id: &Uuid,
) -> Result<AFRole, AppError> {
  let uid = state.user_cache.get_user_uid(user_uuid).await?;
  let access = &state.workspace_access_control;
  if access
    .enforce_role_weak(&uid, workspace_id, AFRole::Owner)
    .await
    .is_ok()
  {
    return Ok(AFRole::Owner);
  }
  if access
    .enforce_role_weak(&uid, workspace_id, AFRole::Member)
    .await
    .is_ok()
  {
    return Ok(AFRole::Member);
  }
  access
    .enforce_role_weak(&uid, workspace_id, AFRole::Guest)
    .await?;
  Ok(AFRole::Guest)
}

/// GET /api/workspace/{workspace_id}/collab/{object_id}/permission?collab_type=N
async fn collab_permission_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  query: web::Query<CollabPermissionQuery>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, object_id) = path.into_inner();
  let role = workspace_role(&state, &user_uuid, &workspace_id).await?;
  let (access_level, can_write, can_share) = match role {
    AFRole::Owner => (ACCESS_FULL, true, true),
    AFRole::Member => (ACCESS_READ_AND_WRITE, true, true),
    AFRole::Guest => (ACCESS_READ_ONLY, false, false),
  };
  Ok(Json(AppResponse::Ok().with_data(json!({
    "object_id": object_id.to_string(),
    "collab_type": query.collab_type.unwrap_or(0),
    "governing_view_id": object_id.to_string(),
    "access_level": access_level,
    "can_read": true,
    "can_write": can_write,
    "can_comment": can_write,
    "can_share": can_share,
  }))))
}

/// GET /api/workspace/{workspace_id}/spaces/{space_id}/permission
/// Spaces on this server are always open to every workspace member.
async fn space_permission_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, space_id) = path.into_inner();
  let role = workspace_role(&state, &user_uuid, &workspace_id).await?;
  // The person who created a space may also manage it (rename, icon).
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let space = load_view(&state, &req, &user_uuid, workspace_id, space_id, 0)
    .await
    .ok();
  let is_creator = space
    .as_ref()
    .and_then(|v| v.created_by)
    .map(|created_by| created_by == uid)
    .unwrap_or(false);
  let is_private = space
    .as_ref()
    .and_then(|v| v.extra.as_ref())
    .and_then(|e| e.get("space_permission"))
    .and_then(|v| v.as_i64())
    == Some(1);
  let visibility = if is_private { "private" } else { "public" };
  let is_owner = matches!(role, AFRole::Owner) || (is_creator && !matches!(role, AFRole::Guest));
  let is_editor = !matches!(role, AFRole::Guest);
  let current_level = match role {
    AFRole::Owner => ACCESS_FULL,
    AFRole::Member => ACCESS_READ_AND_WRITE,
    AFRole::Guest => ACCESS_READ_ONLY,
  };
  Ok(Json(AppResponse::Ok().with_data(json!({
    "space_id": space_id.to_string(),
    "permission": {
      "visibility": visibility,
      "owner_access_level": ACCESS_FULL,
      "member_default_access_level": ACCESS_READ_AND_WRITE,
      "everyone_else_access_level": Value::Null,
      "invite_policy": "members_and_owners",
      "sidebar_edit_policy": "members_and_owners",
      "invite_link_enabled": false,
      "security": {
        "disable_guests": false,
        "disable_public_links": true,
        "disable_export": false,
      },
    },
    "current_user_access_level": current_level,
    "can_manage_space": is_owner,
    "can_manage_members": is_owner,
    "can_invite_members": is_editor,
    "can_edit_sidebar": is_editor,
    "explicit_member_count": 0,
  }))))
}

// ---------------------------------------------------------------------------
// Batch 2: profile, row pages, comments, notifications, spaces
// ---------------------------------------------------------------------------

/// GET /api/workspace/{workspace_id}/collab/{object_id}/collab-exists
/// Used before opening the notes area of a database row.
async fn collab_exists_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, object_id) = path.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  let exists = select_collab_id_exists(&state.pg_pool, &object_id)
    .await
    .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "exists": exists }))))
}

/// GET /api/workspace/{workspace_id}/workspace-profile
/// The signed-in user's own profile in this workspace.
async fn workspace_profile_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let workspace_id = workspace_id.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let person_id: Uuid = *user_uuid;
  let person = get_workspace_mentionable_person(&state.pg_pool, &workspace_id, &person_id).await?;
  let mut value = serde_json::to_value(&person)
    .map_err(|err| AppError::Internal(anyhow::anyhow!("profile encode failed: {}", err)))?;
  if let Some(obj) = value.as_object_mut() {
    obj.insert("uid".to_string(), json!(uid.to_string()));
    obj.insert("person_id".to_string(), json!(person_id.to_string()));
    obj
      .entry("last_mentioned_at".to_string())
      .or_insert(Value::Null);
  }
  Ok(Json(AppResponse::Ok().with_data(value)))
}

/// GET /api/workspace/{workspace_id}/view/{view_id}/navigation
/// Returns the workspace tree; the web client finds the path to the view in it.
async fn view_navigation_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<FolderView>>> {
  let (workspace_id, _view_id) = path.into_inner();
  let tree = load_view(
    &state,
    &req,
    &user_uuid,
    workspace_id,
    workspace_id,
    MAX_FOLDER_DEPTH,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(tree)))
}

/// GET /api/sharing/workspace/{workspace_id}/view/{view_id}
/// "Shared with me": this server has no per-page sharing, so it is empty.
async fn shared_with_me_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<FolderView>>> {
  let (workspace_id, view_id) = path.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  let root = FolderView {
    view_id,
    name: "Shared with me".to_string(),
    ..Default::default()
  };
  Ok(Json(AppResponse::Ok().with_data(root)))
}

/// GET .../document/{view_id}/inline-comment (no margin comments stored yet)
async fn inline_comments_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, _view_id) = path.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "comments": [],
    "has_more": false,
    "next_offset": Value::Null,
  }))))
}

/// GET .../document/{view_id}/inline-comment/reaction
async fn inline_comment_reactions_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, _view_id) = path.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "reactions": [] }))))
}

/// POST .../row/{row_id}/comment/notification
/// Row comments are saved inside the row itself; this only acknowledges the
/// "notify people" step (CareHub has no notification emails yet).
async fn row_comment_notification_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid, Uuid)>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<()>>> {
  let (workspace_id, _view_id, _row_id) = path.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok()))
}

/// GET /api/workspace/{workspace_id}/notifications
async fn notifications_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let workspace_id = workspace_id.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "notifications": [],
    "has_more": false,
  }))))
}

/// GET /api/workspace/{workspace_id}/notifications/unread-count
async fn notifications_unread_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let workspace_id = workspace_id.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "unread_count": 0 }))))
}

/// POST /api/workspace/{workspace_id}/notifications/{read|read-all|archive|archive-all}
async fn notifications_action_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, String)>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<()>>> {
  let (workspace_id, _action) = path.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok()))
}

#[derive(Debug, Deserialize)]
struct UpdateStructuredSpaceBody {
  name: Option<String>,
  space_icon: Option<String>,
  space_icon_color: Option<String>,
}

fn extra_str(extra: &Option<Value>, key: &str) -> String {
  extra
    .as_ref()
    .and_then(|e| e.get(key))
    .and_then(|v| v.as_str())
    .unwrap_or_default()
    .to_string()
}

/// PATCH /api/workspace/{workspace_id}/spaces/{space_id}
/// Rename a space or change its icon (Manage Space > General).
async fn update_structured_space_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  body: Json<UpdateStructuredSpaceBody>,
  state: Data<AppState>,
  req: HttpRequest,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, space_id) = path.into_inner();
  let body = body.into_inner();
  let current = load_view(&state, &req, &user_uuid, workspace_id, space_id, 0).await?;
  let is_private = current
    .extra
    .as_ref()
    .and_then(|e| e.get("space_permission"))
    .and_then(|v| v.as_i64())
    == Some(1);
  let permission = if is_private {
    SpacePermission::Private
  } else {
    SpacePermission::PublicToAll
  };
  let name = body.name.unwrap_or_else(|| current.name.clone());
  let icon = body
    .space_icon
    .unwrap_or_else(|| extra_str(&current.extra, "space_icon"));
  let color = body
    .space_icon_color
    .unwrap_or_else(|| extra_str(&current.extra, "space_icon_color"));
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  let user = realtime_user_for_web_request(req.headers(), uid)?;
  update_space(
    &state,
    user,
    workspace_id,
    &space_id.to_string(),
    &permission,
    &name,
    &icon,
    &color,
  )
  .await?;
  Ok(Json(AppResponse::Ok().with_data(
    json!({ "view_id": space_id.to_string() }),
  )))
}

/// GET /api/workspace/{workspace_id}/spaces/{space_id}/members
/// Spaces are open to the whole workspace, so there are no extra members.
async fn space_members_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, _space_id) = path.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "members": [], "groups": [] }))))
}

/// GET /api/workspace/{workspace_id}/groups (no groups on this server)
async fn workspace_groups_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let workspace_id = workspace_id.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "groups": [] }))))
}

/// GET .../page-view/{view_id}/upgrade-database-container
/// Older databases stay as they are; nothing to upgrade.
async fn upgrade_database_container_status_handler(
  user_uuid: UserUuid,
  path: web::Path<(Uuid, Uuid)>,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let (workspace_id, _view_id) = path.into_inner();
  workspace_role(&state, &user_uuid, &workspace_id).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "eligible": false,
    "already_upgraded": false,
  }))))
}

// ---------------------------------------------------------------------------
// Batch 3: server information and connections
// ---------------------------------------------------------------------------

/// GET /api/server-info (no sign-in needed)
/// Tells the web app which optional features this server offers. Connections
/// to outside services (Google Drive and similar) are not offered.
async fn server_info_handler(req: HttpRequest) -> ActixResult<Json<AppResponse<Value>>> {
  let platform = req
    .headers()
    .get("x-platform")
    .and_then(|v| v.to_str().ok())
    .unwrap_or_default();
  if platform == "app" {
    return Ok(Json(AppResponse::Ok().with_data(json!({ "connections": [] }))));
  }
  Ok(Json(AppResponse::Ok().with_data(json!({
    "enable_page_history": false,
    "enable_database_history": false,
    "ai_enabled": false,
    "self_hosted": true,
  }))))
}

/// GET /api/integrations/connections?workspace_id=...
/// No outside services are connected on this server.
async fn integration_connections_handler(
  _user_uuid: UserUuid,
) -> ActixResult<Json<AppResponse<Value>>> {
  Ok(Json(AppResponse::Ok().with_data(json!({ "connections": [] }))))
}
