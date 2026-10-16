//! CareHub: Messages (direct messages and group chats between people who share
//! a workspace).
//!
//! * A chat has two or more members. Only members can read or write in it.
//! * You can only start a chat with people who are in one of your workspaces.
//! * The browser asks for new messages every few seconds; unread counts come
//!   from the time each member last opened the chat.

use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::Result as ActixResult;
use app_error::AppError;
use serde::Deserialize;
use serde_json::{json, Value};
use shared_entity::response::AppResponse;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const MAX_BODY_CHARS: usize = 10_000;
const MAX_MEMBERS: usize = 50;
const PAGE: i64 = 100;

static TABLES_READY: AtomicBool = AtomicBool::new(false);

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg
    .service(web::resource("/api/carehub/messages/people").route(web::get().to(people_handler)))
    .service(web::resource("/api/carehub/messages/unread").route(web::get().to(unread_handler)))
    .service(
      web::resource("/api/carehub/messages/chats")
        .route(web::get().to(list_chats_handler))
        .route(web::post().to(create_chat_handler)),
    )
    .service(
      web::resource("/api/carehub/messages/chats/{chat_id}")
        .route(web::put().to(update_chat_handler)),
    )
    .service(
      web::resource("/api/carehub/messages/chats/{chat_id}/messages")
        .route(web::get().to(list_messages_handler))
        .route(web::post().to(send_message_handler)),
    )
    .service(
      web::resource("/api/carehub/messages/chats/{chat_id}/read")
        .route(web::post().to(read_handler)),
    )
    .service(
      web::resource("/api/carehub/messages/message/{message_id}")
        .route(web::put().to(edit_message_handler))
        .route(web::delete().to(delete_message_handler)),
    );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn bad(msg: &str) -> AppError {
  AppError::InvalidRequest(msg.to_string())
}

fn clip(s: &str, max: usize) -> String {
  s.chars().take(max).collect()
}

fn now_ms() -> i64 {
  std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map(|d| d.as_millis() as i64)
    .unwrap_or(0)
}

async fn ensure_tables(pool: &sqlx::PgPool) -> Result<(), AppError> {
  if TABLES_READY.load(Ordering::Relaxed) {
    return Ok(());
  }
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_chat (\
       id UUID PRIMARY KEY,\
       name TEXT NOT NULL DEFAULT '',\
       is_group BOOLEAN NOT NULL DEFAULT FALSE,\
       created_by BIGINT NOT NULL,\
       created_ms BIGINT NOT NULL,\
       last_ms BIGINT NOT NULL)",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_chat_member (\
       chat_id UUID NOT NULL REFERENCES carehub_chat(id) ON DELETE CASCADE,\
       uid BIGINT NOT NULL,\
       read_ms BIGINT NOT NULL DEFAULT 0,\
       joined_ms BIGINT NOT NULL DEFAULT 0,\
       PRIMARY KEY (chat_id, uid))",
  )
  .execute(pool)
  .await?;
  sqlx::query("CREATE INDEX IF NOT EXISTS carehub_chat_member_uid ON carehub_chat_member (uid)")
    .execute(pool)
    .await?;
  sqlx::query(
    "CREATE TABLE IF NOT EXISTS carehub_chat_message (\
       id UUID PRIMARY KEY,\
       chat_id UUID NOT NULL REFERENCES carehub_chat(id) ON DELETE CASCADE,\
       sender BIGINT NOT NULL,\
       body TEXT NOT NULL DEFAULT '',\
       created_ms BIGINT NOT NULL,\
       edited_ms BIGINT NOT NULL DEFAULT 0,\
       deleted BOOLEAN NOT NULL DEFAULT FALSE)",
  )
  .execute(pool)
  .await?;
  sqlx::query(
    "CREATE INDEX IF NOT EXISTS carehub_chat_message_chat_time \
     ON carehub_chat_message (chat_id, created_ms)",
  )
  .execute(pool)
  .await?;
  TABLES_READY.store(true, Ordering::Relaxed);
  Ok(())
}

async fn me(state: &AppState, user_uuid: &UserUuid) -> Result<i64, AppError> {
  ensure_tables(&state.pg_pool).await?;
  Ok(state.user_cache.get_user_uid(user_uuid).await?)
}

async fn require_member(state: &AppState, chat_id: Uuid, uid: i64) -> Result<(), AppError> {
  let found: Option<i64> =
    sqlx::query_scalar("SELECT uid FROM carehub_chat_member WHERE chat_id = $1 AND uid = $2")
      .bind(chat_id)
      .bind(uid)
      .fetch_optional(&state.pg_pool)
      .await?;
  match found {
    Some(_) => Ok(()),
    None => Err(AppError::RecordNotFound("Chat not found".to_string())),
  }
}

/// (uid, uuid, name, email, icon)
type PersonRow = (i64, Uuid, String, String, String);

fn person_json(p: &PersonRow) -> Value {
  json!({ "uuid": p.1, "name": p.2, "email": p.3, "avatar": p.4 })
}

const PERSON_COLUMNS: &str = "u.uid, u.uuid, COALESCE(u.name, ''), COALESCE(u.email, ''), \
   COALESCE(u.metadata ->> 'icon_url', '')";

/// Everyone who shares at least one workspace with `uid` (not including them).
async fn people_for(state: &AppState, uid: i64) -> Result<Vec<PersonRow>, AppError> {
  let sql = format!(
    "SELECT DISTINCT {PERSON_COLUMNS} FROM af_workspace_member mine \
     JOIN af_workspace_member other ON other.workspace_id = mine.workspace_id \
     JOIN af_user u ON u.uid = other.uid \
     WHERE mine.uid = $1 AND u.uid <> $1 AND COALESCE(u.email, '') NOT LIKE '%.removed-%'"
  );
  let rows: Vec<PersonRow> = sqlx::query_as(&sql).bind(uid).fetch_all(&state.pg_pool).await?;
  Ok(rows)
}

async fn members_of(state: &AppState, chat_id: Uuid) -> Result<Vec<PersonRow>, AppError> {
  let sql = format!(
    "SELECT {PERSON_COLUMNS} FROM carehub_chat_member m JOIN af_user u ON u.uid = m.uid \
     WHERE m.chat_id = $1 ORDER BY m.joined_ms, u.uid"
  );
  let rows: Vec<PersonRow> = sqlx::query_as(&sql).bind(chat_id).fetch_all(&state.pg_pool).await?;
  Ok(rows)
}

async fn chat_json(state: &AppState, chat_id: Uuid, uid: i64) -> Result<Value, AppError> {
  let row: Option<(String, bool, i64, i64)> = sqlx::query_as(
    "SELECT c.name, c.is_group, c.last_ms, m.read_ms FROM carehub_chat c \
     JOIN carehub_chat_member m ON m.chat_id = c.id AND m.uid = $2 WHERE c.id = $1",
  )
  .bind(chat_id)
  .bind(uid)
  .fetch_optional(&state.pg_pool)
  .await?;
  let (name, is_group, last_ms, read_ms) =
    row.ok_or_else(|| AppError::RecordNotFound("Chat not found".to_string()))?;
  let members = members_of(state, chat_id).await?;
  let last: Option<(i64, String, i64, bool)> = sqlx::query_as(
    "SELECT sender, body, created_ms, deleted FROM carehub_chat_message \
     WHERE chat_id = $1 ORDER BY created_ms DESC LIMIT 1",
  )
  .bind(chat_id)
  .fetch_optional(&state.pg_pool)
  .await?;
  let unread: i64 = sqlx::query_scalar(
    "SELECT COUNT(*) FROM carehub_chat_message \
     WHERE chat_id = $1 AND created_ms > $2 AND sender <> $3 AND NOT deleted",
  )
  .bind(chat_id)
  .bind(read_ms)
  .bind(uid)
  .fetch_one(&state.pg_pool)
  .await?;
  let last_json = match last {
    Some((sender, body, created, deleted)) => {
      let who = members.iter().find(|p| p.0 == sender);
      json!({
        "body": if deleted { String::new() } else { clip(&body, 200) },
        "deleted": deleted,
        "mine": sender == uid,
        "sender_name": who.map(|p| if p.2.is_empty() { p.3.clone() } else { p.2.clone() }).unwrap_or_default(),
        "created_at": created,
      })
    },
    None => Value::Null,
  };
  Ok(json!({
    "id": chat_id,
    "name": name,
    "is_group": is_group,
    "updated_at": last_ms,
    "unread": unread,
    "members": members.iter().filter(|p| p.0 != uid).map(person_json).collect::<Vec<_>>(),
    "last": last_json,
  }))
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

/// GET /api/carehub/messages/people
async fn people_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let people = people_for(&state, uid).await?;
  Ok(Json(AppResponse::Ok().with_data(json!({
    "people": people.iter().map(person_json).collect::<Vec<_>>(),
  }))))
}

/// GET /api/carehub/messages/unread -> { unread, latest }
async fn unread_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let (unread, latest): (i64, i64) = sqlx::query_as(
    "SELECT COUNT(*), COALESCE(MAX(msg.created_ms), 0)::BIGINT FROM carehub_chat_message msg \
     JOIN carehub_chat_member m ON m.chat_id = msg.chat_id AND m.uid = $1 \
     WHERE msg.created_ms > m.read_ms AND msg.sender <> $1 AND NOT msg.deleted",
  )
  .bind(uid)
  .fetch_one(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "unread": unread, "latest": latest }))))
}

/// GET /api/carehub/messages/chats
async fn list_chats_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let ids: Vec<Uuid> = sqlx::query_scalar(
    "SELECT c.id FROM carehub_chat c JOIN carehub_chat_member m ON m.chat_id = c.id \
     WHERE m.uid = $1 ORDER BY c.last_ms DESC LIMIT 200",
  )
  .bind(uid)
  .fetch_all(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  let mut chats = Vec::with_capacity(ids.len());
  for id in ids {
    chats.push(chat_json(&state, id, uid).await?);
  }
  Ok(Json(AppResponse::Ok().with_data(json!({ "chats": chats }))))
}

#[derive(Debug, Deserialize)]
struct CreateChatBody {
  members: Vec<Uuid>,
  #[serde(default)]
  name: String,
}

/// POST /api/carehub/messages/chats { members: [uuid], name? }
/// With one other person and no name, an existing direct chat is reused.
async fn create_chat_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  body: Json<CreateChatBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let allowed = people_for(&state, uid).await?;
  let mut uids: Vec<i64> = Vec::new();
  for member in &body.members {
    match allowed.iter().find(|p| p.1 == *member) {
      Some(p) => {
        if !uids.contains(&p.0) {
          uids.push(p.0)
        }
      },
      None => return Err(bad("You can only message people in your workspace").into()),
    }
  }
  if uids.is_empty() {
    return Err(bad("Choose at least one person").into());
  }
  if uids.len() > MAX_MEMBERS {
    return Err(bad("Too many people in one chat").into());
  }
  let name = clip(body.name.trim(), 200);
  let is_group = uids.len() > 1 || !name.is_empty();

  if !is_group {
    let other = uids[0];
    let existing: Option<Uuid> = sqlx::query_scalar(
      "SELECT c.id FROM carehub_chat c \
       JOIN carehub_chat_member a ON a.chat_id = c.id AND a.uid = $1 \
       JOIN carehub_chat_member b ON b.chat_id = c.id AND b.uid = $2 \
       WHERE NOT c.is_group LIMIT 1",
    )
    .bind(uid)
    .bind(other)
    .fetch_optional(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
    if let Some(id) = existing {
      return Ok(Json(AppResponse::Ok().with_data(chat_json(&state, id, uid).await?)));
    }
  }

  let id = Uuid::new_v4();
  let now = now_ms();
  sqlx::query(
    "INSERT INTO carehub_chat (id, name, is_group, created_by, created_ms, last_ms) \
     VALUES ($1, $2, $3, $4, $5, $5)",
  )
  .bind(id)
  .bind(&name)
  .bind(is_group)
  .bind(uid)
  .bind(now)
  .execute(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  uids.push(uid);
  for member in &uids {
    sqlx::query(
      "INSERT INTO carehub_chat_member (chat_id, uid, read_ms, joined_ms) VALUES ($1, $2, $3, $3) \
       ON CONFLICT DO NOTHING",
    )
    .bind(id)
    .bind(*member)
    .bind(now)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  }
  Ok(Json(AppResponse::Ok().with_data(chat_json(&state, id, uid).await?)))
}

#[derive(Debug, Deserialize)]
struct UpdateChatBody {
  name: Option<String>,
  #[serde(default)]
  add: Vec<Uuid>,
  #[serde(default)]
  leave: bool,
}

/// PUT /api/carehub/messages/chats/{chat_id} { name?, add?, leave? }
async fn update_chat_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
  body: Json<UpdateChatBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let chat_id = path.into_inner();
  require_member(&state, chat_id, uid).await?;

  if body.leave {
    sqlx::query("DELETE FROM carehub_chat_member WHERE chat_id = $1 AND uid = $2")
      .bind(chat_id)
      .bind(uid)
      .execute(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
    sqlx::query(
      "DELETE FROM carehub_chat c WHERE c.id = $1 \
       AND NOT EXISTS (SELECT 1 FROM carehub_chat_member m WHERE m.chat_id = c.id)",
    )
    .bind(chat_id)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
    return Ok(Json(AppResponse::Ok().with_data(json!({ "left": true }))));
  }

  if let Some(name) = &body.name {
    sqlx::query("UPDATE carehub_chat SET name = $2, is_group = TRUE WHERE id = $1")
      .bind(chat_id)
      .bind(clip(name.trim(), 200))
      .execute(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
  }

  if !body.add.is_empty() {
    let allowed = people_for(&state, uid).await?;
    let now = now_ms();
    for member in &body.add {
      let person = allowed
        .iter()
        .find(|p| p.1 == *member)
        .ok_or_else(|| bad("You can only add people in your workspace"))?;
      sqlx::query(
        "INSERT INTO carehub_chat_member (chat_id, uid, read_ms, joined_ms) VALUES ($1, $2, 0, $3) \
         ON CONFLICT DO NOTHING",
      )
      .bind(chat_id)
      .bind(person.0)
      .bind(now)
      .execute(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
    }
    sqlx::query("UPDATE carehub_chat SET is_group = TRUE WHERE id = $1")
      .bind(chat_id)
      .execute(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
  }

  Ok(Json(AppResponse::Ok().with_data(chat_json(&state, chat_id, uid).await?)))
}

#[derive(Debug, Deserialize)]
struct MessagesQuery {
  after: Option<i64>,
  before: Option<i64>,
}

/// (id, sender, body, created_ms, edited_ms, deleted)
type MessageRow = (Uuid, i64, String, i64, i64, bool);

fn message_json(row: &MessageRow, uid: i64, members: &[PersonRow]) -> Value {
  let sender = members.iter().find(|p| p.0 == row.1);
  json!({
    "id": row.0,
    "mine": row.1 == uid,
    "sender": sender.map(person_json).unwrap_or(Value::Null),
    "body": if row.5 { String::new() } else { row.2.clone() },
    "created_at": row.3,
    "edited_at": row.4,
    "deleted": row.5,
  })
}

/// GET /api/carehub/messages/chats/{chat_id}/messages?after=ms | ?before=ms
/// Newest page by default, oldest first in the answer.
async fn list_messages_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
  query: web::Query<MessagesQuery>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let chat_id = path.into_inner();
  require_member(&state, chat_id, uid).await?;
  let mut rows: Vec<MessageRow> = if let Some(after) = query.after {
    // New and changed messages since `after`.
    sqlx::query_as(
      "SELECT id, sender, body, created_ms, edited_ms, deleted FROM carehub_chat_message \
       WHERE chat_id = $1 AND (created_ms > $2 OR edited_ms > $2) ORDER BY created_ms ASC LIMIT 500",
    )
    .bind(chat_id)
    .bind(after)
    .fetch_all(&state.pg_pool)
    .await
    .map_err(AppError::from)?
  } else {
    let before = query.before.unwrap_or(i64::MAX);
    let mut page: Vec<MessageRow> = sqlx::query_as(
      "SELECT id, sender, body, created_ms, edited_ms, deleted FROM carehub_chat_message \
       WHERE chat_id = $1 AND created_ms < $2 ORDER BY created_ms DESC LIMIT $3",
    )
    .bind(chat_id)
    .bind(before)
    .bind(PAGE)
    .fetch_all(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
    page.reverse();
    page
  };
  rows.truncate(500);
  let members = members_of(&state, chat_id).await?;
  // Senders who have left the chat are still shown by name.
  let mut people = members.clone();
  for row in &rows {
    if !people.iter().any(|p| p.0 == row.1) {
      let sql = format!("SELECT {PERSON_COLUMNS} FROM af_user u WHERE u.uid = $1");
      let found: Option<PersonRow> = sqlx::query_as(&sql)
        .bind(row.1)
        .fetch_optional(&state.pg_pool)
        .await
        .map_err(AppError::from)?;
      if let Some(p) = found {
        people.push(p);
      }
    }
  }
  let read_by: Vec<(i64, i64)> =
    sqlx::query_as("SELECT uid, read_ms FROM carehub_chat_member WHERE chat_id = $1")
      .bind(chat_id)
      .fetch_all(&state.pg_pool)
      .await
      .map_err(AppError::from)?;
  let others_read = read_by
    .iter()
    .filter(|(u, _)| *u != uid)
    .map(|(_, r)| *r)
    .min()
    .unwrap_or(0);
  Ok(Json(AppResponse::Ok().with_data(json!({
    "messages": rows.iter().map(|r| message_json(r, uid, &people)).collect::<Vec<_>>(),
    "others_read_at": others_read,
    "has_more": query.after.is_none() && rows.len() as i64 >= PAGE,
  }))))
}

#[derive(Debug, Deserialize)]
struct SendBody {
  body: String,
}

/// POST /api/carehub/messages/chats/{chat_id}/messages { body }
async fn send_message_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
  body: Json<SendBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let chat_id = path.into_inner();
  require_member(&state, chat_id, uid).await?;
  let text = body.body.trim();
  if text.is_empty() {
    return Err(bad("The message is empty").into());
  }
  if text.chars().count() > MAX_BODY_CHARS {
    return Err(bad("The message is too long").into());
  }
  let id = Uuid::new_v4();
  // Strictly increasing per chat, so polling with `after` never misses one.
  let last: i64 = sqlx::query_scalar("SELECT last_ms FROM carehub_chat WHERE id = $1")
    .bind(chat_id)
    .fetch_one(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  let now = now_ms().max(last + 1);
  sqlx::query(
    "INSERT INTO carehub_chat_message (id, chat_id, sender, body, created_ms) VALUES ($1, $2, $3, $4, $5)",
  )
  .bind(id)
  .bind(chat_id)
  .bind(uid)
  .bind(text)
  .bind(now)
  .execute(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  sqlx::query("UPDATE carehub_chat SET last_ms = GREATEST(last_ms, $2) WHERE id = $1")
    .bind(chat_id)
    .bind(now)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  sqlx::query("UPDATE carehub_chat_member SET read_ms = GREATEST(read_ms, $3) WHERE chat_id = $1 AND uid = $2")
    .bind(chat_id)
    .bind(uid)
    .bind(now)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  let members = members_of(&state, chat_id).await?;
  let row: MessageRow = (id, uid, text.to_string(), now, 0, false);
  Ok(Json(AppResponse::Ok().with_data(message_json(&row, uid, &members))))
}

/// POST /api/carehub/messages/chats/{chat_id}/read
async fn read_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let chat_id = path.into_inner();
  require_member(&state, chat_id, uid).await?;
  let at = now_ms();
  sqlx::query("UPDATE carehub_chat_member SET read_ms = GREATEST(read_ms, $3) WHERE chat_id = $1 AND uid = $2")
    .bind(chat_id)
    .bind(uid)
    .bind(at)
    .execute(&state.pg_pool)
    .await
    .map_err(AppError::from)?;
  Ok(Json(AppResponse::Ok().with_data(json!({ "read_at": at }))))
}

/// PUT /api/carehub/messages/message/{message_id} { body } (own messages only)
async fn edit_message_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
  body: Json<SendBody>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let text = body.body.trim();
  if text.is_empty() || text.chars().count() > MAX_BODY_CHARS {
    return Err(bad("The message is empty or too long").into());
  }
  let done = sqlx::query(
    "UPDATE carehub_chat_message SET body = $3, edited_ms = $4 \
     WHERE id = $1 AND sender = $2 AND NOT deleted",
  )
  .bind(path.into_inner())
  .bind(uid)
  .bind(text)
  .bind(now_ms())
  .execute(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  if done.rows_affected() == 0 {
    return Err(AppError::RecordNotFound("Message not found".to_string()).into());
  }
  Ok(Json(AppResponse::Ok().with_data(json!({ "ok": true }))))
}

/// DELETE /api/carehub/messages/message/{message_id} (own messages only)
async fn delete_message_handler(
  user_uuid: UserUuid,
  state: Data<AppState>,
  path: web::Path<Uuid>,
) -> ActixResult<Json<AppResponse<Value>>> {
  let uid = me(&state, &user_uuid).await?;
  let done = sqlx::query(
    "UPDATE carehub_chat_message SET body = '', deleted = TRUE, edited_ms = $3 \
     WHERE id = $1 AND sender = $2",
  )
  .bind(path.into_inner())
  .bind(uid)
  .bind(now_ms())
  .execute(&state.pg_pool)
  .await
  .map_err(AppError::from)?;
  if done.rows_affected() == 0 {
    return Err(AppError::RecordNotFound("Message not found".to_string()).into());
  }
  Ok(Json(AppResponse::Ok().with_data(json!({ "ok": true }))))
}
