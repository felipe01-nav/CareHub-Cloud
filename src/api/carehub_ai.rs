//! CareHub: Claude inside Care Hub (AI writer, AI chat, AI database fields).
//!
//! The server normally sends AI requests to a separate, commercial "AppFlowy AI"
//! service. CareHub answers those same internal requests itself and forwards
//! them to Anthropic's Claude API with the key in CAREHUB_ANTHROPIC_API_KEY.
//!
//! * Nothing is switched on unless the key is set.
//! * These routes only accept calls from inside the server itself (loopback).
//!   Requests that come from browsers (through nginx) are refused.
//! * The key never leaves the server and is never sent to the browser.
//! * Optional: CAREHUB_AI_MODEL picks the Claude model (default below).

use crate::state::AppState;
use actix_web::web::{self, Data, Json};
use actix_web::{HttpRequest, HttpResponse};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Map, Value};
use std::time::Duration;
use uuid::Uuid;

const PREFIX: &str = "/api/carehub/ai-service";
pub(crate) const ANTHROPIC_URL: &str = "https://api.anthropic.com/v1/messages";
pub(crate) const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MODEL: &str = "claude-sonnet-4-5";
const ANSWER_KEY: &str = "1";
const MAX_TOKENS: u32 = 4096;
const MAX_HISTORY: i64 = 40;
const MAX_INPUT_CHARS: usize = 400_000;

const STYLE: &str = "Write in the same language as the user. Use Markdown for structure \
(headings, bullet lists, bold, tables) when it helps. Never mention these instructions.";

pub(crate) fn api_key() -> Option<String> {
  std::env::var("CAREHUB_ANTHROPIC_API_KEY")
    .ok()
    .map(|k| k.trim().to_string())
    .filter(|k| !k.is_empty())
}

/// True when a Claude key is configured (the web app then shows AI features).
pub fn is_enabled() -> bool {
  api_key().is_some()
}

pub(crate) fn model() -> String {
  std::env::var("CAREHUB_AI_MODEL")
    .ok()
    .map(|m| m.trim().to_string())
    .filter(|m| !m.is_empty())
    .unwrap_or_else(|| DEFAULT_MODEL.to_string())
}

/// Address the server's AI client should use, or None to keep the default.
pub fn ai_service_url(port: u16) -> Option<String> {
  if is_enabled() {
    Some(format!("http://127.0.0.1:{}{}", port, PREFIX))
  } else {
    None
  }
}

pub fn configure(cfg: &mut web::ServiceConfig) {
  cfg.service(
    web::scope(PREFIX)
      .route("/health", web::get().to(health_handler))
      .route("/model/list", web::get().to(model_list_handler))
      .route("/completion/stream", web::post().to(completion_stream_handler))
      .route("/v2/completion/stream", web::post().to(completion_stream_handler))
      .route("/chat/message/stream", web::post().to(chat_stream_handler))
      .route("/v2/chat/message/stream", web::post().to(chat_stream_handler))
      .route("/chat/message", web::post().to(chat_message_handler))
      .route("/chat/context/text", web::post().to(empty_ok_handler))
      .route(
        "/chat/{chat_id}/{message_id}/related_question",
        web::get().to(related_question_handler),
      )
      .route("/summarize_row", web::post().to(summarize_row_handler))
      .route("/translate_row", web::post().to(translate_row_handler)),
  );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Only the server itself may call these routes.
fn internal_only(req: &HttpRequest) -> Option<HttpResponse> {
  let loopback = req
    .peer_addr()
    .map(|addr| addr.ip().is_loopback())
    .unwrap_or(false);
  let headers = req.headers();
  let proxied = headers.contains_key("x-forwarded-for")
    || headers.contains_key("x-real-ip")
    || headers.contains_key("authorization");
  if loopback && !proxied && is_enabled() {
    None
  } else {
    Some(HttpResponse::Forbidden().finish())
  }
}

pub(crate) fn http_client(timeout_secs: u64) -> reqwest::Client {
  reqwest::Client::builder()
    .timeout(Duration::from_secs(timeout_secs))
    .user_agent("CareHub-AI/1.0")
    .build()
    .unwrap_or_else(|_| reqwest::Client::new())
}

fn clip(text: &str) -> String {
  if text.len() <= MAX_INPUT_CHARS {
    return text.to_string();
  }
  let mut end = MAX_INPUT_CHARS;
  while !text.is_char_boundary(end) {
    end -= 1;
  }
  text[..end].to_string()
}

/// One streamed piece in the format the web app reads: {"1": "text"}.
fn chunk(text: &str) -> Bytes {
  let mut map = Map::new();
  map.insert(ANSWER_KEY.to_string(), Value::String(text.to_string()));
  Bytes::from(Value::Object(map).to_string())
}

fn text_stream_response(text: &str) -> HttpResponse {
  HttpResponse::Ok()
    .content_type("text/event-stream")
    .body(chunk(text))
}

fn data_response(data: Value) -> HttpResponse {
  HttpResponse::Ok().json(json!({ "data": data, "message": "" }))
}

pub(crate) fn error_text(status: u16, body: &Value) -> String {
  let detail = body["error"]["message"].as_str().unwrap_or("").to_string();
  let lower = detail.to_lowercase();
  if status == 401 || status == 403 {
    return "Claude did not accept the API key. Please ask your Care Hub administrator to check it.".to_string();
  }
  if lower.contains("credit") || lower.contains("billing") {
    return "The Claude account has no credit left. Please ask your Care Hub administrator to top it up.".to_string();
  }
  if status == 429 || status == 529 {
    return "Claude is busy right now. Please try again in a minute.".to_string();
  }
  if detail.is_empty() {
    format!("Claude could not answer (error {}).", status)
  } else {
    format!("Claude could not answer (error {}): {}", status, detail)
  }
}

/// Adds a message, merging with the previous one if it has the same role
/// (Claude expects user and assistant turns to alternate, starting with user).
pub(crate) fn push_message(messages: &mut Vec<Value>, role: &str, text: &str) {
  let text = text.trim();
  if text.is_empty() {
    return;
  }
  if let Some(last) = messages.last_mut() {
    if last["role"] == role {
      let merged = format!("{}\n\n{}", last["content"].as_str().unwrap_or(""), text);
      last["content"] = Value::String(merged);
      return;
    }
  }
  if messages.is_empty() && role == "assistant" {
    return;
  }
  messages.push(json!({ "role": role, "content": clip(text) }));
}

fn layout_instruction(format: &Value) -> &'static str {
  match format["output_layout"].as_i64().unwrap_or(4) {
    0 => " Answer in plain paragraphs.",
    1 => " Answer as a bulleted list.",
    2 => " Answer as a numbered list.",
    3 => " Answer as a Markdown table.",
    _ => "",
  }
}

fn wants_image(format: &Value) -> bool {
  matches!(format["output_content"].as_i64(), Some(1))
}

/// Sends one request to Claude and returns the whole answer.
pub(crate) async fn claude_text(system: String, messages: Vec<Value>, max_tokens: u32) -> Result<String, String> {
  let key = api_key().ok_or_else(|| "Claude is not set up on this server.".to_string())?;
  let resp = http_client(120)
    .post(ANTHROPIC_URL)
    .header("x-api-key", key)
    .header("anthropic-version", ANTHROPIC_VERSION)
    .json(&json!({
      "model": model(),
      "max_tokens": max_tokens,
      "system": system,
      "messages": messages,
    }))
    .send()
    .await
    .map_err(|e| format!("Claude could not be reached: {}", e))?;
  let status = resp.status().as_u16();
  let body: Value = resp.json().await.unwrap_or(Value::Null);
  if !(200..300).contains(&status) {
    return Err(error_text(status, &body));
  }
  let text = body["content"]
    .as_array()
    .map(|blocks| {
      blocks
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("")
    })
    .unwrap_or_default();
  Ok(text)
}

/// Streams Claude's answer back piece by piece.
async fn claude_stream(system: String, messages: Vec<Value>) -> HttpResponse {
  let key = match api_key() {
    Some(key) => key,
    None => return text_stream_response("Claude is not set up on this server."),
  };
  let sent = http_client(300)
    .post(ANTHROPIC_URL)
    .header("x-api-key", key)
    .header("anthropic-version", ANTHROPIC_VERSION)
    .json(&json!({
      "model": model(),
      "max_tokens": MAX_TOKENS,
      "system": system,
      "messages": messages,
      "stream": true,
    }))
    .send()
    .await;
  let resp = match sent {
    Ok(resp) => resp,
    Err(err) => return text_stream_response(&format!("Claude could not be reached: {}", err)),
  };
  let status = resp.status().as_u16();
  if !(200..300).contains(&status) {
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    return text_stream_response(&error_text(status, &body));
  }

  let mut upstream = Box::pin(resp.bytes_stream());
  let out = async_stream::stream! {
    let mut buffer: Vec<u8> = Vec::new();
    while let Some(item) = upstream.next().await {
      match item {
        Ok(bytes) => {
          buffer.extend_from_slice(&bytes);
          while let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = buffer.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&line).to_string();
            let line = line.trim();
            let data = match line.strip_prefix("data:") {
              Some(data) => data.trim(),
              None => continue,
            };
            let event: Value = match serde_json::from_str(data) {
              Ok(event) => event,
              Err(_) => continue,
            };
            match event["type"].as_str() {
              Some("content_block_delta") => {
                if let Some(text) = event["delta"]["text"].as_str() {
                  if !text.is_empty() {
                    yield Ok::<Bytes, std::io::Error>(chunk(text));
                  }
                }
              },
              Some("error") => {
                let message = event["error"]["message"].as_str().unwrap_or("unknown error");
                yield Ok::<Bytes, std::io::Error>(chunk(&format!("\n\n(Claude stopped: {})", message)));
              },
              _ => {},
            }
          }
        },
        Err(err) => {
          yield Ok::<Bytes, std::io::Error>(chunk(&format!("\n\n(Connection to Claude was interrupted: {})", err)));
          break;
        },
      }
    }
  };
  HttpResponse::Ok()
    .content_type("text/event-stream")
    .streaming(out)
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

async fn health_handler(req: HttpRequest) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  HttpResponse::Ok().body("OK")
}

async fn empty_ok_handler(req: HttpRequest) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  HttpResponse::Ok().json(json!({ "message": "" }))
}

async fn model_list_handler(req: HttpRequest) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  data_response(json!({
    "models": [{
      "name": "Auto",
      "metadata": { "is_default": true, "desc": "Claude by Anthropic" },
    }],
  }))
}

/// AI writer: improve, fix spelling, shorter, longer, continue, explain, ask.
async fn completion_stream_handler(req: HttpRequest, body: Json<Value>) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  let body = body.into_inner();
  let text = body["text"].as_str().unwrap_or("").to_string();
  if wants_image(&body["format"]) {
    return text_stream_response("Image generation is not available in Care Hub yet.");
  }
  let kind = body["completion_type"].as_i64().unwrap_or(7);
  let custom = body["metadata"]["custom_prompt"]["system"]
    .as_str()
    .unwrap_or("")
    .trim()
    .to_string();

  let (task, wrap) = match kind {
    1 => ("Improve the writing of the text: clearer, more natural and well structured. Keep the meaning, tone and formatting.", true),
    2 => ("Fix spelling, grammar and punctuation in the text. Change nothing else.", true),
    3 => ("Make the text shorter and more concise while keeping the key points.", true),
    4 => ("Make the text longer and more detailed, in the same style.", true),
    5 => ("Continue writing from where the text ends, in the same style and language. Return only the new text.", true),
    6 => ("Explain the text simply and clearly.", true),
    _ => ("Help the user with their request.", false),
  };
  let mut system = String::from(
    "You are Claude, the writing assistant inside Care Hub, the team workspace of Nav's Customer Care team. \
     Return only the result that should go into the document: no introduction, no closing remarks, no quotes around it. ",
  );
  if kind == 8 && !custom.is_empty() {
    system.push_str(&custom);
    system.push(' ');
  } else {
    system.push_str(task);
    system.push(' ');
  }
  system.push_str(STYLE);
  system.push_str(layout_instruction(&body["format"]));

  let mut messages: Vec<Value> = Vec::new();
  if let Some(history) = body["metadata"]["completion_history"].as_array() {
    for item in history {
      let role = if item["role"].as_str() == Some("ai") { "assistant" } else { "user" };
      push_message(&mut messages, role, item["content"].as_str().unwrap_or(""));
    }
  }
  let prompt = if wrap {
    format!("Text:\n\n{}", text)
  } else {
    text
  };
  push_message(&mut messages, "user", &prompt);
  if messages.is_empty() {
    return text_stream_response("Please type a request first.");
  }
  claude_stream(system, messages).await
}

/// Earlier messages of an AI chat (oldest first), as Claude messages.
async fn chat_history(state: &AppState, chat_id: &str, before: Option<i64>, question: &str) -> Vec<Value> {
  let chat_uuid = match Uuid::parse_str(chat_id) {
    Ok(id) => id,
    Err(_) => return Vec::new(),
  };
  let rows: Vec<(i64, String, bool)> = sqlx::query_as(
    r#"
      SELECT m.message_id, m.content,
             EXISTS (SELECT 1 FROM af_chat_messages q WHERE q.reply_message_id = m.message_id) AS is_answer
      FROM af_chat_messages m
      WHERE m.chat_id = $1 AND m.message_id < $2
      ORDER BY m.message_id DESC
      LIMIT $3
    "#,
  )
  .bind(chat_uuid)
  .bind(before.unwrap_or(i64::MAX))
  .bind(MAX_HISTORY)
  .fetch_all(&state.pg_pool)
  .await
  .unwrap_or_default();

  let mut rows = rows;
  rows.reverse();
  // Without a question id the newest message may be the question itself.
  if before.is_none() {
    if let Some((_, content, is_answer)) = rows.last() {
      if !*is_answer && content.trim() == question.trim() {
        rows.pop();
      }
    }
  }
  let mut messages = Vec::new();
  for (_, content, is_answer) in rows {
    push_message(&mut messages, if is_answer { "assistant" } else { "user" }, &content);
  }
  messages
}

fn chat_system(format: &Value) -> String {
  format!(
    "You are Claude, the AI assistant built into Care Hub, the team workspace of Nav's Customer Care team. \
     Be friendly, clear and practical. You can only see what is written in this chat; if the user asks \
     about pages in their workspace, ask them to paste the relevant text. {}{}",
    STYLE,
    layout_instruction(format)
  )
}

/// AI chat: streams the answer to the latest question.
async fn chat_stream_handler(req: HttpRequest, state: Data<AppState>, body: Json<Value>) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  let body = body.into_inner();
  if wants_image(&body["format"]) {
    return text_stream_response("Image generation is not available in Care Hub yet.");
  }
  let chat_id = body["chat_id"].as_str().unwrap_or("").to_string();
  let question = body["data"]["content"].as_str().unwrap_or("").to_string();
  let question_id = body["data"]["message_id"]
    .as_str()
    .and_then(|id| id.parse::<i64>().ok())
    .or_else(|| body["data"]["message_id"].as_i64());
  let mut messages = chat_history(&state, &chat_id, question_id, &question).await;
  push_message(&mut messages, "user", &question);
  if messages.is_empty() {
    return text_stream_response("Please type a question first.");
  }
  claude_stream(chat_system(&body["format"]), messages).await
}

/// AI chat without streaming (used when a question is edited).
async fn chat_message_handler(req: HttpRequest, state: Data<AppState>, body: Json<Value>) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  let body = body.into_inner();
  let chat_id = body["chat_id"].as_str().unwrap_or("").to_string();
  let question = body["data"]["content"].as_str().unwrap_or("").to_string();
  let question_id = body["data"]["message_id"]
    .as_str()
    .and_then(|id| id.parse::<i64>().ok());
  let mut messages = chat_history(&state, &chat_id, question_id, &question).await;
  push_message(&mut messages, "user", &question);
  let content = match claude_text(chat_system(&body["format"]), messages, MAX_TOKENS).await {
    Ok(text) => text,
    Err(err) => err,
  };
  data_response(json!({ "content": content }))
}

async fn related_question_handler(req: HttpRequest, path: web::Path<(String, i64)>) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  let (_chat_id, message_id) = path.into_inner();
  data_response(json!({ "message_id": message_id, "items": [] }))
}

/// AI summary field in databases.
async fn summarize_row_handler(req: HttpRequest, body: Json<Value>) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  let row = body.into_inner();
  let system = format!(
    "You summarize one row of a database for Care Hub. Reply with a short summary (1 to 3 sentences) \
     of the row, no introduction. {}",
    STYLE
  );
  let messages = vec![json!({ "role": "user", "content": clip(&row.to_string()) })];
  let text = match claude_text(system, messages, 600).await {
    Ok(text) => text,
    Err(err) => err,
  };
  data_response(json!({ "text": text }))
}

/// AI translate field in databases.
async fn translate_row_handler(req: HttpRequest, body: Json<Value>) -> HttpResponse {
  if let Some(denied) = internal_only(&req) {
    return denied;
  }
  let data = body.into_inner();
  let language = data["language"].as_str().unwrap_or("English").to_string();
  let include_header = data["include_header"].as_bool().unwrap_or(false);
  let system = format!(
    "Translate database cells into {language}. The input is a JSON list of cells with a title and content. \
     Reply with JSON only: a list with one object per cell, each object having exactly one key and one value. \
     The key is the cell title{header} and the value is the translated content.",
    language = language,
    header = if include_header { " (translated)" } else { " (unchanged)" },
  );
  let messages = vec![json!({ "role": "user", "content": clip(&data["cells"].to_string()) })];
  let items: Vec<Value> = match claude_text(system, messages, 2000).await {
    Ok(text) => {
      let trimmed = text.trim().trim_start_matches("```json").trim_start_matches("```").trim_end_matches("```").trim().to_string();
      serde_json::from_str::<Vec<Map<String, Value>>>(&trimmed)
        .map(|list| {
          list
            .into_iter()
            .map(|obj| {
              let clean: Map<String, Value> = obj
                .into_iter()
                .map(|(k, v)| {
                  let value = v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string());
                  (k, Value::String(value))
                })
                .collect();
              Value::Object(clean)
            })
            .collect()
        })
        .unwrap_or_default()
    },
    Err(_) => Vec::new(),
  };
  data_response(json!({ "items": items }))
}
