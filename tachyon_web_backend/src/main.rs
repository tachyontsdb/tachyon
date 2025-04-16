use axum::{
    extract::{
        ws::{Message, Utf8Bytes, WebSocket},
        Query, WebSocketUpgrade,
    },
    http::StatusCode,
    response::Response,
    routing::{any, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tachyon_core::{
    error::TachyonErr, Connection, QueryUtils, QueryUtilsResultData, Timestamp, ValueType,
};
use tokio::{net::TcpListener, time};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

impl From<TachyonErr> for ErrorResponse {
    fn from(value: TachyonErr) -> Self {
        Self {
            error: value.to_string(),
        }
    }
}

#[derive(Deserialize)]
struct GetStreamsRequest {
    path: String,
}

#[derive(Serialize)]
struct GetStreamsResponseMatcher {
    label: String,
    value: String,
}

#[derive(Serialize)]
struct GetStreamsResponseStream {
    name: String,
    value_type: String,
    matchers: Vec<GetStreamsResponseMatcher>,
}

#[derive(Serialize)]
struct GetStreamsResponse {
    streams: Vec<GetStreamsResponseStream>,
}

async fn get_streams(
    Json(request): Json<GetStreamsRequest>,
) -> Result<Json<GetStreamsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let connection =
        Connection::new(request.path).map_err(|err| (StatusCode::BAD_REQUEST, Json(err.into())))?;
    let raw_streams = connection
        .get_all_streams()
        .map_err(|err| (StatusCode::BAD_REQUEST, Json(err.into())))?;

    let mut streams = Vec::new();
    for (_, pairs, value_type) in raw_streams {
        let mut name: Option<String> = None;
        let mut matchers = Vec::new();

        for (label, value) in pairs {
            if label == "__name" {
                name = Some(value);
            } else {
                matchers.push(GetStreamsResponseMatcher { label, value });
            }
        }

        streams.push(GetStreamsResponseStream {
            name: name.unwrap(),
            value_type: value_type.to_string(),
            matchers,
        });
    }

    Ok(Json(GetStreamsResponse { streams }))
}

#[derive(Serialize)]
struct QueryResponse {
    value_type: String,
    timestamps: Option<Vec<Timestamp>>,
    values_u64: Option<Vec<u64>>,
    values_i64: Option<Vec<i64>>,
    values_f64: Option<Vec<f64>>,
}

fn query(
    connection: &mut Connection,
    query: impl AsRef<str>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> Result<QueryResponse, TachyonErr> {
    let (_, value_type, timestamps, data) = QueryUtils::perform(connection, query, start, end)?;

    let mut values_u64 = None;
    let mut values_i64 = None;
    let mut values_f64 = None;

    if value_type == ValueType::UInteger64 {
        let QueryUtilsResultData::UInteger64(values) = data else {
            unreachable!();
        };
        values_u64 = Some(values);
    } else if value_type == ValueType::Integer64 {
        let QueryUtilsResultData::Integer64(values) = data else {
            unreachable!();
        };
        values_i64 = Some(values);
    } else if value_type == ValueType::Float64 {
        let QueryUtilsResultData::Float64(values) = data else {
            unreachable!();
        };
        values_f64 = Some(values);
    }

    Ok(QueryResponse {
        value_type: value_type.to_string(),
        timestamps,
        values_u64,
        values_i64,
        values_f64,
    })
}

#[derive(Deserialize)]
struct PerformQueryRequest {
    path: String,
    query: String,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}

async fn perform_query(
    Json(request): Json<PerformQueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mut connection =
        Connection::new(request.path).map_err(|err| (StatusCode::BAD_REQUEST, Json(err.into())))?;
    let response = query(&mut connection, request.query, request.start, request.end)
        .map_err(|err| (StatusCode::BAD_REQUEST, Json(err.into())))?;

    Ok(Json(response))
}

#[derive(Deserialize)]
struct BeginSocketRequest {
    path: String,
    queries: String,
    interval_milliseconds: u64,
    time_begin: Option<Timestamp>,
    time_end: Option<Timestamp>,
    time_diff: Option<u64>,
}

async fn websocket_perform_query(
    ws: WebSocketUpgrade,
    Query(request): Query<BeginSocketRequest>,
) -> Response {
    ws.on_upgrade(move |socket| handle_perform_query_socket(socket, request))
}

async fn handle_perform_query_socket(mut socket: WebSocket, request: BeginSocketRequest) {
    let mut interval = time::interval(Duration::from_millis(request.interval_milliseconds));
    let mut i = 0u64;

    let queries = request.queries.split('.').collect::<Vec<_>>();

    loop {
        interval.tick().await;

        let start_time = if let Some(time_begin) = request.time_begin {
            Some(time_begin + request.interval_milliseconds * i)
        } else {
            None
        };

        let end_time = if let Some(time_diff) = request.time_diff {
            if let Some(start_time) = start_time {
                let end_time = start_time + time_diff;

                if request.time_end.is_some_and(|time_end| end_time > time_end) {
                    // Reached the end time
                    println!("Reached the end time!");
                    break;
                }

                Some(end_time)
            } else {
                None
            }
        } else {
            None
        };

        let responses = {
            let mut connection = Connection::new(&request.path).unwrap();
            queries
                .iter()
                .map(|query_str| query(&mut connection, query_str, start_time, end_time).unwrap())
                .collect::<Vec<_>>()
        };
        let responses_str = serde_json::to_string(&responses).unwrap();

        println!("Sending response now");

        let message = Message::text(Utf8Bytes::from(&responses_str));
        if socket.send(message).await.is_err() {
            // Client disconnected
            println!("Client disconnected!");
            return;
        }

        i += 1;
    }
}

#[tokio::main]
pub async fn main() {
    let app = Router::new()
        .route("/health", get(|| async {}))
        .route("/get_streams", post(get_streams))
        .route("/query", post(perform_query))
        .route("/ws/query", any(websocket_perform_query))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
