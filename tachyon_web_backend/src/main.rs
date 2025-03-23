use axum::{
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tachyon_core::{error::TachyonErr, Connection, Timestamp, ValueType, Vector};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

fn get_value_type_str(value_type: ValueType) -> String {
    match value_type {
        ValueType::Integer64 => String::from("Integer64"),
        ValueType::UInteger64 => String::from("UInteger64"),
        ValueType::Float64 => String::from("Float64"),
    }
}

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
            value_type: get_value_type_str(value_type),
            matchers,
        });
    }

    Ok(Json(GetStreamsResponse { streams }))
}

#[derive(Deserialize)]
struct PerformQueryRequest {
    path: String,
    query: String,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
}

#[derive(Serialize)]
struct PerformQueryResponse {
    value_type: String,
    timestamps: Vec<Timestamp>,
    values_u64: Option<Vec<u64>>,
    values_i64: Option<Vec<i64>>,
    values_f64: Option<Vec<f64>>,
}

async fn perform_query(
    Json(request): Json<PerformQueryRequest>,
) -> Result<Json<PerformQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let mut connection =
        Connection::new(request.path).map_err(|err| (StatusCode::BAD_REQUEST, Json(err.into())))?;
    let mut query = connection
        .prepare_query(request.query, request.start, request.end)
        .map_err(|err| (StatusCode::BAD_REQUEST, Json(err.into())))?;

    let value_type = query.value_type();

    let mut timestamps = Vec::new();

    let mut values_u64 = Vec::<u64>::new();
    let mut values_i64 = Vec::<i64>::new();
    let mut values_f64 = Vec::<f64>::new();

    while let Some(Vector { timestamp, value }) = query.next_vector() {
        timestamps.push(timestamp);
        match value_type {
            ValueType::UInteger64 => values_u64.push(value.get_uinteger64()),
            ValueType::Integer64 => values_i64.push(value.get_integer64()),
            ValueType::Float64 => values_f64.push(value.get_float64()),
        }
    }

    Ok(Json(PerformQueryResponse {
        value_type: get_value_type_str(value_type),
        timestamps,
        values_u64: if value_type == ValueType::UInteger64 {
            Some(values_u64)
        } else {
            None
        },
        values_i64: if value_type == ValueType::Integer64 {
            Some(values_i64)
        } else {
            None
        },
        values_f64: if value_type == ValueType::Float64 {
            Some(values_f64)
        } else {
            None
        },
    }))
}

#[tokio::main]
pub async fn main() {
    let app = Router::new()
        .route("/health", get(|| async {}))
        .route("/get_streams", post(get_streams))
        .route("/query", post(perform_query))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
