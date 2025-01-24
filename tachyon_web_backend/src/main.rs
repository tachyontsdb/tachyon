use axum::{
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tachyon_core::{Connection, Timestamp, ValueType, Vector};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

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

async fn perform_query(Json(request): Json<PerformQueryRequest>) -> Json<PerformQueryResponse> {
    let mut connection = Connection::new(request.path);
    let mut query = connection.prepare_query(request.query, request.start, request.end);

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

    Json(PerformQueryResponse {
        value_type: match value_type {
            ValueType::Integer64 => String::from("Integer64"),
            ValueType::UInteger64 => String::from("UInteger64"),
            ValueType::Float64 => String::from("Float64"),
        },
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
    })
}

#[tokio::main]
pub async fn main() {
    let app = Router::new()
        .route("/health", get(|| async {}))
        .route("/query", post(perform_query))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
