use crate::{Client, QueryExecutor, Row, ToSqlParam, Transaction};

// ---- Helper: String wrapper that accepts any type (including custom enums) ----

/// A String wrapper that implements `FromSql` for any PostgreSQL type.
/// PostgreSQL enum values are transmitted as text on the wire, so reading
/// them as String is safe. Standard `String::from_sql` only accepts TEXT/VARCHAR,
/// rejecting custom enum types.
struct AnyString(String);

impl<'a> tokio_postgres::types::FromSql<'a> for AnyString {
    fn from_sql(
        _ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(AnyString(String::from_utf8(raw.to_vec())?))
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }
}

// ---- Row implementation wrapping tokio_postgres::Row ----

pub struct PgRow(pub tokio_postgres::Row);

impl Row for PgRow {
    fn get_i16(&self, idx: usize) -> i16 { self.0.get(idx) }
    fn get_i32(&self, idx: usize) -> i32 { self.0.get(idx) }
    fn get_i64(&self, idx: usize) -> i64 { self.0.get(idx) }
    fn get_f32(&self, idx: usize) -> f32 { self.0.get(idx) }
    fn get_f64(&self, idx: usize) -> f64 { self.0.get(idx) }
    fn get_string(&self, idx: usize) -> String { self.0.get::<_, AnyString>(idx).0 }
    fn get_bool(&self, idx: usize) -> bool { self.0.get(idx) }
    fn get_uuid(&self, idx: usize) -> uuid::Uuid { self.0.get(idx) }
    fn get_timestamp(&self, idx: usize) -> chrono::NaiveDateTime { self.0.get(idx) }
    fn get_timestamptz(&self, idx: usize) -> chrono::DateTime<chrono::Utc> { self.0.get(idx) }
    fn get_date(&self, idx: usize) -> chrono::NaiveDate { self.0.get(idx) }
    fn get_decimal(&self, idx: usize) -> rust_decimal::Decimal { self.0.get(idx) }
    fn get_json(&self, idx: usize) -> serde_json::Value { self.0.get(idx) }

    fn get_opt_i16(&self, idx: usize) -> Option<i16> { self.0.get(idx) }
    fn get_opt_i32(&self, idx: usize) -> Option<i32> { self.0.get(idx) }
    fn get_opt_i64(&self, idx: usize) -> Option<i64> { self.0.get(idx) }
    fn get_opt_f32(&self, idx: usize) -> Option<f32> { self.0.get(idx) }
    fn get_opt_f64(&self, idx: usize) -> Option<f64> { self.0.get(idx) }
    fn get_opt_string(&self, idx: usize) -> Option<String> {
        self.0.get::<_, Option<AnyString>>(idx).map(|s| s.0)
    }
    fn get_opt_bool(&self, idx: usize) -> Option<bool> { self.0.get(idx) }
    fn get_opt_uuid(&self, idx: usize) -> Option<uuid::Uuid> { self.0.get(idx) }
    fn get_opt_timestamp(&self, idx: usize) -> Option<chrono::NaiveDateTime> { self.0.get(idx) }
    fn get_opt_timestamptz(&self, idx: usize) -> Option<chrono::DateTime<chrono::Utc>> { self.0.get(idx) }
    fn get_opt_date(&self, idx: usize) -> Option<chrono::NaiveDate> { self.0.get(idx) }
    fn get_opt_decimal(&self, idx: usize) -> Option<rust_decimal::Decimal> { self.0.get(idx) }
    fn get_opt_json(&self, idx: usize) -> Option<serde_json::Value> { self.0.get(idx) }

    fn get_vec_i16(&self, idx: usize) -> Vec<i16> { self.0.get(idx) }
    fn get_vec_i32(&self, idx: usize) -> Vec<i32> { self.0.get(idx) }
    fn get_vec_i64(&self, idx: usize) -> Vec<i64> { self.0.get(idx) }
    fn get_vec_f32(&self, idx: usize) -> Vec<f32> { self.0.get(idx) }
    fn get_vec_f64(&self, idx: usize) -> Vec<f64> { self.0.get(idx) }
    fn get_vec_string(&self, idx: usize) -> Vec<String> { self.0.get(idx) }
    fn get_vec_bool(&self, idx: usize) -> Vec<bool> { self.0.get(idx) }
    fn get_vec_uuid(&self, idx: usize) -> Vec<uuid::Uuid> { self.0.get(idx) }
}

// ---- Owned parameter enum for crossing async boundaries ----

/// An owned SQL parameter value. Used internally to convert borrowed `&dyn ToSqlParam`
/// into owned values that can be moved into async futures.
#[derive(Debug)]
pub enum OwnedParam {
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    String(String),
    Uuid(uuid::Uuid),
    NaiveDateTime(chrono::NaiveDateTime),
    DateTimeUtc(chrono::DateTime<chrono::Utc>),
    NaiveDate(chrono::NaiveDate),
    Decimal(rust_decimal::Decimal),
    Json(serde_json::Value),
    OptI16(Option<i16>),
    OptI32(Option<i32>),
    OptI64(Option<i64>),
    OptF32(Option<f32>),
    OptF64(Option<f64>),
    OptBool(Option<bool>),
    OptString(Option<String>),
    OptUuid(Option<uuid::Uuid>),
    OptNaiveDateTime(Option<chrono::NaiveDateTime>),
    OptDateTimeUtc(Option<chrono::DateTime<chrono::Utc>>),
    OptNaiveDate(Option<chrono::NaiveDate>),
    OptDecimal(Option<rust_decimal::Decimal>),
    OptJson(Option<serde_json::Value>),
    VecI16(Vec<i16>),
    VecI32(Vec<i32>),
    VecI64(Vec<i64>),
    VecF32(Vec<f32>),
    VecF64(Vec<f64>),
    VecBool(Vec<bool>),
    VecString(Vec<String>),
    VecUuid(Vec<uuid::Uuid>),
}

impl tokio_postgres::types::ToSql for OwnedParam {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            OwnedParam::I16(v) => v.to_sql(ty, out),
            OwnedParam::I32(v) => v.to_sql(ty, out),
            OwnedParam::I64(v) => v.to_sql(ty, out),
            OwnedParam::F32(v) => v.to_sql(ty, out),
            OwnedParam::F64(v) => v.to_sql(ty, out),
            OwnedParam::Bool(v) => v.to_sql(ty, out),
            OwnedParam::String(v) => v.to_sql(ty, out),
            OwnedParam::Uuid(v) => v.to_sql(ty, out),
            OwnedParam::NaiveDateTime(v) => v.to_sql(ty, out),
            OwnedParam::DateTimeUtc(v) => v.to_sql(ty, out),
            OwnedParam::NaiveDate(v) => v.to_sql(ty, out),
            OwnedParam::Decimal(v) => v.to_sql(ty, out),
            OwnedParam::Json(v) => v.to_sql(ty, out),
            OwnedParam::OptI16(v) => v.to_sql(ty, out),
            OwnedParam::OptI32(v) => v.to_sql(ty, out),
            OwnedParam::OptI64(v) => v.to_sql(ty, out),
            OwnedParam::OptF32(v) => v.to_sql(ty, out),
            OwnedParam::OptF64(v) => v.to_sql(ty, out),
            OwnedParam::OptBool(v) => v.to_sql(ty, out),
            OwnedParam::OptString(v) => v.to_sql(ty, out),
            OwnedParam::OptUuid(v) => v.to_sql(ty, out),
            OwnedParam::OptNaiveDateTime(v) => v.to_sql(ty, out),
            OwnedParam::OptDateTimeUtc(v) => v.to_sql(ty, out),
            OwnedParam::OptNaiveDate(v) => v.to_sql(ty, out),
            OwnedParam::OptDecimal(v) => v.to_sql(ty, out),
            OwnedParam::OptJson(v) => v.to_sql(ty, out),
            OwnedParam::VecI16(v) => v.to_sql(ty, out),
            OwnedParam::VecI32(v) => v.to_sql(ty, out),
            OwnedParam::VecI64(v) => v.to_sql(ty, out),
            OwnedParam::VecF32(v) => v.to_sql(ty, out),
            OwnedParam::VecF64(v) => v.to_sql(ty, out),
            OwnedParam::VecBool(v) => v.to_sql(ty, out),
            OwnedParam::VecString(v) => v.to_sql(ty, out),
            OwnedParam::VecUuid(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        // Accept all types unconditionally. OwnedParam is a dynamic wrapper that
        // delegates serialization to the inner value's to_sql(). PostgreSQL handles
        // type coercion (e.g. text -> enum) server-side when the value is sent as text.
        // Rejecting custom/enum types here would break INSERT/UPDATE for enum columns.
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Convert a `&dyn ToSqlParam` to an owned parameter value by downcasting.
pub fn to_owned_param(param: &dyn ToSqlParam) -> OwnedParam {
    let any = param.as_any();
    // Non-optional types
    if let Some(v) = any.downcast_ref::<i16>() { return OwnedParam::I16(*v); }
    if let Some(v) = any.downcast_ref::<i32>() { return OwnedParam::I32(*v); }
    if let Some(v) = any.downcast_ref::<i64>() { return OwnedParam::I64(*v); }
    if let Some(v) = any.downcast_ref::<f32>() { return OwnedParam::F32(*v); }
    if let Some(v) = any.downcast_ref::<f64>() { return OwnedParam::F64(*v); }
    if let Some(v) = any.downcast_ref::<bool>() { return OwnedParam::Bool(*v); }
    if let Some(v) = any.downcast_ref::<String>() { return OwnedParam::String(v.clone()); }
    if let Some(v) = any.downcast_ref::<uuid::Uuid>() { return OwnedParam::Uuid(*v); }
    if let Some(v) = any.downcast_ref::<chrono::NaiveDateTime>() { return OwnedParam::NaiveDateTime(*v); }
    if let Some(v) = any.downcast_ref::<chrono::DateTime<chrono::Utc>>() { return OwnedParam::DateTimeUtc(*v); }
    if let Some(v) = any.downcast_ref::<chrono::NaiveDate>() { return OwnedParam::NaiveDate(*v); }
    if let Some(v) = any.downcast_ref::<rust_decimal::Decimal>() { return OwnedParam::Decimal(*v); }
    if let Some(v) = any.downcast_ref::<serde_json::Value>() { return OwnedParam::Json(v.clone()); }
    // Optional types
    if let Some(v) = any.downcast_ref::<Option<i16>>() { return OwnedParam::OptI16(*v); }
    if let Some(v) = any.downcast_ref::<Option<i32>>() { return OwnedParam::OptI32(*v); }
    if let Some(v) = any.downcast_ref::<Option<i64>>() { return OwnedParam::OptI64(*v); }
    if let Some(v) = any.downcast_ref::<Option<f32>>() { return OwnedParam::OptF32(*v); }
    if let Some(v) = any.downcast_ref::<Option<f64>>() { return OwnedParam::OptF64(*v); }
    if let Some(v) = any.downcast_ref::<Option<bool>>() { return OwnedParam::OptBool(*v); }
    if let Some(v) = any.downcast_ref::<Option<String>>() { return OwnedParam::OptString(v.clone()); }
    if let Some(v) = any.downcast_ref::<Option<uuid::Uuid>>() { return OwnedParam::OptUuid(*v); }
    if let Some(v) = any.downcast_ref::<Option<chrono::NaiveDateTime>>() { return OwnedParam::OptNaiveDateTime(*v); }
    if let Some(v) = any.downcast_ref::<Option<chrono::DateTime<chrono::Utc>>>() { return OwnedParam::OptDateTimeUtc(*v); }
    if let Some(v) = any.downcast_ref::<Option<chrono::NaiveDate>>() { return OwnedParam::OptNaiveDate(*v); }
    if let Some(v) = any.downcast_ref::<Option<rust_decimal::Decimal>>() { return OwnedParam::OptDecimal(*v); }
    if let Some(v) = any.downcast_ref::<Option<serde_json::Value>>() { return OwnedParam::OptJson(v.clone()); }
    // Vec types
    if let Some(v) = any.downcast_ref::<Vec<i16>>() { return OwnedParam::VecI16(v.clone()); }
    if let Some(v) = any.downcast_ref::<Vec<i32>>() { return OwnedParam::VecI32(v.clone()); }
    if let Some(v) = any.downcast_ref::<Vec<i64>>() { return OwnedParam::VecI64(v.clone()); }
    if let Some(v) = any.downcast_ref::<Vec<f32>>() { return OwnedParam::VecF32(v.clone()); }
    if let Some(v) = any.downcast_ref::<Vec<f64>>() { return OwnedParam::VecF64(v.clone()); }
    if let Some(v) = any.downcast_ref::<Vec<bool>>() { return OwnedParam::VecBool(v.clone()); }
    if let Some(v) = any.downcast_ref::<Vec<String>>() { return OwnedParam::VecString(v.clone()); }
    if let Some(v) = any.downcast_ref::<Vec<uuid::Uuid>>() { return OwnedParam::VecUuid(v.clone()); }
    panic!("Unsupported parameter type for tokio-postgres backend");
}

/// Convert a slice of borrowed params to a vec of owned params.
pub fn convert_params(params: &[&dyn ToSqlParam]) -> Vec<OwnedParam> {
    params.iter().map(|p| to_owned_param(*p)).collect()
}

// ---- Client wrapper ----

/// A wrapper around `tokio_postgres::Client` implementing the NextSQL `Client` trait.
pub struct PgClient {
    inner: tokio_postgres::Client,
}

impl PgClient {
    pub fn new(client: tokio_postgres::Client) -> Self {
        Self { inner: client }
    }

    /// Get a reference to the underlying `tokio_postgres::Client`.
    pub fn inner(&self) -> &tokio_postgres::Client {
        &self.inner
    }

    /// Consume this wrapper and return the underlying `tokio_postgres::Client`.
    pub fn into_inner(self) -> tokio_postgres::Client {
        self.inner
    }
}

impl QueryExecutor for PgClient {
    type Error = tokio_postgres::Error;
    type Row = PgRow;

    fn query(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl std::future::Future<Output = Result<Vec<Self::Row>, Self::Error>> + Send {
        let owned_params = convert_params(params);
        let sql = sql.to_owned();
        let client = &self.inner;
        async move {
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                owned_params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            let rows = client.query(&sql, &param_refs).await?;
            Ok(rows.into_iter().map(PgRow).collect())
        }
    }

    fn execute(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl std::future::Future<Output = Result<u64, Self::Error>> + Send {
        let owned_params = convert_params(params);
        let sql = sql.to_owned();
        let client = &self.inner;
        async move {
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                owned_params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            client.execute(&sql, &param_refs).await
        }
    }
}

impl Client for PgClient {
    type Transaction<'a> = PgTransaction<'a>;

    async fn transaction(&mut self) -> Result<Self::Transaction<'_>, Self::Error> {
        let tx = self.inner.transaction().await?;
        Ok(PgTransaction { inner: tx })
    }
}

// ---- Transaction wrapper ----

/// A wrapper around `tokio_postgres::Transaction` implementing the NextSQL `Transaction` trait.
/// Supports nested transactions (savepoints) and commit/rollback.
/// Drop without calling `commit()` will rollback the transaction.
pub struct PgTransaction<'a> {
    inner: tokio_postgres::Transaction<'a>,
}

impl<'a> PgTransaction<'a> {
    pub fn new(tx: tokio_postgres::Transaction<'a>) -> Self {
        Self { inner: tx }
    }
}

impl QueryExecutor for PgTransaction<'_> {
    type Error = tokio_postgres::Error;
    type Row = PgRow;

    fn query(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl std::future::Future<Output = Result<Vec<Self::Row>, Self::Error>> + Send {
        let owned_params = convert_params(params);
        let sql = sql.to_owned();
        let client = &self.inner;
        async move {
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                owned_params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            let rows = client.query(&sql, &param_refs).await?;
            Ok(rows.into_iter().map(PgRow).collect())
        }
    }

    fn execute(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl std::future::Future<Output = Result<u64, Self::Error>> + Send {
        let owned_params = convert_params(params);
        let sql = sql.to_owned();
        let client = &self.inner;
        async move {
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                owned_params.iter().map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            client.execute(&sql, &param_refs).await
        }
    }
}

impl Transaction for PgTransaction<'_> {
    type Nested<'a> = PgTransaction<'a> where Self: 'a;

    async fn transaction(&mut self) -> Result<Self::Nested<'_>, Self::Error> {
        let tx = self.inner.transaction().await?;
        Ok(PgTransaction { inner: tx })
    }

    async fn commit(self) -> Result<(), Self::Error> { self.inner.commit().await }

    async fn rollback(self) -> Result<(), Self::Error> { self.inner.rollback().await }
}
