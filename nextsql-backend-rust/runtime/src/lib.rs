use std::future::Future;

/// Represents a field in a partial update operation.
/// - `Unchanged`: field is not included in the SET clause
/// - `Set(T)`: field is set to the given value (use `Set(None)` for nullable columns to set NULL)
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateField<T> {
    Unchanged,
    Set(T),
}

impl<T> UpdateField<T> {
    pub fn is_set(&self) -> bool {
        matches!(self, UpdateField::Set(_))
    }

    pub fn is_unchanged(&self) -> bool {
        matches!(self, UpdateField::Unchanged)
    }
}

impl<T> Default for UpdateField<T> {
    fn default() -> Self {
        UpdateField::Unchanged
    }
}

/// Trait for values that can be bound as SQL query parameters.
pub trait ToSqlParam: Send + Sync {
    /// Convert this value into a format the database driver can accept.
    /// The returned Any should be downcastable to the backend's native param type.
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync);
}

/// Trait for reading typed values from a database result row.
pub trait Row: Send {
    fn get_i16(&self, idx: usize) -> i16;
    fn get_i32(&self, idx: usize) -> i32;
    fn get_i64(&self, idx: usize) -> i64;
    fn get_f32(&self, idx: usize) -> f32;
    fn get_f64(&self, idx: usize) -> f64;
    fn get_string(&self, idx: usize) -> String;
    fn get_bool(&self, idx: usize) -> bool;
    fn get_uuid(&self, idx: usize) -> uuid::Uuid;
    fn get_timestamp(&self, idx: usize) -> chrono::NaiveDateTime;
    fn get_timestamptz(&self, idx: usize) -> chrono::DateTime<chrono::Utc>;
    fn get_date(&self, idx: usize) -> chrono::NaiveDate;
    fn get_decimal(&self, idx: usize) -> rust_decimal::Decimal;
    fn get_json(&self, idx: usize) -> serde_json::Value;

    // Optional variants for nullable columns
    fn get_opt_i16(&self, idx: usize) -> Option<i16>;
    fn get_opt_i32(&self, idx: usize) -> Option<i32>;
    fn get_opt_i64(&self, idx: usize) -> Option<i64>;
    fn get_opt_f32(&self, idx: usize) -> Option<f32>;
    fn get_opt_f64(&self, idx: usize) -> Option<f64>;
    fn get_opt_string(&self, idx: usize) -> Option<String>;
    fn get_opt_bool(&self, idx: usize) -> Option<bool>;
    fn get_opt_uuid(&self, idx: usize) -> Option<uuid::Uuid>;
    fn get_opt_timestamp(&self, idx: usize) -> Option<chrono::NaiveDateTime>;
    fn get_opt_timestamptz(&self, idx: usize) -> Option<chrono::DateTime<chrono::Utc>>;
    fn get_opt_date(&self, idx: usize) -> Option<chrono::NaiveDate>;
    fn get_opt_decimal(&self, idx: usize) -> Option<rust_decimal::Decimal>;
    fn get_opt_json(&self, idx: usize) -> Option<serde_json::Value>;

    // Array variants
    fn get_vec_i16(&self, idx: usize) -> Vec<i16>;
    fn get_vec_i32(&self, idx: usize) -> Vec<i32>;
    fn get_vec_i64(&self, idx: usize) -> Vec<i64>;
    fn get_vec_f32(&self, idx: usize) -> Vec<f32>;
    fn get_vec_f64(&self, idx: usize) -> Vec<f64>;
    fn get_vec_string(&self, idx: usize) -> Vec<String>;
    fn get_vec_bool(&self, idx: usize) -> Vec<bool>;
    fn get_vec_uuid(&self, idx: usize) -> Vec<uuid::Uuid>;
}

/// Trait for executing SQL queries against a database.
/// Uses `impl Future` instead of async_trait to avoid proc macro compile overhead.
///
/// This trait is backend-agnostic: implementations exist for PostgreSQL (tokio-postgres),
/// and can be added for MySQL, SQLite, etc.
pub trait Client: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Row: Row;
    type Transaction<'a>: Transaction<Error = Self::Error, Row = Self::Row> + 'a where Self: 'a;

    /// Execute a query that returns rows.
    fn query(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl Future<Output = Result<Vec<Self::Row>, Self::Error>> + Send;

    /// Execute a statement that returns the number of affected rows.
    fn execute(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Begin a new transaction.
    fn transaction(&mut self) -> impl Future<Output = Result<Self::Transaction<'_>, Self::Error>> + Send;
}

/// Trait for a database transaction.
/// Supports the same query/execute operations as `Client`, plus commit/rollback.
/// Dropping without calling `commit()` should rollback the transaction.
pub trait Transaction: Send {
    type Error: std::error::Error + Send + Sync + 'static;
    type Row: Row;
    type Nested<'a>: Transaction<Error = Self::Error, Row = Self::Row> + 'a where Self: 'a;

    /// Execute a query that returns rows.
    fn query(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl Future<Output = Result<Vec<Self::Row>, Self::Error>> + Send;

    /// Execute a statement that returns the number of affected rows.
    fn execute(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Begin a nested transaction (savepoint).
    fn transaction(&mut self) -> impl Future<Output = Result<Self::Nested<'_>, Self::Error>> + Send;

    /// Commit this transaction.
    fn commit(self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Rollback this transaction.
    fn rollback(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

// ---- ToSqlParam implementations for primitive types ----

macro_rules! impl_to_sql_param {
    ($($ty:ty),*) => {
        $(
            impl ToSqlParam for $ty {
                fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
                    self
                }
            }
        )*
    };
}

impl_to_sql_param!(
    i16, i32, i64, f32, f64, bool, String,
    uuid::Uuid, chrono::NaiveDateTime, chrono::NaiveDate,
    rust_decimal::Decimal
);

impl ToSqlParam for chrono::DateTime<chrono::Utc> {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

impl ToSqlParam for serde_json::Value {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

impl ToSqlParam for &'static str {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

impl<T: ToSqlParam + 'static> ToSqlParam for Option<T> {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

impl<T: ToSqlParam + 'static> ToSqlParam for Vec<T> {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

impl<T: ToSqlParam + 'static> ToSqlParam for UpdateField<T> {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        match self {
            UpdateField::Set(v) => v.as_any(),
            UpdateField::Unchanged => panic!("Unchanged fields should not be passed as SQL params"),
        }
    }
}

#[cfg(feature = "backend-tokio-postgres")]
pub mod tokio_postgres_impl;
