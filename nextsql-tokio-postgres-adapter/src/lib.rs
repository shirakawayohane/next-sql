//! tokio-postgres / deadpool-postgres adapter for NextSQL runtime.
//!
//! Provides [`PooledPgClient`] which wraps a `deadpool_postgres::Object`
//! and implements the NextSQL [`Client`] trait.
//!
//! Also re-exports the core tokio-postgres types from the runtime crate
//! for convenience.

pub use nextsql_backend_rust_runtime::tokio_postgres_impl::{
    convert_params, to_owned_param, OwnedParam, PgClient, PgRow, PgTransaction,
};
pub use nextsql_backend_rust_runtime::{Client, Row, ToSqlParam, Transaction};

/// Convert a slice of [`OwnedParam`] to a vec of trait-object references
/// suitable for passing to `tokio_postgres` query methods.
pub fn owned_params_to_refs(owned: &[OwnedParam]) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
    owned
        .iter()
        .map(|p| p as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect()
}

/// A wrapper around `deadpool_postgres::Object` implementing the NextSQL [`Client`] trait.
///
/// This allows using a pooled connection from deadpool-postgres directly
/// with NextSQL generated query/mutation functions.
pub struct PooledPgClient {
    conn: deadpool_postgres::Object,
}

impl PooledPgClient {
    pub fn new(conn: deadpool_postgres::Object) -> Self {
        Self { conn }
    }

    /// Get a reference to the underlying `tokio_postgres::Client`.
    pub fn inner(&self) -> &tokio_postgres::Client {
        use std::ops::Deref;
        self.conn.deref()
    }

    fn inner_mut(&mut self) -> &mut tokio_postgres::Client {
        use std::ops::DerefMut;
        self.conn.deref_mut()
    }
}

impl Client for PooledPgClient {
    type Error = tokio_postgres::Error;
    type Row = PgRow;
    type Transaction<'a> = PgTransaction<'a>;

    fn query(
        &self,
        sql: &str,
        params: &[&dyn ToSqlParam],
    ) -> impl std::future::Future<Output = Result<Vec<Self::Row>, Self::Error>> + Send {
        let owned_params = convert_params(params);
        let sql = sql.to_owned();
        let client = self.inner();
        async move {
            let param_refs = owned_params_to_refs(&owned_params);
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
        let client = self.inner();
        async move {
            let param_refs = owned_params_to_refs(&owned_params);
            client.execute(&sql, &param_refs).await
        }
    }

    fn transaction(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Self::Transaction<'_>, Self::Error>> + Send {
        async move {
            let tx = self.inner_mut().transaction().await?;
            Ok(PgTransaction::new(tx))
        }
    }
}
