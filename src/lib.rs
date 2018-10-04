//! Crate for identifying and expiring long-running database activities.
//! The core primitive provided by the crate is a `PleaseHandle`. These
//! handles represent long-running operations, and can be used as the
//! basis for implementing exclusive locking behaviour when a lock may
//! be held for too long for transaction-level locking to be acceptable.
//! 
//! # Setup
//! 
//! This crate requires that certain tables exist, and for this reason
//! it exports its own migrations. To manage migrations of third-party
//! crates you can install the `diesel-setup-deps` tool:
//! 
//! ```ignore
//! cargo install diesel-setup-deps
//! diesel setup
//! diesel-setup-deps
//! ```
//! 
//! The necessary migrations will be automatically added to your diesel
//! migrations directory, and these should be committed to version
//! control along with your other migrations.
//! 
//! # Usage
//! 
//! To begin with, you will typically have a long-running operation
//! which you want to have exclusive access to some part of your database.
//! 
//! A good example might be generating a report: this operation will process
//! large amounts of data, and then save the result to the `output` field
//! in our `reports` table. We want the operation to have exclusive access
//! to the `output` field, so that nobody else tries to generate the report
//! whilst we are running, and we also may want to track whether a report
//! is in progress or not.
//! 
//! This operation can be implemented as a single function:
//! 
//! ```ignore
//! fn generate_report(
//!     connection_pool: Arc<ConnectionPool>,
//!     report_id: i32
//! ) -> PleaseResult<ConnectionPoolError> {
//! ```
//! 
//! First we obtain a handle to represent our operation:
//! 
//! ```ignore
//!     let mut handle = PleaseHandle::new_with_cleanup(
//!         connection_pool, "generating report"
//!     )?;
//! ```
//! 
//! Next we store our handle's ID in the reports table:
//! 
//! ```ignore
//!     handle.transaction(|conn, handle_id| {
//!         diesel::update(
//!             reports::table
//!                 .filter(reports::id.eq(report_id))
//!                 .filter(reports::operation_id.is_null())
//!         )
//!         .set(reports::operation_id.eq(Some(handle_id)))
//!         .execute(conn)
//!     })?;
//! ```
//! 
//! Importantly, we fail if the operation ID is already set.
//! 
//! Now, we can perform whatever work is required to generate the report.
//! If the report may take longer than the operation timout, then you can
//! either increase the timeout, or call `handle.refresh()` every so often
//! to ensure the timeout is never reached.
//! 
//! When we have our result, we simply save it back, and close the handle:
//! 
//! ```ignore
//!     handle.transaction(|conn, handle_id| {
//!         diesel::update(
//!             reports::table
//!                 .filter(reports::id.eq(report_id))
//!         )
//!         .set(reports::output.eq(Some(result)))
//!         .execute(conn)
//!     })?;
//! 
//!     handle.close()?;
//!     Ok(())
//! })
//! ```
//! 
//! The handle will be automatically closed if it is instead allowed to
//! fall out of scope, but errors will be ignored.
//! 
//! # Database Schema
//! 
//! In the above example, it is expected that the `reports::operation_id` column
//! is a nullable integer column, and a foreign key into the `please_ids`
//! table.
//! 
//! Ideally you should set up cascade rules such that when rows are deleted
//! from the `please_ids` table, corresponding reports have their `operation_id`
//! column set to null.
//! 
//! # Operation Timeouts
//! 
//! If no activity happens on a `PleaseHandle` for longer than the *operation timeout*
//! then the handle may expire. This will happen automatically whenever
//! `perform_cleanup` or `new_with_cleanup` is called from another thread or another
//! database client.
//! 
//! Calling the methods `transaction` or `refresh` are considered activity, and will
//! prevent the handle from expiring, assuming it has not already expired. Both methods
//! will fail-fast if the operation has already expired. In this case you should cancel
//! any work you were doing as part of the operation.
//! 
//! The operation timeout is controlled by a database function: `please_timeout()`.
//! To change the timeout, use a migration to alter this function and return a
//! different value. It is not currently possible to change the timeout on a per-operation
//! basis.
//! 
//! It is recommended to set the operation timeout to as short a time as possible, so
//! that if your application crashes, is terminated unexpectedly, or simply loses
//! connectivity to the database, any locks it might have held are released as
//! soon as possible.
//! 
//! The operation timeout is by default set to *two minutes*.
#![allow(proc_macro_derive_resolution_fallback)]
#![deny(missing_docs)]

use std::marker::PhantomData;
use std::error::Error;
use std::fmt::{Formatter, Display, self};

#[macro_use]
extern crate diesel;
extern crate chrono;

use chrono::NaiveDateTime;

use diesel::Connection;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::dsl;

mod schema;

#[derive(Queryable, Debug, Clone)]
struct PleaseId {
    id: i32,
    creation: NaiveDateTime,
    expiry: NaiveDateTime,
    title: String,
    refresh_count: i32,
}

/// Expired ID, only used for logging/debug purposes
#[derive(Debug, Clone)]
pub struct ExpiredId(PleaseId);


/// Error type returned by this library
#[derive(Debug, PartialEq)]
pub enum PleaseError<P> {
    /// An error occurred whilst obtaining a connection from the
    /// connection provider.
    Provider(P),
    /// An error occurred whilst querying the database.
    Query(diesel::result::Error),
    /// Tried to use a handle which had already expired.
    Expired,

    #[doc(hidden)]
    __Nonexhaustive,
}

impl<P: Error> Display for PleaseError<P> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            &PleaseError::Provider(ref e) => Display::fmt(e, f),
            &PleaseError::Query(ref e) => Display::fmt(e, f),
            &PleaseError::Expired => Display::fmt(self.description(), f),
            &PleaseError::__Nonexhaustive => unreachable!(),
        }
    }
}

impl<P: Error> Error for PleaseError<P> {
    fn description(&self) -> &str {
        match self {
            &PleaseError::Provider(ref e) => e.description(),
            &PleaseError::Query(ref e) => e.description(),
            &PleaseError::Expired => "The `please` handle has expired and can no longer be used",
            &PleaseError::__Nonexhaustive => unreachable!(),
        }
    }
    fn cause(&self) -> Option<&Error> {
        match self {
            &PleaseError::Provider(ref e) => Some(e),
            &PleaseError::Query(ref e) => Some(e),
            &PleaseError::Expired => None,
            &PleaseError::__Nonexhaustive => unreachable!(),
        }
    }
}

impl<P> From<diesel::result::Error> for PleaseError<P> {
    fn from(other: diesel::result::Error) -> Self {
        PleaseError::Query(other)
    }
}

struct ErrorWrapper<E, P>(E, PhantomData<fn(P) -> P>);
impl<P, E: From<PleaseError<P>>> From<diesel::result::Error> for ErrorWrapper<E, P> {
    fn from(other: diesel::result::Error) -> Self {
        ErrorWrapper(PleaseError::Query(other).into(), PhantomData)
    }
}
impl<P, E: From<PleaseError<P>>> From<PleaseError<P>> for ErrorWrapper<E, P> {
    fn from(other: PleaseError<P>) -> Self {
        ErrorWrapper(other.into(), PhantomData)
    }
}

/// Convenient alias for `Result` types returned from this library.
pub type PleaseResult<T, P> = Result<T, PleaseError<P>>;

/// Trait for types providing database connections. This is typically
/// implemented by an `Arc<ConnectionPool>` or functionally equivalent
/// type.
pub trait ConnectionProvider {
    /// Error type which may be returned when the provider is unable
    /// to obtain a database connection.
    type Error;
    /// Type of the connection returned. Currently the connection must
    /// use the postgres backend.
    type Connection: Connection<Backend=Pg>;
    /// Obtain a connection from this provider.
    fn get(&self) -> Result<Self::Connection, Self::Error>;
}

/// This handle identifies a long-running operation using a unique integer.
#[derive(Debug)]
pub struct PleaseHandle<P: ConnectionProvider> {
    provider: P,
    id: i32
}

impl<P: ConnectionProvider> PleaseHandle<P> {
    fn transaction_internal<R, E: From<PleaseError<P::Error>>, F: FnOnce(&P::Connection) -> Result<R, E>>(provider: &P, f: F) -> Result<R, E> {
        let conn = provider.get()
            .map_err(PleaseError::Provider)?;
        
        conn.transaction(|| f(&conn).map_err(|e| ErrorWrapper(e, PhantomData)))
            .map_err(|ErrorWrapper(e, _)| e)
    }

    /// Construct a new handle using the specified connection provider.
    /// 
    /// The connection provider is typically a connection pool of some kind,
    /// or it may be implemented by establishing a new connection each time.
    /// 
    /// The title is used as a human-readable name to identify this handle.
    /// This is useful if you are inspecting the database, or for logging
    /// expired handles.
    pub fn new(provider: P, title: &str) -> PleaseResult<Self, P::Error> {
        use self::schema::*;

        // Allocate a new ID
        let id: i32 = Self::transaction_internal(&provider, |conn| -> PleaseResult<i32, P::Error> {
            Ok(diesel::insert_into(please_ids::table)
                .values(&please_ids::title.eq(title))
                .returning(please_ids::id)
                .get_result(conn)?)
        })?;

        Ok(PleaseHandle { provider, id })
    }

    /// Convenience constructor.
    /// 
    /// Equivalent to calling `perform_cleanup` followed by `new`.
    /// If you wish to handle expired handles (eg. record them to a log) then
    /// call the methods individually.
    pub fn new_with_cleanup(provider: P, title: &str) -> PleaseResult<Self, P::Error> {
        let _ = Self::perform_cleanup(&provider);
        Self::new(provider, title)
    }

    /// Constructor to use from within an existing transaction.
    /// 
    /// Allows conditionally creating a handle without losing the atomicity
    /// of a single transaction.
    pub fn new_with_connection<C>(provider: P, title: &str, conn: &C) -> QueryResult<Self>
    where
        C: Connection<Backend=Pg> + ?Sized
    {
        use self::schema::*;

        let id = diesel::insert_into(please_ids::table)
            .values(&please_ids::title.eq(title))
            .returning(please_ids::id)
            .get_result(conn)?;

        Ok(PleaseHandle {
            provider,
            id,
        })
    }

    /// Explicitly clean up old handles. It is recommended to call this before
    /// creating a new handle.
    /// 
    /// This function returns the expired handles (if any) so that you can log them
    /// or use them for debugging.
    pub fn perform_cleanup(provider: &P) -> PleaseResult<Vec<ExpiredId>, P::Error> {
        use self::schema::*;

        Self::transaction_internal(provider, |conn| {
            diesel::delete(please_ids::table.filter(please_ids::expiry.lt(dsl::now)))
                .get_results(conn)
                .map_err(PleaseError::Query)
                .map(|v| {
                    v.into_iter().map(ExpiredId).collect()
                })

        })
    }

    /// Run a transaction as part of the operation this handle represents.
    /// 
    /// After beginning the transaction, this method validates that the handle
    /// has not expired, whilst also refreshing the expiry and taking a lock
    /// on the row, to prevent it being expired by another thread whilst this
    /// transaction is in progress.
    /// 
    /// The callback is passed two arguments: a reference to the connection,
    /// and the integer ID of this handle. Currently this is the only way to
    /// access the handle's ID as it is not recommended to use this ID outside
    /// of a transaction.
    pub fn transaction<R, E, F>(&mut self, f: F) -> Result<R, E>
    where
        E: From<PleaseError<P::Error>>,
        F: FnOnce(&P::Connection, i32) -> Result<R, E>
    {
        use self::schema::*;

        Self::transaction_internal(&self.provider, |conn| {
            // We simultaneously refresh the ID, *and* validate that
            // it hasn't expired.
            let num_rows = diesel::update(
                please_ids::table.filter(please_ids::id.eq(self.id))
            ).set(
                please_ids::refresh_count.eq(please_ids::refresh_count + 1)
            ).execute(conn).map_err(PleaseError::Query)?;

            if num_rows == 1 {
                // One row was updated, we're good
                f(conn, self.id)
            } else {
                // Presumably the row has expired and been removed
                Err(PleaseError::Expired.into())
            }
        })
    }

    /// Refreshes the handle, resetting the time before it will be automatically closed.
    /// 
    /// This is equivalent to running an empty transaction.
    pub fn refresh(&mut self) -> PleaseResult<(), P::Error> {
        // Just do an empty transaction
        self.transaction(|_conn, _id| Ok(()))
    }

    /// Expire the handle. Future operations on this handle will fail with the error `Expired`.
    /// 
    /// Useful for testing.
    pub fn expire(&mut self) -> PleaseResult<ExpiredId, P::Error> {
        use self::schema::*;

        Self::transaction_internal(&self.provider, |conn| {
            diesel::delete(
                    please_ids::table.filter(please_ids::id.eq(self.id))
                )
                .get_result(conn)
                .optional()?
                .ok_or(PleaseError::Expired)
        }).map(ExpiredId)
    }

    /// Close the handle, allowing any errors to be handled.
    /// 
    /// This is automatically called when a handle is dropped,
    /// but in that case errors are silently ignored.
    pub fn close(mut self) -> PleaseResult<(), P::Error> {
        self.expire()?;
        self.id = -1;
        Ok(())
    }

    /// Get the ID of this handle.
    /// 
    /// A good rule of thumb is to never use this outside of a transaction,
    /// as in that case it may not have been recently validated.
    pub fn id(&self) -> i32 { self.id }
}

impl<P: ConnectionProvider> Drop for PleaseHandle<P> {
    /// Closes the handle, ignoring any errors that might
    /// have occurred.
    fn drop(&mut self) {
        // Ignore errors here
        if self.id != -1 {
            let _ = self.expire();
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate dotenv;

    use super::*;
    use std::env;

    #[derive(Copy, Clone, Debug)]
    struct TestConnectionProvider;

    impl ConnectionProvider for TestConnectionProvider {
        type Connection = PgConnection;
        type Error = diesel::ConnectionError;

        fn get(&self) -> Result<PgConnection, Self::Error> {
            dotenv::dotenv().ok();
            PgConnection::establish(&env::var("DATABASE_URL").unwrap())
        }
    }

    fn new_handle(name: &str) -> PleaseHandle<TestConnectionProvider> {
        PleaseHandle::new(TestConnectionProvider, name)
            .expect("Failed to create handle")
    }

    fn new_handle_with_connection(name: &str) -> PleaseHandle<TestConnectionProvider> {
        let conn = TestConnectionProvider.get()
            .expect("Failed to get connection");

        conn.transaction(|| {
            PleaseHandle::new_with_connection(TestConnectionProvider, name, &conn)
        }).expect("Failed to run transaction")
    }

    #[test]
    fn smoke() {
        let mut handle = new_handle("smoke");

        handle.transaction(|_conn, _id| Ok::<(), PleaseError<_>>(()))
            .expect("Failed to run no-op transaction");

        handle.close()
            .expect("Failed to close handle");
    }

    #[test]
    fn smoke_with_connection() {
        let mut handle = new_handle_with_connection("smoke");

        handle.transaction(|_conn, _id| Ok::<(), PleaseError<_>>(()))
            .expect("Failed to run no-op transaction");

        handle.close()
            .expect("Failed to close handle");
    }

    #[test]
    fn two_handles() {
        let mut handle1 = new_handle("two_handles::1");
        let mut handle2 = new_handle("two_handles::2");

        handle1.transaction(|_conn1, id1| {
            handle2.transaction(|_conn2, id2| {
                assert_ne!(id1, id2);
                Ok::<(), PleaseError<_>>(())
            })
        }).expect("Failed to run transactions");

        handle1.close()
            .expect("Failed to close handle 1");
        handle2.close()
            .expect("Failed to close handle 2");
    }

    #[test]
    fn expiry() {
        let mut handle = new_handle("smoke");

        handle.expire()
            .expect("Failed to expire handle");

        let err = handle.transaction(|_conn, _id| Ok::<(), PleaseError<_>>(()))
            .expect_err("Transaction should fail on expired handle");
        
        assert_eq!(err, PleaseError::Expired);
    }
}
