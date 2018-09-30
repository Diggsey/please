use std::marker::PhantomData;

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

#[derive(Debug, Clone)]
pub struct ExpiredId(PleaseId);


#[derive(Debug)]
pub enum PleaseError<P: ConnectionProvider> {
    Provider(P::Error),
    Query(diesel::result::Error),
    Expired
}

impl<P: ConnectionProvider> From<diesel::result::Error> for PleaseError<P> {
    fn from(other: diesel::result::Error) -> Self {
        PleaseError::Query(other)
    }
}

struct ErrorWrapper<E, P>(E, PhantomData<fn(P) -> P>);
impl<P: ConnectionProvider, E: From<PleaseError<P>>> From<diesel::result::Error> for ErrorWrapper<E, P> {
    fn from(other: diesel::result::Error) -> Self {
        ErrorWrapper(PleaseError::Query(other).into(), PhantomData)
    }
}
impl<P: ConnectionProvider, E: From<PleaseError<P>>> From<PleaseError<P>> for ErrorWrapper<E, P> {
    fn from(other: PleaseError<P>) -> Self {
        ErrorWrapper(other.into(), PhantomData)
    }
}

pub type PleaseResult<T, P> = Result<T, PleaseError<P>>;

pub trait ConnectionProvider {
    type Error;
    type Connection: Connection<Backend=Pg>;
    fn get(&self) -> Result<Self::Connection, Self::Error>;
}

pub struct PleaseHandle<P: ConnectionProvider> {
    provider: P,
    id: i32
}

impl<P: ConnectionProvider> PleaseHandle<P> {
    fn transaction_internal<R, E: From<PleaseError<P>>, F: FnOnce(&P::Connection) -> Result<R, E>>(provider: &P, f: F) -> Result<R, E> {
        let conn = provider.get()
            .map_err(PleaseError::Provider)?;
        
        conn.transaction(|| f(&conn).map_err(|e| ErrorWrapper(e, PhantomData)))
            .map_err(|ErrorWrapper(e, _)| e)
    }

    fn close_internal(&mut self) -> PleaseResult<(), P> {
        use self::schema::*;

        if self.id != -1 {
            self.id = -1;
            Self::transaction_internal(&self.provider, |conn| {
                let num_rows = diesel::delete(please_ids::table.filter(
                    please_ids::id.eq(self.id)
                )).execute(conn)?;

                if num_rows == 1 {
                    Ok(())
                } else {
                    Err(PleaseError::Expired)
                }
            })
        } else {
            Ok(())
        }
    }

    pub fn perform_cleanup(provider: &P) -> PleaseResult<Vec<ExpiredId>, P> {
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

    pub fn new(provider: P, title: &str) -> PleaseResult<Self, P> {
        use self::schema::*;

        // Clean up orphaned IDs, but we don't care if it fails
        let _ = Self::perform_cleanup(&provider);

        // Allocate a new ID
        let id: i32 = Self::transaction_internal(&provider, |conn| -> PleaseResult<i32, P> {
            Ok(diesel::insert_into(please_ids::table)
                .values(&please_ids::title.eq(title))
                .returning(please_ids::id)
                .get_result(conn)?)
        })?;

        Ok(PleaseHandle { provider, id })
    }

    pub fn transaction<R, E: From<PleaseError<P>>, F: FnOnce(&P::Connection, i32) -> Result<R, E>>(&self, f: F) -> Result<R, E> {
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

    pub fn refresh(&self) -> PleaseResult<(), P> {
        // Just do an empty transaction
        self.transaction(|_conn, _id| Ok(()))
    }

    pub fn close(mut self) -> PleaseResult<(), P> {
        self.close_internal()
    }
}

impl<P: ConnectionProvider> Drop for PleaseHandle<P> {
    fn drop(&mut self) {
        // Ignore errors here
        let _ = self.close_internal();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
