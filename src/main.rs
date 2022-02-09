use color_eyre::eyre::{eyre, Result};
use rand::{distributions::Alphanumeric, Rng};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    pool::PoolConnection,
    Connection, Executor, MySql, Pool,
};
use tokio::spawn;
fn main() {
    println!("Hello, world!");
}

// This one succeeds, the tranasction it committed async and awaited
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_transaction_commit() {
    // setup fresh DB with migration
    let db = DatabaseGuard::new().await;
    // get connection
    let mut conn = &mut *db.conn().await;
    // the problem function
    let _ = some_transaction(&mut conn, false).await.unwrap();
    // now cleanup
    println!("end of test, cleanup");
    db.drop_async().await;
    println!("cleanup finished");
}

// This one always hangs during drop_async()
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_transaction_return() {
    // setup fresh DB with migration
    let db = DatabaseGuard::new().await;
    // get connection
    let mut conn = &mut *db.conn().await;
    // the problem function
    let _ = some_transaction(&mut conn, true).await.unwrap_err();
    // now cleanup
    println!("end of test, cleanup");
    db.drop_async().await;
    println!("cleanup finished");
}

async fn some_transaction(sql: &mut sqlx::MySqlConnection, early_return: bool) -> Result<()> {
    let sql_sel = "SELECT a FROM foo WHERE a = ?";
    let mut transaction = sql.begin().await?;

    let some_val = "asdf";

    // don't early return, insert valid entry
    if !early_return {
        sqlx::query("INSERT INTO foo (a) VALUES (?)")
            .bind(some_val)
            .execute(&mut transaction)
            .await?;
    }

    let res: Option<String> = sqlx::query_scalar::<_, String>(sql_sel)
        .bind(some_val)
        .fetch_optional(&mut transaction)
        .await?;
    match res {
        None => Err(eyre!("foobar")),
        Some(entry) => {
            dbg!(entry);
            // doing or not doing this is the essentials problem it seems
            transaction.commit().await?;

            Ok(())
        }
    }
}

async fn setup_db(sql: &mut sqlx::MySqlConnection) -> Result<()> {
    sqlx::query("CREATE TABLE foo (a VARCHAR(255))")
        .execute(sql)
        .await?;
    Ok(())
}

pub struct DatabaseGuard {
    pub db: Pool<MySql>,
    pub db_name: String,
}

impl DatabaseGuard {
    pub async fn new() -> Self {
        // ignore on purpose, can only be installed once
        // let _ = color_eyre::install();
        let rng = rand::thread_rng();
        let db_name: String = format!(
            "testing_temp_{}",
            rng.sample_iter(Alphanumeric)
                .take(7)
                .map(char::from)
                .collect::<String>()
        );
        println!("Temp DB: {}", db_name);

        let options = MySqlConnectOptions::new()
            .host("127.0.0.1")
            .port(3306)
            .username("root");

        let conn_uri = std::env::var("DATABASE_URL").ok();

        {
            let opts = MySqlPoolOptions::new().after_connect(|conn| Box::pin(async move {
                conn.execute("SET SESSION sql_mode=STRICT_ALL_TABLES; SET SESSION innodb_strict_mode=ON;").await.unwrap();
                Ok(())
            }));
            let db_pool = match conn_uri.as_deref() {
                Some(v) => opts.connect(v).await.unwrap(),
                None => opts.connect_with(options.clone()).await.unwrap(),
            };
            let conn = &mut *db_pool.acquire().await.unwrap();
            sqlx::query(format!("CREATE DATABASE {}", db_name).as_str())
                .execute(conn)
                .await
                .unwrap();
        }

        let options = options.database(&db_name);
        // hack to avoid problems with URI/ENV connections that already select a database
        // which prevents us from doing so via MySqlConnectOptions
        let conn_db_switch = format!("use `{}`", db_name);
        let opts = MySqlPoolOptions::new().max_connections(10).after_connect(move |conn| {
            let c = conn_db_switch.clone();
            Box::pin(async move {
                conn.execute(
                    "SET SESSION sql_mode=STRICT_ALL_TABLES; SET SESSION innodb_strict_mode=ON;",
                )
                .await
                .unwrap();
                conn.execute(c.as_str()).await.unwrap();
                Ok(())
            })
        });
        let db_pool = match conn_uri.as_deref() {
            Some(v) => opts.connect(v).await.unwrap(),
            None => opts.connect_with(options.clone()).await.unwrap(),
        };

        let conn = &mut *db_pool.begin().await.unwrap();

        let res = sqlx::query_as::<_, (String,)>("SELECT DATABASE() FROM DUAL")
            .fetch_optional(&mut *conn)
            .await
            .unwrap();
        println!("Selected Database: {:?}", res);

        setup_db(&mut *db_pool.begin().await.unwrap())
            .await
            .unwrap();

        Self {
            db: db_pool,
            db_name,
        }
    }

    pub async fn conn(&self) -> PoolConnection<MySql> {
        self.db.acquire().await.unwrap()
    }

    /// Has to be called manually, hack due to problem with async in drop code
    pub async fn drop_async(self) {
        sqlx::query(format!("DROP DATABASE IF EXISTS `{}`", self.db_name).as_str())
            .execute(&mut *self.db.acquire().await.unwrap())
            .await
            .unwrap();
    }
}

impl Drop for DatabaseGuard {
    fn drop(&mut self) {
        // TODO: fixme, doesn't actually work, blocking will result in a deadlock
        // let db = self.db.clone();
        // let name = self.db_name.clone();
        // spawn(async move {
        //     sqlx::query(format!("DROP DATABASE IF EXISTS `{}`", name).as_str())
        //         .execute(&mut *db.begin().await.unwrap())
        //         .await
        //         .unwrap();
        //     println!("Dropped");
        // });
    }
}
