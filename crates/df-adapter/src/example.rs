//! The worked-example query, its sample data, and a session configured the way
//! the spike configures it. Used by tests and demos; not part of the adapter.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::error::Result;
use datafusion::prelude::*;

pub const QUERY: &str = "\
SELECT c.segment, COUNT(*) AS orders, SUM(o.total) AS revenue \
FROM orders o JOIN customers c ON o.cust_id = c.id \
WHERE o.order_date >= DATE '2024-01-01' \
GROUP BY c.segment \
ORDER BY revenue DESC";

const ORDERS: &str = "\
SELECT value AS order_id, \
       (value % 100) + 1 AS cust_id, \
       CAST(value % 50 AS DOUBLE) AS total, \
       CASE WHEN value % 2 = 0 THEN DATE '2024-03-01' ELSE DATE '2023-06-01' END AS order_date \
FROM generate_series(1, 20000)";

const CUSTOMERS: &str = "\
SELECT value AS id, \
       CASE value % 3 WHEN 0 THEN 'retail' WHEN 1 THEN 'wholesale' ELSE 'online' END AS segment \
FROM generate_series(1, 100)";

async fn write_parquet(ctx: &SessionContext, select: &str, path: &Path) -> Result<()> {
    let sql = format!("COPY ({select}) TO '{}' STORED AS PARQUET", path.display());
    ctx.sql(&sql).await?.collect().await?;
    Ok(())
}

/// A session with 4 target partitions and partitioned (shuffle) joins, with the
/// `orders` and `customers` tables registered from freshly written Parquet files.
pub async fn context() -> Result<SessionContext> {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "df-adapter-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let mut config = SessionConfig::new().with_target_partitions(4);
    // Dynamic filters share state between a join and a scan in the same process and make
    // each join partition wait for the others to report. Once partitions run as separate
    // tasks that wait would never end, so they must be off for distributed plans.
    config.options_mut().optimizer.enable_dynamic_filter_pushdown = false;
    let ctx = SessionContext::new_with_config(config);
    for stmt in [
        "SET datafusion.optimizer.hash_join_single_partition_threshold = 0",
        "SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0",
    ] {
        if let Err(e) = ctx.sql(stmt).await {
            eprintln!("note: could not apply `{stmt}`: {e}");
        }
    }

    let orders = dir.join("orders.parquet");
    let customers = dir.join("customers.parquet");
    write_parquet(&ctx, ORDERS, &orders).await?;
    write_parquet(&ctx, CUSTOMERS, &customers).await?;
    ctx.register_parquet("orders", orders.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    ctx.register_parquet("customers", customers.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    Ok(ctx)
}
