//! Version spike for DataFusion 55.1.
//!
//! Goal: see what a real multi-partition physical plan looks like for the
//! example query, which operators mark exchange boundaries, and whether a plan
//! survives a datafusion-proto round trip and still executes.
//!
//!     cargo run -p df-spike
//!
//! Paste the output back to guide the plan adapter design.

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::*;
use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};

const QUERY: &str = "\
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

async fn write_parquet(ctx: &SessionContext, select: &str, path: &std::path::Path) -> Result<()> {
    let sql = format!("COPY ({select}) TO '{}' STORED AS PARQUET", path.display());
    ctx.sql(&sql).await?.collect().await?;
    Ok(())
}

fn print_tree(plan: &Arc<dyn ExecutionPlan>, depth: usize) {
    println!("{}{}", "  ".repeat(depth), plan.name());
    for child in plan.children() {
        print_tree(child, depth + 1);
    }
}

fn collect_names(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
    let name = plan.name().to_string();
    if !out.contains(&name) {
        out.push(name);
    }
    for child in plan.children() {
        collect_names(child, out);
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("df-spike");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    // Four partitions, like a four-worker cluster.
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4).with_information_schema(true));

    // Prefer partitioned (shuffle) joins over collecting the whole left side.
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

    let df = ctx.sql(QUERY).await?;
    let plan = df.clone().create_physical_plan().await?;

    println!("== Physical plan ==");
    println!("{}", displayable(plan.as_ref()).indent(false));

    println!("== Operator tree ==");
    print_tree(&plan, 0);

    println!("\n== Fragments (df-adapter cut) ==");
    match df_adapter::cut(&plan) {
        Ok(cut) => println!("{cut}"),
        Err(e) => println!("cut failed: {e}"),
    }

    println!("\n== Dynamic-filter settings ==");
    ctx.sql("SELECT name, value FROM information_schema.df_settings WHERE name LIKE '%dynamic%'")
        .await?
        .show()
        .await?;

    let mut names = Vec::new();
    collect_names(&plan, &mut names);
    println!("\n== Distinct operators ==");
    for n in &names {
        println!("{n}");
    }

    println!("\n== Query result ==");
    df.show().await?;

    // Serialize, deserialize, and run the decoded plan.
    let bytes = physical_plan_to_bytes(Arc::clone(&plan))?;
    let decoded = physical_plan_from_bytes(&bytes, &ctx.task_ctx())?;
    println!("== Round trip ==");
    println!("serialized size: {} bytes", bytes.len());
    println!("debug output identical after round trip: {}", format!("{plan:?}") == format!("{decoded:?}"));

    let batches = collect(Arc::clone(&decoded), ctx.task_ctx()).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("decoded plan executed, {rows} result row(s)");
    Ok(())
}
