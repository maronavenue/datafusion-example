use datafusion::prelude::*;
use std::sync::Arc;

mod custom_table_provider_csv;
use custom_table_provider_csv::CustomDataSourceCsv;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a new execution context
    let ctx = SessionContext::new();

    // A. Register a natively supported source (CSV)
    ctx.register_csv("simple_csv", "tests/data/example.csv", CsvReadOptions::new())
        .await?;

    // B. Register a custom source using `TableProvider` (Same CSV)
    let custom_table_provider = CustomDataSourceCsv::new("tests/data/example.csv");
    let _ = ctx.register_table("custom_csv", Arc::new(custom_table_provider));

    // Create a plan to run a SQL query against simple_csv table
    let df = ctx
        .sql("SELECT c, b FROM simple_csv")
        .await?;

    // Execute and print results
    df.show().await?;

    // Create a plan to run a SQL query against custom_csv table
    let df = ctx
        .sql("SELECT c, b FROM custom_csv")
        .await?;

    // Execute and print results
    df.show().await?;

    Ok(())
}