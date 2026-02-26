use anyhow::Context;
use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use rustlake_core::RustLakeConfig;
use rustlake_engine::RustLakeContext;
use rustlake_router::{QueryClassifier, QueryType};

#[derive(Parser)]
#[command(
    name = "rustlake",
    about = "RustLake — The All-Rust Data Platform",
    version,
    long_about = "A complete Databricks alternative built entirely on Rust.\nPowered by Apache Arrow, DataFusion, and Iceberg."
)]
struct Cli {
    /// Path to config file (TOML)
    #[arg(short, long, global = true)]
    config: Option<String>,

    /// Output format
    #[arg(short, long, global = true, default_value = "table")]
    format: OutputFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, clap::ValueEnum)]
enum OutputFormat {
    Table,
    Json,
    Csv,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a SQL query
    Query {
        /// The SQL query to execute
        sql: String,
    },

    /// Start the API server
    Serve {
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        /// Port to bind to
        #[arg(long, default_value = "3000")]
        port: u16,
    },

    /// Table management
    Tables {
        #[command(subcommand)]
        action: TableCommands,
    },
}

#[derive(Subcommand)]
enum TableCommands {
    /// List all registered tables
    List,

    /// Register a file as a table
    Register {
        /// Table name
        #[arg(short, long)]
        name: String,

        /// Path to data file (Parquet or CSV)
        #[arg(short, long)]
        path: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();

    // Load config
    let config = match &cli.config {
        Some(path) => RustLakeConfig::from_file(path)
            .context(format!("Failed to load config from '{}'", path))?,
        None => RustLakeConfig::default(),
    };

    match cli.command {
        Commands::Query { sql } => {
            run_query(&config, &sql, &cli.format).await?;
        }
        Commands::Serve { host, port } => {
            println!("Starting RustLake API server on {}:{}...", host, port);
            println!("(Use `rustlake-api` binary for full server functionality)");
            // For the CLI, we just run a quick server inline
            run_inline_server(&config, &host, port).await?;
        }
        Commands::Tables { action } => match action {
            TableCommands::List => {
                let ctx = RustLakeContext::new(config).await?;
                let tables = ctx.list_tables().await?;
                if tables.is_empty() {
                    println!("No tables registered.");
                } else {
                    println!("Registered tables:");
                    for t in &tables {
                        println!("  - {}", t);
                    }
                }
            }
            TableCommands::Register { name, path } => {
                let ctx = RustLakeContext::new(config).await?;
                ctx.register_table(&name, &path).await?;
                println!("Table '{}' registered from '{}'", name, path);

                // Show schema
                let batches = ctx.sql(&format!("DESCRIBE {}", name)).await?;
                print_batches(&batches, &OutputFormat::Table)?;
            }
        },
    }

    Ok(())
}

async fn run_query(
    config: &RustLakeConfig,
    sql: &str,
    format: &OutputFormat,
) -> anyhow::Result<()> {
    let ctx = RustLakeContext::new(config.clone()).await?;

    // Classify the query
    let query_type = QueryClassifier::classify(sql).unwrap_or(QueryType::Olap);
    eprintln!("Query type: {}", query_type);

    // Execute
    let batches = ctx.sql(sql).await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    print_batches(&batches, format)?;
    eprintln!("\n({} rows)", total_rows);

    Ok(())
}

fn print_batches(batches: &[RecordBatch], format: &OutputFormat) -> anyhow::Result<()> {
    match format {
        OutputFormat::Table => {
            let formatted = pretty_format_batches(batches)?;
            println!("{}", formatted);
        }
        OutputFormat::Json => {
            let mut buf = Vec::new();
            let mut writer = arrow::json::ArrayWriter::new(&mut buf);
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
            let json_str = String::from_utf8(buf).unwrap_or_default();
            println!("{}", json_str);
        }
        OutputFormat::Csv => {
            for batch in batches {
                // Print header for first batch
                let schema = batch.schema();
                let headers: Vec<&str> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                println!("{}", headers.join(","));

                // Print rows using arrow_cast's display
                for row in 0..batch.num_rows() {
                    let values: Vec<String> = (0..batch.num_columns())
                        .map(|col| {
                            arrow_cast::display::ArrayFormatter::try_new(
                                batch.column(col).as_ref(),
                                &arrow_cast::display::FormatOptions::default(),
                            )
                            .map(|f| f.value(row).to_string())
                            .unwrap_or_default()
                        })
                        .collect();
                    println!("{}", values.join(","));
                }
            }
        }
    }
    Ok(())
}

async fn run_inline_server(
    _config: &RustLakeConfig,
    _host: &str,
    _port: u16,
) -> anyhow::Result<()> {
    println!("Inline server not yet implemented. Use `cargo run --bin rustlake-api` instead.");
    Ok(())
}
