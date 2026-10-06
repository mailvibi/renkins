use std::path::PathBuf;

use clap::{Parser, Subcommand};
use renkins::server::{load_server_config, serve};

#[derive(Debug, Parser)]
#[command(
    name = "renkins",
    version,
    about = "Single-node CI engine prototype",
    arg_required_else_help = true
)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Serve {
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,
    },
    Job {
        #[command(subcommand)]
        action: JobCommands,
    },
    Build {
        #[command(subcommand)]
        action: BuildCommands,
    },
}

#[derive(Debug, Subcommand)]
pub enum JobCommands {
    Create,
    List,
}

#[derive(Debug, Subcommand)]
pub enum BuildCommands {
    List,
    Show,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let result: Result<(), String> = match cli.command {
        Some(Commands::Serve { config }) => match load_server_config(config.as_deref()) {
            Ok(config) => serve(&config).await,
            Err(err) => Err(err),
        },
        Some(Commands::Job { .. }) | Some(Commands::Build { .. }) => {
            println!(
                "Renkins is bootstrapping; core domain validation and SQLite persistence are in place."
            );
            Ok(())
        }
        None => {
            println!(
                "Renkins is bootstrapping; core domain validation and SQLite persistence are in place."
            );
            Ok(())
        }
    };

    if let Err(err) = result {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}
