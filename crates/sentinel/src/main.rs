//! `sentinel` вЂ” Agent Sentinel CLI.
//!
//! Phase 1 scope: demo-event generation into the real pipeline,
//! session listing and a timeline report with lifecycle diagnostics.

mod commands;
mod report;
mod tail;
mod watch;

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "sentinel", version, about = "Agent Sentinel вЂ” local agent observability runtime")]
struct Cli {
    /// Database file (default: ./sentinel.db in the current directory).
    #[arg(long, global = true, default_value = "sentinel.db")]
    db: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Generate a synthetic session through the real pipeline
    /// (conformance-harness seed; useful to validate the store).
    Demo {
        /// Inject anomalies: drop the closing event and duplicate one event.
        #[arg(long)]
        anomalies: bool,
    },
    /// List known sessions with basic lifecycle status.
    Sessions,
    /// Print a session timeline with lifecycle diagnostics.
    Report { session: uuid::Uuid },
    /// Run the spool-watcher daemon: watches the Claude Code hook
    /// spool and persists events in real time.
    Watch,
    /// One-shot import of spool files (no daemon).
    ImportSpool,
    /// One-shot import of Claude Code session transcripts from
    /// `~/.claude/projects` (out-of-band: the agent is never touched).
    ImportTranscripts {
        /// Projects directory (default: ~/.claude/projects).
        #[arg(long)]
        projects_dir: Option<PathBuf>,
    },
    /// Out-of-band daemon: tail Claude Code transcripts in real time
    /// and ingest. Claude Code is not modified in any way.
    Tail {
        /// Projects directory (default: ~/.claude/projects).
        #[arg(long)]
        projects_dir: Option<PathBuf>,
    },
    /// Analyze every recorded session and print sessions with
    /// findings (default severity >= warning).
    Audit {
        /// Minimum severity to report: info, warning or critical.
        #[arg(long, default_value = "warning")]
        severity: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Demo { anomalies } => commands::demo(cli.db, anomalies).await?,
        Command::Sessions => commands::sessions(cli.db).await?,
        Command::Report { session } => commands::report(cli.db, session).await?,
        Command::Watch => commands::watch(cli.db).await?,
        Command::ImportSpool => commands::import_spool(cli.db).await?,
        Command::ImportTranscripts { projects_dir } => {
            commands::import_transcripts(cli.db, projects_dir).await?
        }
        Command::Tail { projects_dir } => commands::tail(cli.db, projects_dir).await?,
        Command::Audit { severity } => commands::audit(cli.db, &severity).await?,
    }
    Ok(())
}
