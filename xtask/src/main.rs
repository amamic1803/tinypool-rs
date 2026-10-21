use clap::{Parser, Subcommand};
use std::error::Error;
use std::path::PathBuf;
use xshell::{Shell, cmd};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

trait Executable {
    fn execute(self, sh: &Shell) -> Result<(), Box<dyn Error>>;
}

#[derive(Subcommand)]
enum Commands {
    /// Run all CI tasks
    All,
    /// Build the project
    Build,
    /// Run tests
    Test,
    /// Generate documentation
    Docs,
    /// Run clippy
    Clippy,
    /// Check formatting with rustfmt
    Format,
}
impl Commands {
    fn build(sh: &Shell) -> Result<(), Box<dyn Error>> {
        cmd!(sh, "cargo build --workspace --exclude xtask --all-features").run()?;
        Ok(())
    }

    fn test(sh: &Shell) -> Result<(), Box<dyn Error>> {
        cmd!(sh, "cargo test --all-features").run()?;
        Ok(())
    }

    fn docs(sh: &Shell) -> Result<(), Box<dyn Error>> {
        cmd!(sh, "cargo doc --all-features").run()?;
        Ok(())
    }

    fn clippy(sh: &Shell) -> Result<(), Box<dyn Error>> {
        cmd!(
            sh,
            "cargo clippy --all-features --all-targets -- -D warnings"
        )
        .run()?;
        Ok(())
    }

    fn format(sh: &Shell) -> Result<(), Box<dyn Error>> {
        cmd!(sh, "cargo fmt --all --check").run()?;
        Ok(())
    }
}
impl Executable for Commands {
    fn execute(self, sh: &Shell) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Build => Self::build(sh),
            Self::Test => Self::test(sh),
            Self::Docs => Self::docs(sh),
            Self::Clippy => Self::clippy(sh),
            Self::Format => Self::format(sh),
            Self::All => {
                println!("Running all CI tasks...");

                println!("Building the project...");
                Self::build(sh)?;

                println!("Running tests...");
                Self::test(sh)?;

                println!("Generating documentation...");
                Self::docs(sh)?;

                println!("Running clippy...");
                Self::clippy(sh)?;

                println!("Checking formatting...");
                Self::format(sh)?;

                println!("All CI tasks completed successfully.");
                Ok(())
            }
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let sh = Shell::new()?;

    // change the current working directory to the root of the workspace
    let project_root = workspace_root();
    sh.change_dir(&project_root);

    // execute the command
    cli.command.execute(&sh)
}

/// Returns the root of the current workspace
fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points to xtask/
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // go up to the workspace root
    path
}
