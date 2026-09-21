#![forbid(unsafe_code)]

use std::{future::Future, process::ExitCode};

#[tokio::main]
#[cfg(not(test))]
async fn main() -> ExitCode {
    run(crabbot_runtime::daemon()).await
}

async fn run<F>(future: F) -> ExitCode
where
    F: Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
{
    match future.await {
        Ok(()) => ExitCode::SUCCESS,

        Err(error) => {
            tracing::error!(
                error = %crabbot_runtime::redact_diagnostic(sentence(error.to_string())),
                "Daemon failed."
            );
            ExitCode::FAILURE
        }
    }
}

fn sentence(value: String) -> String {
    let value = value.trim().trim_end_matches('.');
    let mut chars = value.chars();

    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => "Unknown error".into(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn formats_errors() {
        assert_eq!(super::sentence("daemon stopped.".into()), "Daemon stopped");
        assert_eq!(super::sentence("".into()), "Unknown error");
    }

    #[tokio::test]
    async fn returns_success_or_failure() {
        assert_eq!(super::run(async { Ok(()) }).await, std::process::ExitCode::SUCCESS);
        let error: Box<dyn std::error::Error + Send + Sync> = "failed".into();

        assert_eq!(super::run(async { Err(error) }).await, std::process::ExitCode::FAILURE);
    }
}
