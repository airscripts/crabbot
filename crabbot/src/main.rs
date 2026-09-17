#![forbid(unsafe_code)]

#[tokio::main]
async fn main() -> std::process::ExitCode {
    crabbot_runtime::cli().await
}
