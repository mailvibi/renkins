pub mod api;
pub mod config;
pub mod domain;
pub mod executor;
pub mod git;
pub mod lifecycle;
pub mod pipeline;
pub mod scheduler;
pub mod server;
pub mod storage;

pub fn app_name() -> &'static str {
    "renkins"
}
