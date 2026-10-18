pub mod api;
pub mod config;
pub mod data_dir;
pub mod domain;
pub mod executor;
pub mod git;
pub mod lifecycle;
pub mod logs;
pub mod pipeline;
pub mod scheduler;
pub mod server;
pub mod storage;
pub mod worker;

pub fn app_name() -> &'static str {
    "renkins"
}
