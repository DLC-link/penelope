#![doc = include_str!("../README.md")]

mod probe;
mod scenario;

pub use probe::{Class, FATAL_STREAK, ProbeDiag};
pub use scenario::{BoxFut, BoxProbe, Fixture, Scenario};
