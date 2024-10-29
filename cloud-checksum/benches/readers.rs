use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fs;
use std::path::PathBuf;

const RANDOM_SEED: u64 = 42;
const BENCHMARK_FILE_SIZE: usize = 1048576;

/// Generate a test file and return its path.
fn generate_test_file() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();

    fs::create_dir(root.join("data")).unwrap();

    let mut rng = StdRng::seed_from_u64(RANDOM_SEED);
    let mut buf = [0; BENCHMARK_FILE_SIZE];
    rng.fill_bytes(&mut buf);

    let file = root.join("data/benchmark");

    fs::write(&file, buf).unwrap();

    file
}

fn criterion_benchmark(c: &mut Criterion) {
    let file = generate_test_file();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
