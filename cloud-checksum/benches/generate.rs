use cloud_checksum::reader::channel::ChannelReader;
use cloud_checksum::task::generate::GenerateTask;
use cloud_checksum::Checksum;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::runtime::Runtime;

const RANDOM_SEED: u64 = 42;
const BENCHMARK_FILE_SIZE: usize = 10485760;

/// Generate a test file and return its path.
fn generate_test_file() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();

    fs::create_dir_all(root.join("data")).unwrap();

    let file = root.join("data/benchmark_file");

    if !file.exists() {
        let mut rng = StdRng::seed_from_u64(RANDOM_SEED);
        let mut buf = vec![0; BENCHMARK_FILE_SIZE];
        rng.fill_bytes(&mut buf);

        let file = root.join("data/benchmark_file");

        fs::write(&file, buf).unwrap();
    }

    file
}

async fn channel_reader(path: &Path) {
    let reader = ChannelReader::new(File::open(path).await.unwrap(), 1000);

    GenerateTask::default()
        .add_generate_tasks(
            vec![Checksum::MD5, Checksum::SHA1, Checksum::SHA256],
            &reader,
            |digest, checksum| {
                black_box(digest);
                black_box(checksum);
            },
        )
        .add_reader_task(reader)
        .unwrap()
        .run()
        .await
        .unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let path = generate_test_file();

    c.bench_function("generate with channel reader", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| channel_reader(&path))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
