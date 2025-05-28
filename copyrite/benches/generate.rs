use copyrite::io::sums::channel::ChannelReader;
use copyrite::task::generate::GenerateTaskBuilder;
use copyrite::test::TestFileBuilder;
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::path::Path;
use tokio::fs::File;
use tokio::runtime::Runtime;

async fn channel_reader(path: &Path) {
    let reader = ChannelReader::new(File::open(path).await.unwrap(), 100);

    let result = GenerateTaskBuilder::default()
        .with_context(vec![
            "sha1".parse().unwrap(),
            "sha256".parse().unwrap(),
            "md5".parse().unwrap(),
            "crc32".parse().unwrap(),
            "crc32c".parse().unwrap(),
        ])
        .with_reader(reader)
        .build()
        .await
        .unwrap()
        .run()
        .await
        .unwrap();

    black_box(result);
}

fn criterion_benchmark(c: &mut Criterion) {
    let bench_file = TestFileBuilder::default()
        .generate_bench_defaults()
        .unwrap();

    c.bench_function("generate with channel reader", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| channel_reader(&bench_file))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
