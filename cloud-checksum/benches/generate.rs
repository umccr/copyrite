use cloud_checksum::reader::channel::ChannelReader;
use cloud_checksum::task::generate::GenerateTask;
use cloud_checksum::test::TestFileBuilder;
use cloud_checksum::Checksum;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::path::Path;
use tokio::fs::File;
use tokio::runtime::Runtime;

async fn channel_reader(path: &Path) {
    let mut reader = ChannelReader::new(File::open(path).await.unwrap(), 100);

    GenerateTask::default()
        .add_generate_tasks(
            vec![Checksum::MD5, Checksum::SHA1, Checksum::SHA256],
            &mut reader,
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
