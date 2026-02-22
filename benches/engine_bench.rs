use breakout1_kv_store::Engine;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use std::sync::Arc;
use std::thread;
use tempfile::NamedTempFile;

fn bench_set(c: &mut Criterion) {
    c.bench_function("set_single_key", |b| {
        let file = NamedTempFile::new().unwrap();
        let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            engine
                .set(
                    black_box(format!("key{}", i).as_bytes()),
                    black_box(b"value"),
                )
                .unwrap();
            i += 1;
        });
    });
}

fn bench_get_existing(c: &mut Criterion) {
    c.bench_function("get_existing_key", |b| {
        let file = NamedTempFile::new().unwrap();
        let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
        for i in 0..1000u32 {
            engine
                .set(
                    format!("key{}", i).as_bytes(),
                    format!("value{}", i).as_bytes(),
                )
                .unwrap();
        }
        let mut i = 0u32;
        b.iter(|| {
            let key = format!("key{}", i % 1000);
            black_box(engine.get(black_box(key.as_bytes())).unwrap());
            i = i.wrapping_add(1);
        });
    });
}

fn bench_get_missing(c: &mut Criterion) {
    c.bench_function("get_missing_key", |b| {
        let file = NamedTempFile::new().unwrap();
        let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
        engine.set(b"exists", b"yes").unwrap();
        b.iter(|| {
            black_box(engine.get(black_box(b"nonexistent")).unwrap());
        });
    });
}

fn bench_overwrite(c: &mut Criterion) {
    c.bench_function("overwrite_same_key", |b| {
        let file = NamedTempFile::new().unwrap();
        let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            engine
                .set(black_box(b"same_key"), black_box(&i.to_le_bytes()))
                .unwrap();
            i += 1;
        });
    });
}

fn bench_delete(c: &mut Criterion) {
    c.bench_function("delete_key", |b| {
        let file = NamedTempFile::new().unwrap();
        let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key{}", i);
            engine.set(key.as_bytes(), b"value").unwrap();
            engine.del(black_box(key.as_bytes())).unwrap();
            i += 1;
        });
    });
}

fn bench_set_then_get(c: &mut Criterion) {
    c.bench_function("set_then_get", |b| {
        let file = NamedTempFile::new().unwrap();
        let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key{}", i);
            engine.set(key.as_bytes(), b"value").unwrap();
            black_box(engine.get(key.as_bytes()).unwrap());
            i += 1;
        });
    });
}

fn bench_compact(c: &mut Criterion) {
    c.bench_function("compact_after_overwrites", |b| {
        b.iter_batched(
            || {
                let file = NamedTempFile::new().unwrap();
                let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
                for i in 0..500u32 {
                    engine.set(b"k", &i.to_le_bytes()).unwrap();
                }
                (engine, file)
            },
            |(engine, _file)| {
                engine.compact().unwrap();
            },
            BatchSize::PerIteration,
        );
    });
}

fn bench_load_rebuild_index(c: &mut Criterion) {
    c.bench_function("load_rebuild_index_1000_keys", |b| {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_owned();
        {
            let engine = Engine::load(&path).unwrap();
            for i in 0..1000u32 {
                engine
                    .set(
                        format!("key{}", i).as_bytes(),
                        format!("value{}", i).as_bytes(),
                    )
                    .unwrap();
            }
        }
        b.iter(|| {
            black_box(Engine::load(&path).unwrap());
        });
    });
}

fn bench_large_value(c: &mut Criterion) {
    c.bench_function("set_get_4kb_value", |b| {
        let file = NamedTempFile::new().unwrap();
        let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
        let large_val = vec![0xABu8; 4096];
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key{}", i);
            engine.set(key.as_bytes(), black_box(&large_val)).unwrap();
            black_box(engine.get(key.as_bytes()).unwrap());
            i += 1;
        });
    });
}

fn bench_concurrent_reads(c: &mut Criterion) {
    c.bench_function("concurrent_reads_8_threads", |b| {
        b.iter_batched(
            || {
                let file = NamedTempFile::new().unwrap();
                let engine = Arc::new(Engine::load_with_threshold(file.path(), u64::MAX).unwrap());
                for i in 0..1000u32 {
                    engine
                        .set(
                            format!("key{}", i).as_bytes(),
                            format!("value{}", i).as_bytes(),
                        )
                        .unwrap();
                }
                (engine, file)
            },
            |(engine, _file)| {
                let mut handles = vec![];
                for _ in 0..8 {
                    let engine = Arc::clone(&engine);
                    handles.push(thread::spawn(move || {
                        for i in 0..1000u32 {
                            black_box(engine.get(format!("key{}", i).as_bytes()).unwrap());
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
}

fn bench_concurrent_writes(c: &mut Criterion) {
    c.bench_function("concurrent_writes_4_threads", |b| {
        b.iter_batched(
            || {
                let file = NamedTempFile::new().unwrap();
                let engine = Arc::new(Engine::load_with_threshold(file.path(), u64::MAX).unwrap());
                (engine, file)
            },
            |(engine, _file)| {
                let mut handles = vec![];
                for t in 0..4 {
                    let engine = Arc::clone(&engine);
                    handles.push(thread::spawn(move || {
                        for i in 0..250u32 {
                            let key = format!("t{}_k{}", t, i);
                            engine.set(key.as_bytes(), b"value").unwrap();
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
}

fn bench_mixed_workload(c: &mut Criterion) {
    c.bench_function("mixed_set_get_del_1000_ops", |b| {
        b.iter_batched(
            || {
                let file = NamedTempFile::new().unwrap();
                let engine = Engine::load_with_threshold(file.path(), u64::MAX).unwrap();
                (engine, file)
            },
            |(engine, _file)| {
                for i in 0..1000u32 {
                    let key = format!("key{}", i);
                    engine.set(key.as_bytes(), b"value").unwrap();
                    if i % 3 == 0 {
                        black_box(engine.get(key.as_bytes()).unwrap());
                    }
                    if i % 5 == 0 {
                        engine.del(key.as_bytes()).unwrap();
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(
    benches,
    bench_set,
    bench_get_existing,
    bench_get_missing,
    bench_overwrite,
    bench_delete,
    bench_set_then_get,
    bench_compact,
    bench_load_rebuild_index,
    bench_large_value,
    bench_concurrent_reads,
    bench_concurrent_writes,
    bench_mixed_workload,
);
criterion_main!(benches);
