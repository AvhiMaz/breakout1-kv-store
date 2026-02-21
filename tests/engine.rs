use breakout1_kv_store::Engine;
use breakout1_kv_store::constants::DEFAULT_COMPACT_THRESHOLD;
use std::fs;
use std::sync::Arc; 
use std::thread;    
use tempfile::NamedTempFile;

fn temp_engine() -> (Engine, NamedTempFile) {
    let file = NamedTempFile::new().unwrap();
    let engine = Engine::load(file.path()).unwrap();
    (engine, file)
}

#[test]
fn test_set_and_get() {
    let (engine, _f) = temp_engine(); 
    engine.set(b"name".to_vec(), b"alice".to_vec()).unwrap();
    assert_eq!(engine.get(b"name").unwrap(), Some(b"alice".to_vec()));
}

#[test]
fn test_get_nonexistent_key_returns_none() {
    let (engine, _f) = temp_engine(); 
    assert_eq!(engine.get(b"ghost").unwrap(), None);
}

#[test]
fn test_delete_key() {
    let (engine, _f) = temp_engine(); 
    engine.set(b"key".to_vec(), b"value".to_vec()).unwrap();
    engine.del(b"key".to_vec()).unwrap();
    assert_eq!(engine.get(b"key").unwrap(), None);
}

#[test]
fn test_delete_nonexistent_key_is_ok() {
    let (engine, _f) = temp_engine(); 
    engine.del(b"nothing".to_vec()).unwrap();
}

#[test]
fn test_overwrite_key() {
    let (engine, _f) = temp_engine(); 
    engine.set(b"k".to_vec(), b"v1".to_vec()).unwrap();
    engine.set(b"k".to_vec(), b"v2".to_vec()).unwrap();
    assert_eq!(engine.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn test_multiple_keys() {
    let (engine, _f) = temp_engine(); 
    engine.set(b"a".to_vec(), b"1".to_vec()).unwrap();
    engine.set(b"b".to_vec(), b"2".to_vec()).unwrap();
    engine.set(b"c".to_vec(), b"3".to_vec()).unwrap();

    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(engine.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn test_index_rebuilt_after_reload() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    {
        let engine = Engine::load(&path).unwrap(); 
        engine.set(b"foo".to_vec(), b"bar".to_vec()).unwrap();
        engine.set(b"hello".to_vec(), b"world".to_vec()).unwrap();
    }

    let engine = Engine::load(&path).unwrap(); 
    assert_eq!(engine.get(b"foo").unwrap(), Some(b"bar".to_vec()));
    assert_eq!(engine.get(b"hello").unwrap(), Some(b"world".to_vec()));
}

#[test]
fn test_delete_persists_after_reload() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    {
        let engine = Engine::load(&path).unwrap(); 
        engine.set(b"key".to_vec(), b"val".to_vec()).unwrap();
        engine.del(b"key".to_vec()).unwrap();
    }

    let engine = Engine::load(&path).unwrap(); 
    assert_eq!(engine.get(b"key").unwrap(), None);
}

#[test]
fn test_overwrite_persists_after_reload() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();

    {
        let engine = Engine::load(&path).unwrap(); 
        engine.set(b"k".to_vec(), b"old".to_vec()).unwrap();
        engine.set(b"k".to_vec(), b"new".to_vec()).unwrap();
    }

    let engine = Engine::load(&path).unwrap(); 
    assert_eq!(engine.get(b"k").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn test_empty_value() {
    let (engine, _f) = temp_engine(); 
    engine.set(b"empty".to_vec(), b"".to_vec()).unwrap();
    assert_eq!(engine.get(b"empty").unwrap(), Some(b"".to_vec()));
}

#[test]
fn test_large_value() {
    let (engine, _f) = temp_engine(); 
    let large_val = vec![0xABu8; DEFAULT_COMPACT_THRESHOLD as usize];
    engine.set(b"big".to_vec(), large_val.clone()).unwrap();
    assert_eq!(engine.get(b"big").unwrap(), Some(large_val));
}

#[test]
fn test_binary_keys_and_values() {
    let (engine, _f) = temp_engine(); 
    let key = vec![0x00, 0xFF, 0x42, 0x13];
    let val = vec![0xDE, 0xAD, 0xBE, 0xEF];
    engine.set(key.clone(), val.clone()).unwrap();
    assert_eq!(engine.get(&key).unwrap(), Some(val));
}

#[test]
fn test_many_overwrites_index_stays_correct() {
    let (engine, _f) = temp_engine(); 
    for i in 0..100u32 {
        engine
            .set(b"counter".to_vec(), i.to_le_bytes().to_vec())
            .unwrap();
    }
    assert_eq!(
        engine.get(b"counter").unwrap(),
        Some(99u32.to_le_bytes().to_vec())
    );
}

#[test]
fn test_compact_live_keys_still_readable() {
    let (engine, _f) = temp_engine(); 
    engine.set(b"a".to_vec(), b"1".to_vec()).unwrap();
    engine.set(b"b".to_vec(), b"2".to_vec()).unwrap();
    engine.compact().unwrap();
    assert_eq!(engine.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn test_compact_removes_stale_entries() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();
    let engine = Engine::load(&path).unwrap(); 

    for i in 0..50u32 {
        engine.set(b"k".to_vec(), i.to_le_bytes().to_vec()).unwrap();
    }

    let size_before = fs::metadata(&path).unwrap().len();
    engine.compact().unwrap();
    let size_after = fs::metadata(&path).unwrap().len();

    assert!(size_after < size_before);
    assert_eq!(
        engine.get(b"k").unwrap(),
        Some(49u32.to_le_bytes().to_vec())
    );
}

#[test]
fn test_compact_drops_deleted_keys() {
    let (engine, _f) = temp_engine(); 
    engine.set(b"gone".to_vec(), b"bye".to_vec()).unwrap();
    engine.del(b"gone".to_vec()).unwrap();
    engine.compact().unwrap();
    assert_eq!(engine.get(b"gone").unwrap(), None);
}

#[test]
fn test_compact_empty_engine() {
    let (engine, _f) = temp_engine(); 
    engine.compact().unwrap();
    assert_eq!(engine.get(b"anything").unwrap(), None);
}

#[test]
fn test_auto_compact_triggered_by_threshold() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();
    let threshold = 512;
    let engine = Engine::load_with_threshold(&path, threshold).unwrap(); 

    for i in 0..200u32 {
        engine
            .set(b"key".to_vec(), i.to_le_bytes().to_vec())
            .unwrap();
    }

    let size = fs::metadata(&path).unwrap().len();
    assert!(size < threshold * 10);
    assert_eq!(
        engine.get(b"key").unwrap(),
        Some(199u32.to_le_bytes().to_vec())
    );
}

// ==================== New Multithreading Tests ====================

#[test]
fn test_concurrent_reads() {
    let (engine, _f) = temp_engine();
    let engine = Arc::new(engine);

    for i in 0..100u32 {
        engine.set(format!("key{}", i).into_bytes(), format!("value{}", i).into_bytes()).unwrap();
    }

    let mut handles = vec![];
    for _ in 0..8 {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..100u32 {
                let result = engine.get(format!("key{}", i).as_bytes()).unwrap();
                assert_eq!(result, Some(format!("value{}", i).into_bytes()));
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_writes_different_keys() {
    let (engine, _f) = temp_engine();
    let engine = Arc::new(engine);

    let mut handles = vec![];
    for t in 0..4 {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..50u32 {
                let key = format!("t{}_k{}", t, i);
                let value = format!("t{}_v{}", t, i);
                engine.set(key.into_bytes(), value.into_bytes()).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for t in 0..4 {
        for i in 0..50u32 {
            let key = format!("t{}_k{}", t, i);
            let expected = format!("t{}_v{}", t, i);
            assert_eq!(engine.get(key.as_bytes()).unwrap(), Some(expected.into_bytes()));
        }
    }
}

#[test]
fn test_concurrent_writes_same_key() {
    let (engine, _f) = temp_engine();
    let engine = Arc::new(engine);

    let mut handles = vec![];
    for t in 0..4 {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..100u32 {
                engine.set(b"shared".to_vec(), format!("t{}_i{}", t, i).into_bytes()).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert!(engine.get(b"shared").unwrap().is_some());
}

#[test]
fn test_concurrent_reads_and_writes() {
    let (engine, _f) = temp_engine();
    let engine = Arc::new(engine);

    for i in 0..50u32 {
        engine.set(format!("key{}", i).into_bytes(), format!("value{}", i).into_bytes()).unwrap();
    }

    let mut handles = vec![];

    for _ in 0..4 {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                for i in 0..50u32 {
                    let _ = engine.get(format!("key{}", i).as_bytes());
                }
            }
        }));
    }

    for t in 0..2 {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..100u32 {
                let key = format!("new_{}_{}", t, i);
                let value = format!("val_{}_{}", t, i);
                engine.set(key.into_bytes(), value.into_bytes()).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..50u32 {
        assert_eq!(
            engine.get(format!("key{}", i).as_bytes()).unwrap(),
            Some(format!("value{}", i).into_bytes())
        );
    }
}

#[test]
fn test_concurrent_reads_during_compaction() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_owned();
    let engine = Arc::new(Engine::load(&path).unwrap());

    for i in 0..100u32 {
        engine.set(format!("key{}", i % 10).into_bytes(), format!("value{}", i).into_bytes()).unwrap();
    }

    let mut handles = vec![];

    for _ in 0..4 {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                for i in 0..10u32 {
                    let result = engine.get(format!("key{}", i).as_bytes()).unwrap();
                    assert!(result.is_some());
                }
            }
        }));
    }

    {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            engine.compact().unwrap();
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..10u32 {
        assert!(engine.get(format!("key{}", i).as_bytes()).unwrap().is_some());
    }
}

#[test]
fn test_concurrent_writes_and_deletes() {
    let (engine, _f) = temp_engine();
    let engine = Arc::new(engine);

    let mut handles = vec![];

    {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..200u32 {
                engine.set(format!("key{}", i).into_bytes(), format!("value{}", i).into_bytes()).unwrap();
            }
        }));
    }

    {
        let engine = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in (0..200u32).step_by(2) {
                engine.del(format!("key{}", i).into_bytes()).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    engine.set(b"final".to_vec(), b"test".to_vec()).unwrap();
    assert_eq!(engine.get(b"final").unwrap(), Some(b"test".to_vec()));
}