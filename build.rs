fn main() {
    let include_path = std::env::var("DEP_ROCKSDB_INCLUDE")
        .expect("librocksdb-sys didn't expose an include path for RocksDB");

    let include_paths = std::env::split_paths(&include_path);
    let mut config = cpp_build::Config::new();

    for include_path in include_paths {
        config.include(&include_path);
    }

    config.build("src/lib.rs");
}
