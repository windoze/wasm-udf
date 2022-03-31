# WASM UDF for PySpark

1. Add wasm target with rustup
```
rustup target add wasm32-unknown-unknown
```

2. Install `wasm-gc`
```
cargo install wasm-gc
```

3. Compile Rust lib into WASM file and strip unused symbols
```
cd rust-udf
cargo build --target wasm32-unknown-unknown --release
wasm-gc target/wasm32-unknown-unknown/release/rust_udf.wasm
```

4. Install Python dependencies
```
pip3 install -r requirements.txt
```

5. Run `test.py`
