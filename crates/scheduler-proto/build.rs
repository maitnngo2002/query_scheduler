fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Requires `protoc` on PATH (see README).
    println!("cargo:rerun-if-changed=../../proto/scheduler.proto");
    tonic_build::compile_protos("../../proto/scheduler.proto")?;
    Ok(())
}
