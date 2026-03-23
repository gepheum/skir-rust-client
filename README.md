# skir-rust-client

Runtime library for [Skir](https://skir.build)-generated Rust code.

This crate is generated automatically by
[skir-rust-gen](https://github.com/gepheum/skir-rust-gen) — you typically do
not need to add it to your `Cargo.toml` manually. The generated output already
includes the necessary `[dependencies]` entry.

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
skir-client = "0.1"
```

Then import the generated package from your `.skir` file (e.g. `user.skir`):

```rust
use my_project::skirout::user;
// Now you can use: user::User_builder(), user::User_serializer(), etc.
```

See the [skir-rust-example](https://github.com/gepheum/skir-rust-example)
project for a complete working example.

## License

MIT
# skir-rust-client
