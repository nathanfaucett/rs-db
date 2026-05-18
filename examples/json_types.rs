//! Example: JSON Type Support
//!
//! This example demonstrates the JSON type features available in aicacia.
//! To see JSON in action with SQL:
//!   cargo test --test facade_json

fn main() {
  println!("=== Aicacia JSON Type Support ===\n");

  println!("Features:");
  println!("✓ JSON type in table schemas");
  println!("✓ JSON value encoding/decoding");
  println!("✓ JSON Path extraction ($.field, $.array[0])");
  println!("✓ JSON object merging");
  println!("✓ JSON validation");
  println!("✓ Full codec support (roundtrip via KeyEncoding)\n");

  println!("Usage in SQL:");
  println!("  CREATE TABLE config (");
  println!("    id UUID PRIMARY KEY,");
  println!("    settings JSON");
  println!("  );\n");

  println!("  INSERT INTO config VALUES (");
  println!("    '00000000-0000-0000-0000-000000000001'::uuid,");
  println!("    '{{\"theme\": \"dark\", \"notifications\": true}}'");
  println!("  );\n");

  println!("JSON Operations:");
  println!("  • json_valid(value) - Check if value is valid JSON");
  println!("  • json_extract(json, path) - Extract using JSON Path");
  println!("  • json_merge(obj1, obj2) - Merge two JSON objects\n");

  println!("Storage:");
  println!("  • JSON stored as UTF-8 strings");
  println!("  • Tag byte 6 for codec layer");
  println!("  • Schemaless (any valid JSON accepted)\n");

  println!("Tests:");
  println!("  cargo test json_ops           # Unit tests");
  println!("  cargo test json_roundtrip     # Codec tests");
  println!("  cargo test facade_json        # Integration tests");
}
