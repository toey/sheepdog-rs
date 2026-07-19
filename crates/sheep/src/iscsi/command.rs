//! SCSI command response data assembly.
//!
//! Standard SCSI commands (INQUIRY, READ_CAPACITY, MODE_SENSE, TEST_UNIT_READY,
//! REQUEST_SENSE, REPORT_LUNS, START_STOP_UNIT, SYNCHRONIZE_CACHE) are handled
//! natively by the `iscsi-target` crate using the `ScsiBlockDevice` trait methods:
//! - `vendor_id()`, `product_id()`, `product_rev()`: Used for INQUIRY responses
//! - `capacity()`, `block_size()`: Used for READ_CAPACITY responses
//! - `flush()`: Used for SYNCHRONIZE_CACHE responses
//!
//! No explicit command assembly code is required for these standard commands.
//!
//! ## Advanced SCSI Commands (P3 - Nice-to-have)
//!
//! The following SCSI commands are P3 (nice-to-have) features not covered by the
//! basic `ScsiBlockDevice` trait:
//!
//! - **UNMAP (TRIM/DISCARD, opcode 0x42)**: Can be simulated via write-zeroes to the
//!   specified LBA range. The `iscsi-target` crate's `ScsiBlockDevice` trait does not
//!   include an `unmap` method. Support would require extending the trait or implementing
//!   explicit UNMAP SCSI command handling in the iSCSI target layer, mapping UNMAP to
//!   write-zeroes operations.
//!
//! - **VERIFY (opcodes 0x2F/0x8F), WRITE AND VERIFY (0x2D/0x8D), COMPARE AND WRITE (0x39/0x89)**:
//!   These commands are P3 features not covered by the basic `ScsiBlockDevice` trait.
//!   Implementation options include:
//!   1. Extend the `ScsiBlockDevice` trait with methods like `verify(lba, data)`,
//!      `write_and_verify(lba, data)`, `compare_and_write(lba, expected_data, new_data)`.
//!   2. Implement explicit SCSI command handlers that map VERIFY to read-then-compare,
//!      WRITE AND VERIFY to write-then-read-compare, and Compare And Write to
//!      read-compare-write.
//!
//! The `iscsi-target` crate's `ScsiOpcode` enum includes `Verify10` (0x2F) and `Verify16` (0x8F),
//! so VERIFY commands may be handled internally by the crate via read-then-compare logic.
//! UNMAP, WRITE AND VERIFY, and COMPARE AND WRITE opcodes are not in the `ScsiOpcode` enum,
//! so the `iscsi-target` crate may return an "invalid command opcode" error for these commands.
