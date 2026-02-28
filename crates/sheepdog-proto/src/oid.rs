/// Object ID type and manipulation functions.
///
/// Object ID layout (64 bits):
/// - Bits  0-31: data object space (32 bits)
/// - Bits 32-55: VDI object space (24 bits)
/// - Bits 56-59: reserved VDI object space (4 bits)
/// - Bits 60-63: object type identifier (4 bits)

use std::fmt;

use crate::constants::*;

const VDI_SPACE_SHIFT: u64 = 32;
const SD_VDI_MASK: u64 = 0x00FF_FFFF_0000_0000;
const VDI_BIT: u64 = 1u64 << 63;
const VMSTATE_BIT: u64 = 1u64 << 62;
const VDI_ATTR_BIT: u64 = 1u64 << 61;
const VDI_BTREE_BIT: u64 = 1u64 << 60;
const LEDGER_BIT: u64 = 1u64 << 59;

/// A 64-bit object identifier in the sheepdog object store.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct ObjectId(pub u64);

impl ObjectId {
    /// Create a new ObjectId from a raw u64.
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Get the raw u64 value.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Check if this is a VDI (inode) object.
    #[inline]
    pub const fn is_vdi_obj(self) -> bool {
        self.0 & VDI_BIT != 0
    }

    /// Check if this is a VM state object.
    #[inline]
    pub const fn is_vmstate_obj(self) -> bool {
        self.0 & VMSTATE_BIT != 0
    }

    /// Check if this is a VDI attribute object.
    #[inline]
    pub const fn is_vdi_attr_obj(self) -> bool {
        self.0 & VDI_ATTR_BIT != 0
    }

    /// Check if this is a VDI BTree object.
    #[inline]
    pub const fn is_vdi_btree_obj(self) -> bool {
        self.0 & VDI_BTREE_BIT != 0
    }

    /// Check if this is a ledger object.
    #[inline]
    pub const fn is_ledger_obj(self) -> bool {
        self.0 & LEDGER_BIT != 0
    }

    /// Check if this is a data object (not any special type).
    #[inline]
    pub const fn is_data_obj(self) -> bool {
        !self.is_vdi_obj()
            && !self.is_vmstate_obj()
            && !self.is_vdi_attr_obj()
            && !self.is_vdi_btree_obj()
            && !self.is_ledger_obj()
    }

    /// Extract the VDI id from this object ID.
    #[inline]
    pub const fn to_vid(self) -> u32 {
        ((self.0 & SD_VDI_MASK) >> VDI_SPACE_SHIFT) as u32
    }

    /// Get the data object index from a data OID.
    #[inline]
    pub const fn data_index(self) -> u64 {
        self.0 & (MAX_DATA_OBJS - 1)
    }

    /// Convert a ledger OID to its corresponding data OID.
    #[inline]
    pub const fn ledger_to_data(self) -> Self {
        Self(!LEDGER_BIT & self.0)
    }

    /// Convert a data OID to its corresponding ledger OID.
    #[inline]
    pub const fn data_to_ledger(self) -> Self {
        Self(LEDGER_BIT | self.0)
    }

    /// Get the size of the object referred to by this OID.
    pub const fn obj_size(self) -> u64 {
        if self.is_vdi_obj() {
            // SD_INODE_SIZE approximation — actual depends on struct size
            // We'll use the inline data index size as a reasonable estimate
            return (SD_INODE_DATA_INDEX as u64) * 4 + 4096; // header + data_vdi_id + gref
        }
        if self.is_vdi_attr_obj() {
            // sizeof(sheepdog_vdi_attr)
            return (SD_MAX_VDI_LEN + SD_MAX_VDI_TAG_LEN + 8 + 4 + 4
                + SD_MAX_VDI_ATTR_KEY_LEN + SD_MAX_VDI_ATTR_VALUE_LEN) as u64;
        }
        if self.is_vdi_btree_obj() {
            return (SD_INODE_DATA_INDEX as u64) * 4; // SD_INODE_DATA_INDEX_SIZE
        }
        if self.is_ledger_obj() {
            return SD_LEDGER_OBJ_SIZE;
        }
        SD_DATA_OBJ_SIZE
    }

    /// Create a VDI (inode) OID from a VDI id.
    #[inline]
    pub const fn from_vid(vid: u32) -> Self {
        Self(VDI_BIT | ((vid as u64) << VDI_SPACE_SHIFT))
    }

    /// Create a data OID from a VDI id and data index.
    #[inline]
    pub const fn from_vid_data(vid: u32, idx: u64) -> Self {
        Self(((vid as u64) << VDI_SPACE_SHIFT) | idx)
    }

    /// Create a VM state OID from a VDI id and index.
    #[inline]
    pub const fn from_vid_vmstate(vid: u32, idx: u32) -> Self {
        Self(VMSTATE_BIT | ((vid as u64) << VDI_SPACE_SHIFT) | idx as u64)
    }

    /// Create a VDI attribute OID from a VDI id and attribute id.
    #[inline]
    pub const fn from_vid_attr(vid: u32, attr_id: u32) -> Self {
        Self(((vid as u64) << VDI_SPACE_SHIFT) | VDI_ATTR_BIT | attr_id as u64)
    }

    /// Create a BTree OID from a VDI id and btree id.
    #[inline]
    pub const fn from_vid_btree(vid: u32, btree_id: u32) -> Self {
        Self(((vid as u64) << VDI_SPACE_SHIFT) | VDI_BTREE_BIT | btree_id as u64)
    }

    /// Count the number of data objects needed for the given VDI size.
    pub const fn count_data_objs(vdi_size: u64) -> u64 {
        (vdi_size + SD_DATA_OBJ_SIZE - 1) / SD_DATA_OBJ_SIZE
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OID({:#018x})", self.0)
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl From<u64> for ObjectId {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<ObjectId> for u64 {
    fn from(oid: ObjectId) -> Self {
        oid.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vdi_oid() {
        let oid = ObjectId::from_vid(42);
        assert!(oid.is_vdi_obj());
        assert!(!oid.is_data_obj());
        assert_eq!(oid.to_vid(), 42);
    }

    #[test]
    fn test_data_oid() {
        let oid = ObjectId::from_vid_data(42, 100);
        assert!(oid.is_data_obj());
        assert!(!oid.is_vdi_obj());
        assert_eq!(oid.to_vid(), 42);
        assert_eq!(oid.data_index(), 100);
    }

    #[test]
    fn test_vmstate_oid() {
        let oid = ObjectId::from_vid_vmstate(42, 3);
        assert!(oid.is_vmstate_obj());
        assert!(!oid.is_data_obj());
        assert_eq!(oid.to_vid(), 42);
    }

    #[test]
    fn test_attr_oid() {
        let oid = ObjectId::from_vid_attr(42, 7);
        assert!(oid.is_vdi_attr_obj());
        assert_eq!(oid.to_vid(), 42);
    }

    #[test]
    fn test_btree_oid() {
        let oid = ObjectId::from_vid_btree(42, 1);
        assert!(oid.is_vdi_btree_obj());
        assert_eq!(oid.to_vid(), 42);
    }

    #[test]
    fn test_ledger_round_trip() {
        let data_oid = ObjectId::from_vid_data(42, 100);
        let ledger_oid = data_oid.data_to_ledger();
        assert!(ledger_oid.is_ledger_obj());
        assert_eq!(ledger_oid.ledger_to_data(), data_oid);
    }

    #[test]
    fn test_count_data_objs() {
        assert_eq!(ObjectId::count_data_objs(0), 0);
        assert_eq!(ObjectId::count_data_objs(1), 1);
        assert_eq!(ObjectId::count_data_objs(SD_DATA_OBJ_SIZE), 1);
        assert_eq!(ObjectId::count_data_objs(SD_DATA_OBJ_SIZE + 1), 2);
    }
}
