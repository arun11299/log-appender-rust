
use std::fs;
use std::mem;
use std::slice;
use std::path::Path;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;

/// In memory representation of the Index log
pub struct Index {
  /// The segment corresponding to this index
  pub segment_id : u64,
  /// The index file name
  pub file_name  : String,
  /// The index file handle
      fileh      : File,
  /// All the index entries
  pub entries    : Vec<IndexEntry>,
}

/// Represents a single entry in the
/// index file
#[derive(Copy, Clone)]
pub struct IndexEntry {
  /// The record offset in the segment  
  pub offset    : u64,
  /// The seek position in the segment
  pub position  : u64,
  /// The size of the record in bytes
  pub size      : u64,
  /// The epoch timestamp of index creation
  pub timestamp : u64,
}

impl IndexEntry {
  /// Create an instance of IndexEntry
  pub fn new(offset : u64, pos : u64, size : u64, ts : u64) -> Self {
    IndexEntry {
      offset : offset,
      position : pos,
      size : size,
      timestamp : ts,
    }
  }
}

impl Index {
  /// Create an instance of Index
  pub fn new(segment_id : u64, parent_dir : &Path) -> Self {
    let mut idx_fname = segment_id.to_string();
    idx_fname.push_str(".index");
    let pbuf = Path::new(parent_dir.to_str().unwrap()).join(&idx_fname);
    let fname = pbuf.to_str().unwrap().to_owned();

    let file = OpenOptions::new()
               .write(true)
               .append(true)
               .create(true)
               .open(&fname)
               .unwrap();

    let v = Vec::new();
    Index {
      segment_id : segment_id,
      file_name : fname,
      fileh : file,
      entries : v,
    }
  }

  /// Write the index entry to the file
  pub fn write(&mut self, entry: &IndexEntry) -> () {
    let entry_size = mem::size_of::<IndexEntry>();
    unsafe {
      let entry_ptr = entry as *const IndexEntry as *const u8;
      let slice = slice::from_raw_parts(entry_ptr, entry_size);
      self.fileh.write(slice);
    }

    ()
  }

  /// Fill the index from file
  pub fn read_from_file(segment_id: u64, fname: String) -> Self {
    /// Works on the assumption that the file is present
    let file = OpenOptions::new()
               .read(true)
               .open(&fname).unwrap();

    let mut buf_reader = BufReader::new(file);

    let metadata = fs::metadata(&fname).unwrap();
    let mut file_sz = metadata.len();

    let mut entries = Vec::new();
    let entry_size = mem::size_of::<IndexEntry>();

    // Check if there are any partial writes to index.
    // TODO: Error handling on partial writes.
    assert_eq!(file_sz % entry_size as u64, 0);

    // Read entire file
    while (file_sz > 0) {
      let mut entry : IndexEntry = unsafe { mem::zeroed() };

      unsafe {
        let dst_ptr = &mut entry as *mut IndexEntry as *mut u8;
        let mut slice = slice::from_raw_parts_mut(dst_ptr, entry_size);
        buf_reader.read(slice);
      }

      entries.push(entry);
      file_sz -= entry_size as u64;
    }

    // Open up a write handle to the index
    let wfile = OpenOptions::new()
               .write(true)
               .append(true)
               .create(false)
               .open(&fname).unwrap();

    Index {
      segment_id : segment_id,
      file_name : fname,
      fileh : wfile,
      entries : entries,
    }
  }
}