#[path = "index.rs"] mod index;

use std::io;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::fs::OpenOptions;

const MAX_SEGMENT_BYTES : u64 = 1 * 1024; // 1 KB

/// The type to represent the offset of the record
/// from the first record in the segment.
type Offset = u64;

pub struct Segment {
  /// The partition to which the segment belongs to.
  pub partition_id : u32,
  /// The segment id
  pub segment_id : u64,
  /// The segment file name.
  pub file_name : String,
  /// The file handle.
  pub fileh : File,
  /// Number of records inserted in the segment.
  pub records : u64,
  /// Total bytes consumed to store records.
  /// Cannot be more than MAX_SEGMENT_BYTES.
  pub bytes_consumed : u64,
  /// The index structure
  index : index::Index,
}

pub enum SegmentError {
  /// Error while creation of segment file
  FileError(io::Error),
}

pub enum SegmentWriteError {
  WriteError(io::Error),
}

impl From<io::Error> for SegmentError {
  fn from(err : io::Error) -> SegmentError {
    SegmentError::FileError(err)
  }
}

impl From<io::Error> for SegmentWriteError {
  fn from(err : io::Error) -> SegmentWriteError {
    SegmentWriteError::WriteError(err)
  }
}

impl Segment {
  /// Creates a new segment
  pub fn new(partition_id: u32, seg_fname: String, idx_fname: String) -> Result<Self, SegmentError> {
    // If the path exists, read the file and get the number
    // of records and the bytes consumed
    let path = Path::new(&seg_fname);
    let index;
    let file_sz;

    let segment_id = path.to_str().unwrap().parse::<u64>().unwrap();

    if path.exists() {
      let metadata = fs::metadata(path) ?;
      file_sz = metadata.len();
      // Assumption that index file would also be present
      index = index::Index::read_from_file(segment_id, idx_fname);
    } else {
      index = index::Index::new(segment_id);
      file_sz = 0;
    }
    // Try opening the provided file
    let file = OpenOptions::new()
              .write(true) 
              .append(true)
              .create(true)
              .open(&seg_fname) ?;

    Ok(Segment {
      partition_id : partition_id,
      segment_id : segment_id,
      file_name : seg_fname,
      fileh : file,
      //TODO:
      records : 0,
      bytes_consumed : file_sz,
      index : index,
    })
  }

  /// Append the data to the segment. It is assumed that the data 
  /// appended corresponds to only one entry.
  pub fn append_one(&mut self, data : &[u8]) -> Result<(), SegmentWriteError> {
    self.fileh.write(data) ?;

    let index_entry = index::IndexEntry::new(
      self.records, // The offset of this record starting from 0
      self.bytes_consumed,
      data.len() as u64,
      0,
    );

    self.bytes_consumed += data.len() as u64;
    self.records += 1;
    self.index.entries.push(index_entry);

    Ok(())
  }
}