#[path = "index.rs"] mod index;

use std::io;
use std::fs;
use std::fs::File;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::path::Path;
use std::fs::OpenOptions;

use self::index::*;

const MAX_SEGMENT_BYTES : u64 = 1 * 1024; // 1 KB

/// The type to represent the offset of the record
/// from the first record in the segment.
pub type Offset = u64;
/// The type to represent a byte sequence.
/// Essentially a slice of u8.
pub type byte_seq<'a> = &'a [u8];

pub struct Segment {
  /// The partition to which the segment belongs to.
  pub partition_id : u64,
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
  index : Index,
}

#[derive(Debug)]
pub enum SegmentError {
  /// Error while creation of segment file
  FileError(io::Error),
}

#[derive(Debug)]
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
  pub fn new(partition_id: u64, seg_fname: String, idx_fname: String) -> Result<Self, SegmentError> {
    // If the path exists, read the file and get the number
    // of records and the bytes consumed
    let path = Path::new(&seg_fname);
    let index;
    let file_sz;

    let segment_id = path.file_stem().unwrap()
                     .to_str().unwrap()
                     .parse::<u64>().unwrap()
                     ;

    if path.exists() {
      let metadata = fs::metadata(path) ?;
      file_sz = metadata.len();
      // Assumption that index file would also be present
      index = Index::read_from_file(segment_id, idx_fname);
    } else {
      let parent_path_ref = path.parent().unwrap();
      index = Index::new(segment_id, parent_path_ref);
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
  pub fn append_one(&mut self, data : byte_seq) -> Result<(), SegmentWriteError> {
    self.fileh.write(data) ?;

    let index_entry = IndexEntry::new(
      self.records, // The offset of this record starting from 0
      self.bytes_consumed,
      data.len() as u64,
      0, // TODO: Timestamp
    );

    self.bytes_consumed += data.len() as u64;
    self.records += 1;
    self.index.write(&index_entry);
    self.index.entries.push(index_entry);

    Ok(())
  }

  /// A generic append API for clients to write a single block
  /// of byte slice consisting of many records.
  /// In this case, the clients would have to provide the list
  /// of index entries.
  pub fn append(&mut self, data: byte_seq, mut index_entries: Vec<IndexEntry>) -> Result<(), SegmentWriteError> {
    self.fileh.write(data) ?;

    self.bytes_consumed += data.len() as u64;
    self.records += index_entries.len() as u64;
    self.index.entries.append(&mut index_entries);

    Ok(())
  }

  /// 
  fn get_index_for_offset(&self, offset: u64) -> Option<IndexEntry> {
    let res = self.index.entries.binary_search_by(
      |elem : &IndexEntry| {
        elem.offset.cmp(&offset)
      }
    );

    res.ok()
       .map(|idx| self.index.entries[idx])
  }

  ///
  pub fn read_content_at_offset(&mut self, offset: u64) -> Option<Vec<u8>> {
    match self.get_index_for_offset(offset) {
      None => None,
      Some(ientry) => {
        self.fileh.seek(SeekFrom::Start(ientry.position));
        let mut buf: Vec<u8> = Vec::new();
        buf.reserve(ientry.size as usize);
        self.fileh.read_exact(&mut buf).unwrap();
        Some(buf)
      }
    }
  }
}