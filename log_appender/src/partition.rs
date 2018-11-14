#[path = "segment.rs"] mod segment;

use std::io;
use std::fs;
use std::fs::File;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::path::Path;
use std::fs::OpenOptions;
use std::fs::DirEntry;

use self::segment::*;

/// A topic.
/// A topic may have one or more partitions.
/// A topic has atleast 1 partition by default.
/// More partitions can be added dynamically.
pub struct Topic {
  pub name: String,
  pub topic_ws: String,
  pub partitions: Vec<Partition>,
}

pub struct Partition {
  pub partition_id: u64,
  pub partition_ws: String,
  pub active_segment: Option<Segment>,
  pub old_segments: Vec<Segment>,
}

impl Topic {
  /// Creates a new topic under the kafka workspace
  pub fn new(kafka_root_dir: String, topic_name: String) -> Self {
    println!("NEW TOPIC");
    let topic_pathbuf = Path::new(&kafka_root_dir).join(&topic_name);
    let topic_path = topic_pathbuf.as_path();

    if topic_path.exists() {
      return Topic::create_from_existing(topic_path, topic_name);
    }
    /// New topic needs to be created with default partition 0
    let mut partitions: Vec<Partition> = Vec::new();
    let topic_path_str = topic_path.to_str().unwrap().to_owned();
    let part = Partition::new(topic_path_str.clone(), 0 as u64);
    partitions.push(part);

    Topic {
      name: topic_name,
      topic_ws: topic_path_str,
      partitions: partitions,
    }
  }

  /// Recreate the topic from the filesystem
  fn create_from_existing(topic_path: &Path, name: String) -> Self {
    let rd_itr = fs::read_dir(topic_path).unwrap();
    let mut partitions: Vec<Partition> = Vec::new();

    for entry in rd_itr {
      let entry = entry.unwrap();
      let p = entry.path();
      assert_eq!(p.is_dir(), true);
      let mut components = p.components().collect::<Vec<_>>();    
      // get the last component
      let lcomp = components.pop().unwrap();
      let pid = lcomp.as_os_str().to_str().unwrap().parse::<u64>().unwrap();

      partitions.push(Partition::new(
        topic_path.to_str().unwrap().to_owned(),
        pid,
      ));
    }

    Topic {
      name: name,
      topic_ws: topic_path.to_str().unwrap().to_owned(),
      partitions: partitions,
    }
  }

  /// Get the default partition
  fn get_default_partition(&self) -> &Partition {
    &self.partitions[0]
  }

  /// Get the default partition for mutable
  fn get_default_partition_mut(&mut self) -> &mut Partition {
    &mut self.partitions[0]
  }

  /// Write a block of data to the topic
  pub fn write(&mut self, data: byte_seq) -> () {
    //TODO: Writes to the default partition 
    let partition_ref = self.get_default_partition_mut();
    partition_ref.write_to_segment(data);
    ()
  }
}

impl Partition {
  /// Creates a new partition
  pub fn new(topic_path: String, partition_id: u64) -> Self {
    let pid_str = partition_id.to_string();
    let pathbuf = Path::new(&topic_path).join(&pid_str);
    let partition_ws = pathbuf.as_path().to_str().unwrap().to_owned();
    println!("partition: {}", partition_ws);
    fs::create_dir_all(&partition_ws).unwrap();

    Partition {
      partition_id: partition_id,
      partition_ws: partition_ws,
      active_segment: None,
      old_segments: Vec::new(),
    }
  }

  /// Create a segment under this partition
  fn create_segment_and_assign(&mut self) -> () {
    if self.active_segment.is_none() {
      let s_pathbuf = Path::new(&self.partition_ws).join("0.log");
      let i_pathbuf = Path::new(&self.partition_ws).join("0.index");
      let s_path_str = s_pathbuf.to_str().unwrap().to_owned();
      let i_path_str = i_pathbuf.to_str().unwrap().to_owned();

      let segment = Segment::new(self.partition_id, s_path_str, i_path_str);
      self.active_segment = Some(segment.unwrap());
    } else {
      // TODO: Read the current active segment and replace it
    }

    ()
  }

  /// Write the data to the active segment
  fn write_to_segment(&mut self, data: byte_seq) -> () {
    if self.active_segment.is_none() {
      self.create_segment_and_assign();
    } else {
      // TODO:
    }
    if let Some(ref mut segment) = self.active_segment {
      segment.append_one(data);
    } else {
      println!("ERROR: No active segment found!!");
    }

    ()
  }
}