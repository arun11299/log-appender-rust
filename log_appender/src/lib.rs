mod index;
mod segment;
mod partition;

#[test]
fn create_segment() {
  segment::Segment::new(0 as u64, "0.log".to_owned(), "0.index".to_owned());
}

#[test]
fn create_partition() {
  let mut topic = partition::Topic::new(".".to_owned(), "topic-1".to_owned());
  topic.write(b"this is some data");
}