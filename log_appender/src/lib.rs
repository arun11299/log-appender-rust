mod index;
mod segment;

#[test]
fn create_segment() {
  segment::Segment::new(0, "hello.txt".to_owned());
}