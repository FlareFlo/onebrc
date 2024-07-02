use std::collections::HashMap;
use std::env::args;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::ops::{Neg, Range};
use std::os::unix::fs::FileExt;
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::thread::{available_parallelism, JoinHandle};
use std::time::Instant;

#[derive(Copy, Clone, Debug)]
struct City {
    min: i64,
    max: i64,
    sum: i64,
    occurrences: u32,
}

impl City {
    pub fn add_new(&mut self, input: &[u8]) {
        let mut val = 0;
        let mut is_neg = false;
        for &char in input {
            match char {
                b'0'..=b'9' => {
                    val *= 10;
                    let digit = char - b'0';
                    val += digit as i64;
                }
                b'-' => {
                    is_neg = true;
                }
                b'.' => {}
                _ => {
                    panic!("encountered {} in value", char::from(char))
                }
            }
        }
        if is_neg {
            val = val.neg();
        }
        self.add_new_value(val);
    }

    pub fn add_new_value(&mut self, new: i64) {
        self.min = self.min.min(new);
        self.max = self.max.max(new);
        self.sum += new;
        self.occurrences += 1;
    }
    pub fn min(&self) -> f64 {
        self.min as f64 / 10.0
    }
    pub fn mean(&self) -> f64 {
        self.sum as f64 / self.occurrences as f64 / 10.0
    }
    pub fn max(&self) -> f64 {
        self.max as f64 / 10.0
    }

    pub fn add_result(&mut self, other: Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.occurrences += other.occurrences;
    }
}

impl Default for City {
    fn default() -> Self {
        Self {
            min: i64::MAX,
            max: i64::MIN,
            sum: 0,
            occurrences: 0,
        }
    }
}

#[derive(Default, Clone, Debug)]
struct Citymap {
    // Length then values
    pub map: HashMap<u32, (String, City)>,
}

fn hashstr(s: &str) -> u32 {
    let b = s.as_bytes();
    u32::from_le_bytes([s.len() as u8, b[0], b[1], b[2]])
}

impl Citymap {
    pub fn lookup(&mut self, lookup: &str) -> &mut City {
        let hash = hashstr(lookup);
        let get = self.map.get(&hash);
        if get.is_none() {
            self.map
                .insert(hash, (lookup.to_owned(), Default::default()));
        }
        &mut self.map.get_mut(&hash).unwrap().1
    }
    pub fn new() -> Self {
        Self {
            map: Default::default(),
        }
    }
    pub fn into_key_values(self) -> Vec<(String, City)> {
        self.map.into_iter().map(|(_, s)| s).collect()
    }
    pub fn merge_with(&mut self, rhs: Self) {
        for (k, v) in rhs.map.into_iter() {
            self.map
                .entry(k)
                .and_modify(|lhs| {
                    lhs.1.add_result(v.1);
                })
                .or_insert(v);
        }
    }
}

fn main() {
    let mut args = args();

    let start = Instant::now();
    let input = "measurements.txt";

    let results = if args.find(|e| e == "st").is_some() {
        citymap_single_thread(input)
    } else {
        citymap_multi_threaded(input)
    };

    print_results(results);

    println!("{:?}", start.elapsed());
}

fn citymap_single_thread(path: &str) -> Citymap {
    let f = File::open(path).unwrap();

    let mut buf = BufReader::with_capacity(10_usize.pow(8), f);
    citymap_naive(&mut buf)
}

fn citymap_multi_threaded(path: &str) -> Citymap {
    let cpus = available_parallelism().unwrap().get();
    let size = File::open(path).unwrap().metadata().unwrap().len();
    let per_thread = size / cpus as u64;

    let mut index = 0;
    let mut threads = vec![];
    let (sender, receiver) = channel();
    for i in 0..cpus {
        let range = index..({
            index += per_thread;
            index.min(size)
        });
        threads.push(citymap_thread(path.to_owned(), range, i, sender.clone()));
    }
    let mut ranges = (0..cpus)
        .into_iter()
        .map(|_| receiver.recv().unwrap())
        .collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|e| e.start);
    assert!(
        ranges.windows(2).all(|e| {
            let first = &e[0];
            let second = &e[1];
            first.end == second.start
        }),
        "Ranges overlap or have gaps: {ranges:?}"
    );
    let results = threads
        .into_iter()
        .map(|e| e.join().unwrap())
        //.map(|e|dbg!(e))
        .reduce(|mut left, right| {
            left.merge_with(right);
            left
        })
        .unwrap();
    results
}

fn citymap_thread(
    path: String,
    mut range: Range<u64>,
    i: usize,
    range_feedback: Sender<Range<u64>>,
) -> JoinHandle<Citymap> {
    thread::Builder::new()
        .name(format!("process_thread id: {i} assigned: {range:?}"))
        .spawn(move || {
            let mut file = File::open(path).unwrap();
            //println!("Before: {range:?}");

            // Perform alignment of buffer/range at the start
            {
                // Skip head alignment for start of file
                if range.start != 0 {
                    let mut head = vec![0; 50];
                    let len = file.read_at(&mut head, range.start).unwrap();
                    head.truncate(len);

                    for (i, &pos) in head.iter().enumerate() {
                        if pos == '\n' as u8 {
                            range.start += i as u64;
                            break;
                        }
                    }
                }

                // tail alignment
                {
                    let mut head = vec![0; 50];
                    let len = file.read_at(&mut head, range.end).unwrap();
                    head.truncate(len);

                    for (i, &pos) in head.iter().enumerate() {
                        if pos == '\n' as u8 {
                            range.end += i as u64;
                            break;
                        }
                    }
                }
            }

            // Notify main about alignment
            range_feedback.send(range.clone()).unwrap();
            //println!("After: {range:?}");
            // Ensure we remain within bounds of the designated file range
            file.seek(SeekFrom::Start(range.start)).unwrap();

            let limited = BufReader::with_capacity(10_usize.pow(5), file);
            let mut buffered = limited.take(range.end - range.start);
            citymap_naive(&mut buffered)
        })
        .unwrap()
}

fn citymap_naive(input: &mut impl BufRead) -> Citymap {
    let mut map = Citymap::new();
    let mut buf = Vec::with_capacity(50);
    loop {
        let read = input.read_until(b'\n', &mut buf).unwrap();
        // Stream has finished
        if read == 0 {
            break;
        }

        // Skip over just newline strings that get created by the alignment process
        if buf == &[b'\n'] {
            continue;
        }

        let mut city = None;
        let mut val = None;
        for (i, &char) in buf.iter().enumerate() {
            if char == b';' {
                city = Some(&buf[0..i]);
                val = Some(&buf[(i + 1)..(buf.len() - 1)]);
                break;
            }
        }
        if city.is_none() {
            panic!("String:---{}---", String::from_utf8(buf).unwrap());
        }
        #[cfg(not(feature = "unsafe"))]
        let entry = map.lookup(std::str::from_utf8(city.unwrap()).unwrap());

        #[cfg(feature = "unsafe")]
        let entry = map.lookup(unsafe { std::str::from_utf8_unchecked(city.unwrap()) });

        entry.add_new(val.unwrap());
        buf.clear();
    }
    map
}

fn print_results(map: Citymap) {
    let mut res = map.into_key_values();
    res.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    print!("{{");
    for (city, vals) in res {
        let min = vals.min();
        let mean = vals.mean();
        let max = vals.max();
        print!("{city}={min:.1}/{mean:.1}/{max:.1}, ")
    }
    println!("}}");
}
