use std::collections::{BTreeMap, HashMap};
use std::{fs, thread};
use std::fs::File;
use std::hash::{DefaultHasher, Hasher};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, stdout, Take};
use std::ops::{Neg, Range};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::ptr::read;
use std::sync::{Arc, OnceLock};
use std::sync::mpsc::{channel, Sender};
use std::thread::{available_parallelism, current, JoinHandle, sleep};
use std::time::{Duration, Instant};

#[derive(Copy, Clone, Debug)]
struct City {
    min: i64,
    max: i64,
    sum: i64,
    occurrences: u32,
}

impl City {
    pub fn add_new_str(&mut self, input: &str) {
        let mut val = 0;
        let mut is_neg = false;
        for char in input.chars() {
            match char {
                '0'..='9' => {
                    val *= 10;
                    let digit = char as u8 - '0' as u8;
                    val += digit as i64;
                }
                '-' => {
                    is_neg = true;
                }
                '.' => {

                }
                _ => {
                    panic!("encountered {char} in value")
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
    pub map: HashMap<String, City>,
}

impl Citymap {
    pub fn lookup(&mut self, lookup: &str) -> &mut City {
           let mut get = self.map.get(lookup);
            if get.is_none() {
                self.map.insert(lookup.to_owned(), Default::default());
            }
            self.map.get_mut(lookup).unwrap()
    }
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    pub fn into_key_values(self) -> Vec<(String, City)> {
        self.map.into_iter().collect()
    }
    pub fn merge_with(&mut self, rhs: Self) {
        for (k, v) in rhs.map.into_iter() {
            self.map.entry(k)
                .and_modify(|lhs|{
                    lhs.add_result(v);
                })
                .or_insert(v);
        }
    }
}

fn city_to_id(city: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(city.as_bytes());
    hasher.finish()
}

fn main() {
    //let cpus = available_parallelism().unwrap().get();
    let cpus = 8;
    let start = Instant::now();
    let input = "small.txt";
    let size = File::open(input).unwrap().metadata().unwrap().len();
    let per_thread = size / cpus as u64;

    let mut index = 0;
    let mut threads = vec![];
    let (sender, receiver) = channel();
    for i in 0..cpus {
        let range = index..({index += per_thread; index.min(size)});
        threads.push(citymap_threaad(input.to_owned(), range, i, sender.clone()));
    }
    let mut ranges = (0..cpus).into_iter()
        .map(|_|receiver.recv().unwrap())
        .collect::<Vec<_>>();
    ranges.sort_unstable_by_key(|e|e.start);
    assert!(
        ranges.windows(2)
            .all(|e|{
                let first = &e[0];
                let second = &e[1];
                first.end == second.start
            }),
        "Ranges overlap or have gaps: {ranges:?}");
    let results = threads.into_iter()
        .map(|e|e.join().unwrap())
        //.map(|e|dbg!(e))
        .reduce(|mut left, right| {
            left.merge_with(right);
            left
        })
        .unwrap();

    print_results(results);

    println!("{:?}", start.elapsed());
}

fn citymap_threaad(path: String, mut range: Range<u64>, i: usize, range_feedback: Sender<Range<u64>>) -> JoinHandle<Citymap> {
    thread::Builder::new().name(format!("process_thread id: {i} assigned: {range:?}")).spawn(move ||{
        let mut file = File::open(path).unwrap();
        //println!("Before: {range:?}");

        // Perform alignment of buffer/range at the start
        {
            // Skip head alignment for start of file
            if range.start != 0 {
                let mut head = vec![0; 50];
                let len = file.read_at(&mut head, range.start).unwrap();
                head.truncate(len);

                for (i, &pos) in head.iter().enumerate()   {
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

                for (i, &pos) in head.iter().enumerate()   {
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

        let mut limited = BufReader::with_capacity(10^5, file);
        let mut buffered = limited.take(range.end - range.start);
        citymap_naive(&mut buffered)
    }).unwrap()
}

fn citymap_naive(input: &mut impl BufRead) -> Citymap {
    let mut map = Citymap::new();
    let mut line = String::new();
    loop {
        let read = input.read_line(&mut line).unwrap();
        if read == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut s = trimmed.split(";");
        let city = s.next().unwrap();
        let val = s.next().unwrap();
        let entry = map.lookup(city);
        entry.add_new_str(val.trim());
        line.clear();
    }
    map
}


fn print_results(map: Citymap) {
    let mut res = map.into_key_values();
        res.sort_unstable_by(|(a, _), (b, _)|a.cmp(b));
    print!("{{");
    for (city, vals) in res {
        let min = vals.min();
        let mean = vals.mean();
        let max = vals.max();
        print!("{city}={min:.1}/{mean:.1}/{max:.1}, ")
    }
    println!("}}");
}