use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::queue::SegQueue;
use tokio::sync::{Mutex, MutexGuard, Notify};
use tracing::debug;

pub trait PublicationStore {}

#[derive(Debug)]
pub struct MemoryStore<T> {
    items: SegQueue<(usize, T)>,
    offset: AtomicUsize,
    waiting: Notify,
    pending: Mutex<Pending<T>>,
}

impl<T> Default for MemoryStore<T> {
    fn default() -> Self {
        Self {
            items: Default::default(),
            offset: Default::default(),
            waiting: Default::default(),
            pending: Default::default(),
        }
    }
}

impl<T: Clone> MemoryStore<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, item: T) {
        let offset = self.offset.fetch_add(1, Ordering::Relaxed);
        debug!("push {}", offset);
        self.items.push((offset, item));

        self.waiting.notify_one();
    }

    pub async fn batch(&self, size: usize) -> Result<Batch<'_, T>, StoreError> {
        debug!("requesting a new batch");

        let mut batch = self.pending.lock().await;
        if let Pending::Items { items, start, end } = &*batch {
            debug!("requested existing batch [{}..={}]", start, end);
            return Ok(Batch::new(items.to_owned(), batch));
        }

        if self.items.is_empty() {
            debug!("waiting...");
            self.waiting.notified().await;
        }

        debug!("collecting a new batch");

        let mut pending_items = Vec::with_capacity(size);
        let mut start = None;
        let mut end = None;
        for _ in 0..size {
            if let Some((offset, item)) = self.items.pop() {
                pending_items.push(item);
                if start.is_none() {
                    start = Some(offset);
                }

                end = Some(offset);
            }
        }

        *batch = Pending::Items {
            items: pending_items.clone(),
            start: start.unwrap_or_default(),
            end: end.unwrap_or_default(),
        };

        debug!("loaded a new batch [{:?}..={:?}]", start, end,);
        let batch = Batch::new(pending_items, batch);
        Ok(batch)
    }

    pub fn commit(&self, mut batch: Batch<T>) {
        debug!("discarding batch");
        *batch.m = Pending::None;
    }
}

#[derive(Debug)]
enum Pending<T> {
    Items {
        start: usize,
        end: usize,
        items: Vec<T>,
    },
    None,
}

impl<T> Default for Pending<T> {
    fn default() -> Self {
        Pending::None
    }
}

#[derive(Debug)]
pub struct Batch<'a, T> {
    items: Vec<T>,
    m: MutexGuard<'a, Pending<T>>,
}

impl<'a, T> Batch<'a, T> {
    fn new(items: Vec<T>, m: MutexGuard<'a, Pending<T>>) -> Self {
        Self { items, m }
    }

    pub fn take_items(&mut self) -> Vec<T> {
        std::mem::take(&mut self.items)
    }
}

// impl<T> IntoIterator for Batch<'_, T> {
//     type Item = T;

//     type IntoIter = std::vec::IntoIter<T>;

//     fn into_iter(self) -> Self::IntoIter {
//         self.items.into_iter()
//     }
// }

// pub struct IntoIter<T> {
//     _a: PhantomData<T>,
// }

// impl<T> Iterator for IntoIter<T> {
//     type Item = T;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

#[derive(Debug)]
pub struct StoreError;

#[derive(Debug, Clone)]
pub struct Publication {
    pub id: usize,
}
