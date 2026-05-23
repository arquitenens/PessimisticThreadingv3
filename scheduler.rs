use std::any::TypeId;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::{transmute, ManuallyDrop};
use std::ptr;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::thread::JoinHandle;
use std::time::Duration;
use bitvec::order::Lsb0;
use bitvec::slice::BitSlice;
use heapless::spsc::{Consumer, Producer};
use crate::scheduler::SchedulerError::{NoValue, NoWorker, SenderError, WrongType};
use crate::worker::{ACCInner, DataHandle, Exec, MulExec, Worker};

pub struct Config{
    pub(crate) max_threads: usize,
    threads_per_worker: usize,
}
impl Config{

    pub fn new(max_threads: usize, threads_per_worker: usize) -> Self{
        assert!(max_threads > 0, "max_threads must be > 0");
        assert!(threads_per_worker > 0, "threads_per_worker must be > 0");
        assert!(threads_per_worker <= 64, "threads_per_worker must be < 64");
        Config{max_threads, threads_per_worker }
    }
}

impl Default for Config{
    fn default() -> Self{
        Config{max_threads: 8 ,threads_per_worker: 4}
    }
}


pub struct IncompleteScheduler{
    inner: Scheduler,
    idx: usize,
}
impl<'a> IncompleteScheduler {
    pub fn add_scheduler<T: 'static>(mut self) -> Self{
        let type_id = TypeId::of::<T>();

        self.idx += 1;

        //TODO is this masking properly?
        let current = self.inner.config.threads_per_worker * self.idx;
        let workers_to_mask = if current >= self.inner.config.max_threads {current - self.inner.config.max_threads} else { 0 };
        if workers_to_mask > self.inner.config.max_threads {
            panic!("you dont have enough sets in your config")
        }

        let result = if workers_to_mask > 0 {workers_to_mask} else {self.inner.config.threads_per_worker};
        self.inner.type_based_inner.insert(type_id, vec![InnerScheduler::new::<T>(result)]);
        return self;
    }
    pub fn apply(self) -> Scheduler {
        return self.inner;
    }
}

pub struct Scheduler {
    config: Config,
    type_based_inner: HashMap<TypeId, Vec<InnerScheduler>>,
}


#[derive(Debug)]
pub enum SchedulerError{
    NoWorker,
    WrongType(TypeId),
    NoValue,
    NotReady,
    SenderError,
}


pub enum Task<DataIn>{
    New(Box<dyn FnMut(Vec<DataIn>) -> Option<DataHandle> + Send>),
    Reuse
}
unsafe impl<DataIn> Send for Task<DataIn> {}

impl<'a> Scheduler {
    pub fn new(scheduler_config: Config) -> IncompleteScheduler {
        IncompleteScheduler{
            idx: 0,
            inner: Scheduler{
                config: scheduler_config,
                type_based_inner: HashMap::new()
            }
        }
    }
    fn get_available_single(
        workers_per: usize,
        of: &Vec<InnerScheduler>)
        -> *mut InnerScheduler{
        let mut wrap = 0;
        let len = of.len();

        unsafe {
            loop {
                if (*of[wrap % len].finished_workers).load(Ordering::Acquire) < usize::MAX {
                    return ptr::from_ref(&of[wrap % len]) as *mut InnerScheduler;
                }
                wrap += 1;
                std::thread::yield_now();
            }
        }
    }
    fn get_available_multi(
        workers_per: usize,
        of: &mut Vec<InnerScheduler>,
        min_amount: usize)
        -> &mut InnerScheduler {
        let mut wrap = 0;
        let len = of.len();
        unsafe {
            loop {
                if (*of[wrap % len].finished_workers).load(Ordering::Acquire) < (workers_per >> min_amount - 1) {
                    return &mut of[wrap % len];
                }
                wrap += 1;
                std::thread::yield_now();
            }
        }
    }
    pub fn any_once<DataIn: 'static + Send>(&mut self, mut exec: Box<dyn FnMut(Vec<DataIn>)
        -> Option<DataHandle> + Send>, data: Vec<DataIn>) -> Result<(), SchedulerError>{
        let inners = match self.type_based_inner.get(&TypeId::of::<DataIn>()){
            Some(inners) => inners,
            None => Err(WrongType(TypeId::of::<DataIn>()))?,
        };
        let available = Self::get_available_single(self.config.threads_per_worker, inners);

        let raw = unsafe {&mut *available};

        let total_available = unsafe {(*raw.finished_workers).load(Ordering::Acquire)};

        let all_busy_mask = (1usize << self.config.threads_per_worker) - 1;
        if total_available & all_busy_mask == all_busy_mask {
            return Err(NoWorker);
        }

        let mask = !total_available & (total_available + 1);
        let idx = mask.trailing_zeros() as usize;


        let callable: Box<dyn FnMut(Option<DataHandle>) -> Option<DataHandle> + Send> =

            Box::new(move |handle: Option<DataHandle>| {
                let vec = Scheduler::cast_vec::<DataIn>(handle.unwrap());
                exec(vec)
            }) as Box<dyn FnMut(Option<DataHandle>) -> Option<DataHandle> + Send>;


        //raw.handles[idx].thread().unpark();
        if let Ok(_) = raw.tasks[idx].send((Some(callable), Some(Arc::new(DataHandle::new(data))))) {
            (*raw.finished_workers).fetch_or(1 << idx, Ordering::Release);
            return Ok(());
        }
        return Err(SenderError);
        return Err(NoValue);

    }

    pub fn any_mul<DataIn: 'static + Send>(&mut self,
                                               exec: Task<DataIn>,
                                               data: Vec<DataIn>) -> Result<&mut InnerScheduler, SchedulerError>{
        let inners = match self.type_based_inner.get_mut(&TypeId::of::<DataIn>()){
            Some(inners) => inners,
            None => Err(WrongType(TypeId::of::<DataIn>()))?,
        };
        let available = Self::get_available_single(self.config.threads_per_worker, inners);

        let raw = unsafe {&mut *available};
        //println!("workers: {:?}", raw.finished_workers.load(Ordering::Acquire));

        raw.rec.1 += 1;

        let total_available = unsafe {(*raw.finished_workers).load(Ordering::Acquire)};


        let all_busy_mask = (1usize << self.config.threads_per_worker) - 1;
        if total_available & all_busy_mask == all_busy_mask {
            return Err(NoWorker);
        }


        //println!("mem: {:?}", raw.finished_workers);

        let mask = !total_available & (total_available + 1);
        let mut idx = mask.trailing_zeros() as usize;
        idx -= if idx == self.config.threads_per_worker {1} else {0};


        match exec{
            Task::New(mut func) => {

                let callable: Box<dyn FnMut(Option<DataHandle>) -> Option<DataHandle> + Send> =
                    Box::new(move |handle| {
                        let vec = Scheduler::cast_vec::<DataIn>(handle.unwrap());
                        func(vec)
                    });

                let raw_fn: *mut Box<dyn FnMut(Option<DataHandle>) -> Option<DataHandle> + Send> =
                    Box::into_raw(Box::new(callable));

                raw.reused_func.store(raw_fn, Ordering::Release);
                //println!("raw2: {:?}", raw.reused_func);
            }
            Task::Reuse => {

            }


        }
        unsafe {
            if let Ok(_) = raw.tasks[idx].send((None, Some(Arc::new(DataHandle::new(data))))) {
                (*raw.finished_workers).fetch_or(1 << idx, Ordering::Release);
                return Ok(raw);
            }
        }
        return Err(SenderError);

    }

    pub fn cast_vec<T>(vec: DataHandle) -> Vec<T>{

        assert!(std::mem::size_of::<T>() > 0);
        let mut inner = ManuallyDrop::new(vec.inner);


        let ptr = inner.as_mut_ptr() as *mut T;
        assert_eq!(ptr as usize % std::mem::align_of::<T>(), 0, "misaligned cast");
        let len = inner.len() / std::mem::size_of::<T>();
        let cap = inner.capacity() / std::mem::size_of::<T>();

        let bytes = unsafe {
            Vec::from_raw_parts(ptr, len, cap)
        };
        return bytes;
    }
}

pub struct InnerScheduler {
    finished_workers: Arc<AtomicUsize>,
    pub reused_func: Arc<ACCInner>,  // store the pointer, not the value
    rec: (Receiver<Box<Option<DataHandle>>>, usize),
    tasks: Vec<Sender<(Exec, Option<Arc<DataHandle>>)>>,
    handles: Vec<JoinHandle<()>>,
}

impl<'a> InnerScheduler {
    fn new<T>(workers: usize) -> Self {
        let mut handles = Vec::with_capacity(workers);
        let mut producers = Vec::with_capacity(workers);
        let result_ptr = Arc::new(AtomicUsize::new(0));

        let func_slot: Arc<ACCInner> = Arc::new(AtomicPtr::new(std::ptr::null_mut()));


        let (wts_tx, wts_rx) = std::sync::mpsc::channel::<Box<Option<DataHandle>>>();
        //worker threads to scheduler


        for i in 0..workers{
            let copy = wts_tx.clone();
            let cl = result_ptr.clone();
            let cl_ptr = func_slot.clone();
            let (stw_tx, stw_rx) = std::sync::mpsc::channel::<(Exec, Option<Arc<DataHandle>>)>();
            producers.push(stw_tx.clone());
            let h = std::thread::spawn(move || { let _ = Worker::new(i, (copy, stw_rx), cl_ptr, cl).execute::<T>();
                ()
            });
            handles.push(h);
        }

        let s = Self{
            tasks: producers,
            rec: (wts_rx, 0), //the usize is the active anticipated workers
            handles,
            finished_workers: result_ptr.clone(),
            reused_func: func_slot.clone(),
        };

        return s;

    }
    pub fn collect_results(&mut self) -> Vec<Box<Option<DataHandle>>>{
        let mut vec = Vec::with_capacity(self.tasks.len());
        for i in 0..self.rec.1 - 1{
             let rx = self.rec.0.recv().unwrap();
            vec.push(rx);
        }
        return vec;
    }
}
