use std::marker::PhantomData;
use std::mem::{transmute, transmute_copy, ManuallyDrop};
use std::{mem, ptr};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SendError, Sender};
use heapless::spsc::Consumer;
use crate::scheduler::{InnerScheduler, Scheduler};

#[derive(Debug)]
pub struct DataHandle{
    pub inner: Vec<u8>,

}
impl DataHandle {
    #[inline]
    pub fn new<T>(data: Vec<T>) -> Self {
        let mut data = ManuallyDrop::new(data);

        let ptr = data.as_mut_ptr() as *mut u8;
        let len = data.len() * std::mem::size_of::<T>();
        let cap = data.capacity() * std::mem::size_of::<T>();

        let bytes = unsafe {
            Vec::from_raw_parts(ptr, len, cap)
        };

        Self { inner: bytes }
    }
}

type OnceExec = Box<dyn FnOnce() -> Option<DataHandle> +'static + Send>;
pub(crate) type MulExec = Option<Box<dyn FnMut(Option<DataHandle>) -> Option<DataHandle> +'static + Send>>;

pub(crate) type Exec = Option<Box<dyn FnMut(Option<DataHandle>) -> Option<DataHandle> +'static + Send>>;
pub type ACCInner = AtomicPtr<Box<dyn FnMut(Option<DataHandle>) -> Option<DataHandle> +'static + Send>>;
pub struct Worker {
    idx: usize,
    result_ptr: Arc<AtomicUsize>,
    task: (Sender<Box<Option<DataHandle>>>, Receiver<(Exec, Option<Arc<DataHandle>>)>),
    pub parent: Arc<ACCInner>,
}



impl<'a> Worker {
    pub(crate) fn new(
        idx: usize,
        task: (Sender<Box<Option<DataHandle>>>, Receiver<(Exec, Option<Arc<DataHandle>>)>),
        parent: Arc<ACCInner>,
        result_ptr: Arc<AtomicUsize>) -> Worker {
        Self { idx, result_ptr, task, parent }
    }


    pub fn execute_once<T>(&'a mut self, exec: OnceExec) -> Box<Option<DataHandle>> {
        let res = exec();
        self.set_free();
        return Box::new(res);
    }

    pub fn execute<T>(&'a mut self) -> Result<(), SendError<Box<Option<DataHandle>>>> {
        loop {
            match self.task.1.try_recv(){
                Ok((exec, multi)) => {
                    if let Some(data) = multi{
                        let data = self.execute_mul(exec,
                                                    Scheduler::cast_vec::<T>(Arc::into_inner(data).unwrap())
                        );
                        let _ = self.task.0.send(data);
                        self.set_free();
                        continue;
                    }

                    let callable: Box<dyn FnOnce() -> Option<DataHandle> + Send> =
                        Box::new(move || exec.unwrap()(None));

                    let data  =self.execute_once::<T>(callable);
                    let _ = self.task.0.send(data);
                    self.set_free();
                    continue;
                }
                Err(_) => {
                    continue;
                }
            }
        }

    }


    pub fn execute_mul<T>(&'a mut self, exec: MulExec, data: Vec<T>) -> Box<Option<DataHandle>> {
        let data_handle = Some(DataHandle::new(data));
        if self.parent.load(Ordering::Acquire).is_null(){
            panic!("you need to assign a function first")
        }
        unsafe {
            if exec.is_some(){
                let called = exec.unwrap()(data_handle);
                return Box::new(called);
            }else {
                let func = unsafe { &mut *self.parent.load(Ordering::Acquire) };;
                let called = func(data_handle);
                return Box::new(called);

            }

        }
    }

    fn set_free(&mut self) {
        unsafe {&*self.result_ptr}.fetch_and(!(1 << self.idx), Ordering::Release) ;
    }
}
