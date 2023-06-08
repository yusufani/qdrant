use std::os::fd::AsRawFd as _;
use std::{fs, io};

use io_uring::{opcode, types, CompletionQueue, SubmissionQueue, Submitter};

use crate::common::mmap_ops::transmute_from_u8_to_slice;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::PointOffsetType;

const DISK_PARALLELISM: usize = 16; // TODO: benchmark it better, or make it configurable

pub struct UringReader {
    file: fs::File,
    io_uring: io_uring::IoUring,
    buffers: Vec<Buffer>,
    header_size_bytes: usize,
    vector_size_bytes: usize,
}

#[derive(Clone, Debug)]
struct Buffer {
    buffer: Vec<u8>,
    point: Option<Point>,
}

impl Buffer {
    pub fn new(len: usize) -> Self {
        Self {
            buffer: vec![0; len],
            point: None,
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct Point {
    index: usize,
    point: PointOffsetType,
}

impl Point {
    pub fn new(index: usize, point: PointOffsetType) -> Self {
        Self { index, point }
    }
}

impl UringReader {
    pub fn new(
        file: fs::File,
        header_size_bytes: usize,
        vector_size_bytes: usize,
    ) -> OperationResult<Self> {
        let io_uring = io_uring::IoUring::new(DISK_PARALLELISM as _)?;
        let buffers = Vec::new();

        let reader = Self {
            file,
            io_uring,
            buffers,
            header_size_bytes,
            vector_size_bytes,
        };

        Ok(reader)
    }

    /// Takes in iterator of point offsets, reads it, and yields a callback with the read data.
    pub fn read_stream(
        &mut self,
        points: impl IntoIterator<Item = PointOffsetType>,
        mut callback: impl FnMut(usize, PointOffsetType, &[VectorElementType]),
    ) -> OperationResult<()> {
        let (submitter, mut sq, mut cq) = self.io_uring.split();
        let mut points = points.into_iter().enumerate();

        let mut submitted = 0;

        // Push and submit up to either `DISK_PARALLELISM` or CQ capacity (whichever is smaller) SQEs.
        //
        // - CQ capacty is *twice* the SQ capacity
        // - and each iteration may submit up to SQ capacity SQEs
        // - so this loop should spin at most *two* iterations
        while submitted < DISK_PARALLELISM.min(cq.capacity()) {
            // Push SQEs into SQ up to SQ capacity.
            while !sq.is_full() {
                // Check if there are more SQEs...
                let Some((point_index, point)) = points.next() else {
                    // No more SQEs to push.
                    break;
                };

                self.buffers.push(Buffer::new(self.vector_size_bytes));

                let buffer_index = self.buffers.len() - 1;
                let buffer = &mut self.buffers[buffer_index];

                // TODO: Deduplicate with similar snippet in CQ loop?
                let point_offset_bytes =
                    self.header_size_bytes + self.vector_size_bytes * point as usize;

                let sqe = opcode::Read::new(
                    types::Fd(self.file.as_raw_fd()),
                    buffer.buffer.as_mut_ptr(),
                    buffer.buffer.len() as _,
                )
                .offset(point_offset_bytes as _)
                .build()
                .user_data(buffer_index as _);

                buffer.point = Some(Point::new(point_index, point));

                unsafe { sq.push(&sqe).expect("SQ is not full") };
            }

            if sq.is_empty() {
                // SQ is empty, no SQEs to submit.
                break;
            }

            // Track submitted SQEs.
            submitted += submit_sqes(&submitter, &mut sq)?;
        }

        // Wait for at least one CQE.
        probe_or_wait_for_cqe(&submitter, &mut cq)?;

        // TODO
        while submitted > 0 {
            // TODO
            for cqe in &mut cq {
                // Track consumed CQE.
                submitted -= 1;

                // Process consumed CQE.
                let buffer_index = cqe.user_data() as usize;
                let buffer = &mut self.buffers[buffer_index];

                let point = buffer
                    .point
                    .take()
                    .expect("point data is associated with the buffer");
                let vector = transmute_from_u8_to_slice(&buffer.buffer);

                callback(point.index, point.point, &vector);

                // Check if there are more SQEs to push and submit.
                let Some((point_index, point)) = points.next() else {
                    if sq.is_empty() {
                        // All points exhausted and SQ is empty.
                        // Continue processing CQEs in the inner loop until CQ is empty.
                        continue;
                    } else {
                        // All points exhausted and SQ is not empty.
                        // Break out of the inner loop to submit *the last* SQEs.
                        break;
                    }
                };

                // TODO: Deduplicate with similar snippet in SQ loop?
                let point_offset_bytes =
                    self.header_size_bytes + self.vector_size_bytes * point as usize;

                let sqe = io_uring::opcode::Read::new(
                    types::Fd(self.file.as_raw_fd()),
                    buffer.buffer.as_mut_ptr(),
                    buffer.buffer.len() as _,
                )
                .offset(point_offset_bytes as _)
                .build()
                .user_data(buffer_index as _);

                buffer.point = Some(Point::new(point_index, point));

                unsafe { sq.push(&sqe).expect("SQ is not full") };

                if sq.is_full() {
                    // SQ is full.
                    // Break out of the inner loop to submit *more* SQEs.
                    break;
                }
            }

            if sq.is_empty() {
                probe_or_wait_for_cqe(&submitter, &mut cq)?;
            } else {
                submitted += submit_sqes_and_wait_for_cqe(&submitter, &mut sq, &mut cq)?;
            }
        }

        Ok(())
    }
}

fn check_cq_is_empty(cq: &mut io_uring::CompletionQueue<'_>) -> bool {
    cq.sync();
    cq.is_empty()
}

fn probe_cq_is_empty(cq: &mut io_uring::CompletionQueue<'_>) -> bool {
    for _ in 0..3 {
        if !check_cq_is_empty(cq) {
            return false;
        }
    }

    true
}

fn probe_or_wait_for_cqe(
    submitter: &io_uring::Submitter<'_>,
    mut cq: &mut io_uring::CompletionQueue,
) -> io::Result<()> {
    if probe_cq_is_empty(&mut cq) {
        wait_for_cqe(submitter, cq)?;
    }

    Ok(())
}

fn wait_for_cqe(
    submitter: &io_uring::Submitter<'_>,
    cq: &mut io_uring::CompletionQueue<'_>,
) -> io::Result<()> {
    // Submit pushed SQEs (if any) and wait for CQE.
    let submitted = submit_and_wait(submitter, cq)?;

    // Assert that no SQEs have been *unexpectedly* submitted while waiting for CQE.
    debug_assert_eq!(
        submitted, 0,
        "Some SQEs have been *unexpectedly* submitted while waiting for CQEs!",
    );

    Ok(())
}

fn submit_sqes(
    submitter: &io_uring::Submitter<'_>,
    sq: &mut io_uring::SubmissionQueue<'_>,
) -> io::Result<usize> {
    // Sync SQ (so that kernel will see pushed SQEs).
    sq.sync();

    // Submit pushed SQEs.
    let submitted = submitter.submit()?;

    assert_submitted_sqes(sq, submitted);
    Ok(submitted)
}

fn submit_sqes_and_wait_for_cqe(
    submitter: &io_uring::Submitter<'_>,
    sq: &mut io_uring::SubmissionQueue<'_>,
    mut cq: &mut io_uring::CompletionQueue<'_>,
) -> io::Result<usize> {
    // Sync SQ (so that kernel will see pushed SQEs).
    sq.sync();

    // Check if CQ is empty.
    let submitted = if check_cq_is_empty(&mut cq) {
        // If CQ is empty, then submit pushed SQEs and wait for CQEs.
        //
        // We *have to* submit at this point, and "waiting" is the same syscall,
        // so we get a "free" wait here.
        submit_and_wait(&submitter, cq)?
    } else {
        // If CQ is not empty, then submit pushed SQEs without waiting.
        submitter.submit()?
    };

    assert_submitted_sqes(sq, submitted);
    Ok(submitted)
}

fn submit_and_wait(
    submitter: &io_uring::Submitter<'_>,
    cq: &mut io_uring::CompletionQueue<'_>,
) -> io::Result<usize> {
    // Submit pushed SQEs (if any) and wait for CQE.
    let submitted = submitter.submit_and_wait(1)?;

    // Sync CQ (so that we will see CQEs pushed by the kernel).
    cq.sync();

    // Assert that CQ is not empty after `submit_and_wait`.
    debug_assert!(!cq.is_empty(), "CQ is empty after `submit_and_wait`!");

    Ok(submitted)
}

fn assert_submitted_sqes(sq: &mut io_uring::SubmissionQueue<'_>, submitted: usize) {
    // Assert that all (and no more than expected) SQEs have been submitted.
    //
    // Kernel should consume SQEs from SQ during submit, but `sq` state is not updated
    // until `sync` call, so `sq` should still hold pre-`submit` state at this point.
    debug_assert_eq!(
        submitted,
        sq.len(),
        "Not all (or more than expected) SQEs have been submitted!",
    );

    // Sync SQ (so that we will see SQEs consumed by the kernel).
    sq.sync();

    // Assert that all SQEs have been consumed during submit and SQ is empty.
    debug_assert!(
        sq.is_empty(),
        "Not all SQEs have been consumed during submit! SQ is not empty!",
    );
}
