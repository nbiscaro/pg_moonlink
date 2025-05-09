use core::str; // Not used in this file, but was in the input. Keeping it in case it's a snippet.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{ready, Future, Stream}; // ready is used
use pin_project_lite::pin_project;
use tokio::time::{sleep, Sleep}; // For pgrx::info!

// Assuming ProfileGuard is in src/profiling.rs
use super::{BatchBoundary, BatchConfig};
use crate::profiling::ProfileGuard;

// Implementation adapted from https://github.com/tokio-rs/tokio/blob/master/tokio-stream/src/stream_ext/chunks_timeout.rs
pin_project! {
    /// Adapter stream which batches the items of the underlying stream when it
    /// reaches max_size or when a timeout expires. The underlying streams items
    /// must implement [`BatchBoundary`]. A batch is guaranteed to end on an
    /// item which returns true from [`BatchBoundary::is_last_in_batch`]
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct BatchTimeoutStream<B: BatchBoundary, S: Stream<Item = B>> {
        #[pin]
        stream: S,
        #[pin]
        deadline: Option<Sleep>,
        items: Vec<S::Item>,
        batch_config: BatchConfig,
        reset_timer: bool,
        inner_stream_ended: bool,
    }
}

impl<B: BatchBoundary, S: Stream<Item = B>> BatchTimeoutStream<B, S> {
    pub fn new(stream: S, batch_config: BatchConfig) -> Self {
        // new() is cheap, no profiling needed here.
        BatchTimeoutStream {
            stream,
            deadline: None,
            items: Vec::with_capacity(batch_config.max_batch_size),
            batch_config,
            reset_timer: true,
            inner_stream_ended: false,
        }
    }

    pub fn get_inner_mut(&mut self) -> &mut S {
        // get_inner_mut() is cheap, no profiling needed.
        &mut self.stream
    }
}

impl<B: BatchBoundary, S: Stream<Item = B>> Stream for BatchTimeoutStream<B, S> {
    type Item = Vec<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let _guard = ProfileGuard::new("BATCH_TIMEOUT_STREAM_poll_next");
        let mut this = self.as_mut().project();

        if *this.inner_stream_ended {
            return Poll::Ready(None);
        }

        loop {
            if *this.reset_timer {
                this.deadline
                    .set(Some(sleep(this.batch_config.max_batch_fill_time)));
                *this.reset_timer = false;
            }

            // items.reserve_exact is an optimization, not typically a hot path itself unless max_batch_size is huge
            // and allocations are very frequent due to very small actual batches.
            // We'll rely on the overall poll_next timing for now.
            if this.items.is_empty() {
                this.items.reserve_exact(this.batch_config.max_batch_size);
            }

            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => break, // Inner stream is not ready, break loop and check timeout
                Poll::Ready(Some(item)) => {
                    let is_last_in_batch = item.is_last_in_batch();
                    this.items.push(item);

                    if this.items.len() >= this.batch_config.max_batch_size && is_last_in_batch {
                        *this.reset_timer = true;
                        let batch_size = this.items.len();
                        let batch = std::mem::take(this.items);
                        println!(
                            "PROFILE_TIMING: [BATCH_EMIT] Reason: SizeLimitAndBoundary. Size: {}. MaxSizeCfg: {}. TimeoutCfg: {:?}",
                            batch_size, this.batch_config.max_batch_size, this.batch_config.max_batch_fill_time
                        );
                        return Poll::Ready(Some(batch));
                    }
                    // If only size limit reached but not boundary, continue collecting.
                    // If only boundary reached but not size limit, continue collecting (timeout might apply).
                }
                Poll::Ready(None) => {
                    // Inner stream ended
                    let batch_to_return = if this.items.is_empty() {
                        None
                    } else {
                        *this.reset_timer = true; // Though not strictly needed as stream is ending
                        let batch_size = this.items.len();
                        let batch = std::mem::take(this.items);
                        println!(
                            "PROFILE_TIMING: [BATCH_EMIT] Reason: InnerStreamEnded. Size: {}. MaxSizeCfg: {}. TimeoutCfg: {:?}",
                            batch_size, this.batch_config.max_batch_size, this.batch_config.max_batch_fill_time
                        );
                        Some(batch)
                    };

                    *this.inner_stream_ended = true;
                    return Poll::Ready(batch_to_return);
                }
            }
        }

        // If we are here, it means the inner stream returned Poll::Pending.
        // Now check if there are items and if the timeout has occurred.
        if !this.items.is_empty() {
            if let Some(deadline) = this.deadline.as_pin_mut() {
                // ready! will return Poll::Pending from poll_next if the deadline.poll(cx) is Pending.
                // If deadline.poll(cx) is Ready, it means the timer has fired.
                ready!(deadline.poll(cx));

                // If we reach here, the deadline HAS fired.
                let last_item = this.items.last().expect(
                    "BatchTimeoutStream: items should not be empty if deadline was checked and fired",
                );
                if last_item.is_last_in_batch() {
                    *this.reset_timer = true;
                    let batch_size = this.items.len();
                    let batch = std::mem::take(this.items);
                    println!(
                        "PROFILE_TIMING: [BATCH_EMIT] Reason: TimeoutAndBoundary. Size: {}. MaxSizeCfg: {}. TimeoutCfg: {:?}",
                        batch_size, this.batch_config.max_batch_size, this.batch_config.max_batch_fill_time
                    );
                    return Poll::Ready(Some(batch));
                }
                // If deadline fired but last item is not a boundary, we don't emit yet.
                // We'll wait for a boundary item or for the batch to fill up or next timeout.
                // The timer will be reset at the start of the next poll_next call.
            }
        }

        Poll::Pending
    }
}
