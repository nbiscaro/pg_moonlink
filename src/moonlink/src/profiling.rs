// Possible location: src/profiling.rs or src/utils.rs

use std::time::Instant; // For pgrx::info! or pgrx::log!

pub struct ProfileGuard {
    tag: String,
    start_time: Instant,
    // You could add more fields here if you want to log specific context
    // on drop, e.g., number of items processed.
}

impl ProfileGuard {
    pub fn new(tag: &str) -> Self {
        let start_time = Instant::now();
        // Log the start. We rely on Postgres's log_line_prefix for the actual timestamp.
        println!("PROFILE_TIMING: [{}] Starting...", tag);
        ProfileGuard {
            tag: tag.to_string(),
            start_time,
        }
    }

    // A helper to add simple context to the end message
    pub fn new_with_prefix(prefix: &str, specific_tag: &str) -> Self {
        let full_tag = format!("{}_{}", prefix, specific_tag);
        Self::new(&full_tag)
    }
}

impl Drop for ProfileGuard {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed();
        println!(
            "PROFILE_TIMING: [{}] DURATION: {:.3} ms.",
            self.tag,
            duration.as_secs_f64() * 1000.0 // Convert to milliseconds
        );
    }
}

// Optional: A macro to make usage even more concise, especially if you
// don't need to hold onto the guard variable.
// This assumes ProfileGuard is in the same module or imported.
#[macro_export]
macro_rules! profile_scope {
    ($tag:expr) => {
        // The _guard variable name ensures it's not marked as unused,
        // and its lifetime is tied to the current scope.
        let _profile_guard = $crate::profiling::ProfileGuard::new($tag); // Adjust path if needed
    };
    ($prefix:expr, $specific_tag:expr) => {
        let _profile_guard =
            $crate::profiling::ProfileGuard::new_with_prefix($prefix, $specific_tag);
        // Adjust path if needed
    };
}
