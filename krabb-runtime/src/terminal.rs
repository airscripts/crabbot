use std::{
    io::{self, IsTerminal, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

type SharedWriter = Arc<Mutex<Box<dyn Write + Send>>>;

pub(crate) struct Spinner {
    running: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
    writer: SharedWriter,
}

impl Spinner {
    pub(crate) fn start(message: impl Into<String>) -> Option<Self> {
        if !io::stderr().is_terminal() {
            return None;
        }

        Some(Self::spawn(message.into(), Box::new(io::stderr())))
    }

    fn spawn(message: String, writer: Box<dyn Write + Send>) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let worker_running = Arc::clone(&running);
        let writer = Arc::new(Mutex::new(writer));
        let worker_writer = Arc::clone(&writer);
        let worker = thread::spawn(move || {
            const FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
            let mut frame = 0;

            while worker_running.load(Ordering::Relaxed) {
                if let Ok(mut writer) = worker_writer.lock() {
                    let _ = write!(writer, "\r{} {message}", FRAMES[frame]);
                    let _ = writer.flush();
                }

                frame = (frame + 1) % FRAMES.len();
                thread::sleep(Duration::from_millis(90));
            }
        });

        Self { running, worker: Some(worker), writer }
    }

    fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);

        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }

        if let Ok(mut writer) = self.writer.lock() {
            let _ = write!(writer, "\r\x1b[2K");
            let _ = writer.flush();
        }
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use super::Spinner;

    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);

            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn spinner_animates_in_place_and_clears_when_dropped() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let spinner =
            Spinner::spawn("Installing plugins...".into(), Box::new(Buffer(Arc::clone(&output))));

        thread::sleep(Duration::from_millis(10));
        drop(spinner);

        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();

        assert!(output.contains("⠋ Installing plugins..."));
        assert!(output.ends_with("\r\x1b[2K"));
        assert!(!output.contains('\n'));
    }
}
