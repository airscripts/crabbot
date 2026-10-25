use std::io;

#[cfg(windows)]
pub struct TerminalJob(Option<usize>);

#[cfg(windows)]
pub const CREATE_SUSPENDED: u32 = windows_sys::Win32::System::Threading::CREATE_SUSPENDED;

#[cfg(windows)]
impl TerminalJob {
    pub fn new() -> io::Result<Self> {
        use windows_sys::Win32::System::JobObjects::{
            CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };

        // SAFETY: Null security attributes and a null name request a new unnamed job object.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };

        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: The information structure is plain C data and is zero-initialized here.
        let mut limits = unsafe { std::mem::zeroed::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() };

        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

        // SAFETY: `limits` is initialized, and its pointer remains valid for this call.
        let configured = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };

        if configured == 0 {
            // SAFETY: `handle` was returned by CreateJobObjectW and is still owned here.
            unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };

            return Err(io::Error::last_os_error());
        }

        Ok(Self(Some(handle as usize)))
    }

    pub fn assign_and_resume(&self, process_id: u32) -> io::Result<()> {
        use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::System::{
            Diagnostics::ToolHelp::{
                CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First,
                Thread32Next,
            },
            JobObjects::AssignProcessToJobObject,
            Threading::{
                OpenProcess, OpenThread, PROCESS_SET_QUOTA, PROCESS_TERMINATE, ResumeThread,
                THREAD_SUSPEND_RESUME,
            },
        };

        let process =
            // SAFETY: OpenProcess receives access flags and a process ID, with inheritance disabled.

            unsafe { OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, 0, process_id) };

        if process.is_null() {
            return Err(io::Error::last_os_error());
        }

        let Some(job) = self.0 else {
            // SAFETY: `process` is an owned handle returned by OpenProcess.
            unsafe { CloseHandle(process) };

            return Err(io::Error::other("The shell process job is closed."));
        };

        // SAFETY: Both handles are live, and `process` has the rights required for assignment.
        let assigned = unsafe { AssignProcessToJobObject(job as _, process) };

        let assignment_error = (assigned == 0).then(io::Error::last_os_error);

        // SAFETY: `process` is an owned handle returned by OpenProcess.
        unsafe { CloseHandle(process) };

        if let Some(error) = assignment_error {
            return Err(error);
        }

        // SAFETY: Snapshotting system thread entries returns an owned snapshot handle.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };

        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: THREADENTRY32 is a C structure initialized before Thread32First.
        let mut entry = unsafe { std::mem::zeroed::<THREADENTRY32>() };

        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;

        // SAFETY: `snapshot` is live and `entry` is writable for the duration of the call.
        let mut has_entry = unsafe { Thread32First(snapshot, &mut entry) };

        let mut thread_id = None;

        while has_entry != 0 && thread_id.is_none() {
            thread_id = (entry.th32OwnerProcessID == process_id).then_some(entry.th32ThreadID);

            // SAFETY: `snapshot` and `entry` remain valid while enumerating this snapshot.
            let next_entry = unsafe { Thread32Next(snapshot, &mut entry) };

            has_entry = next_entry;
        }

        // SAFETY: `snapshot` is an owned handle returned by CreateToolhelp32Snapshot.
        unsafe { CloseHandle(snapshot) };

        let thread_id = thread_id.ok_or_else(|| {
            io::Error::other("The suspended shell process has no discoverable thread.")
        })?;

        // SAFETY: OpenThread receives a thread ID and requests only the resume right.
        let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, thread_id) };

        if thread.is_null() {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: `thread` is live and has THREAD_SUSPEND_RESUME access.
        let resumed = unsafe { ResumeThread(thread) };

        let resume_error = (resumed == u32::MAX).then(io::Error::last_os_error);

        // SAFETY: `thread` is an owned handle returned by OpenThread.
        unsafe { CloseHandle(thread) };

        resume_error.map_or(Ok(()), Err)
    }

    pub fn terminate(&self) -> bool {
        self.0.is_some_and(|job| {
            // SAFETY: The job handle is owned by this value and remains open during the call.
            unsafe { windows_sys::Win32::System::JobObjects::TerminateJobObject(job as _, 1) != 0 }
        })
    }
}

#[cfg(windows)]
impl Drop for TerminalJob {
    fn drop(&mut self) {
        let _ = self.0.take().map(close_job_handle);
    }
}

#[cfg(windows)]
fn close_job_handle(handle: usize) -> i32 {
    // SAFETY: This value owns the handle returned by CreateJobObjectW.
    let result = unsafe { windows_sys::Win32::Foundation::CloseHandle(handle as _) };

    result
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::{CREATE_SUSPENDED, TerminalJob};

    use std::{os::windows::process::CommandExt, process::Command};

    #[test]
    fn terminates_a_suspended_process_after_job_assignment() {
        let job = TerminalJob::new().expect("create process job");
        let mut command = Command::new("cmd.exe");
        command.args(["/C", "ping -n 60 127.0.0.1 > nul"]);
        command.creation_flags(CREATE_SUSPENDED);

        let mut child = command.spawn().expect("spawn suspended command");
        let process_id = child.id();

        job.assign_and_resume(process_id).expect("assign and resume command");

        let terminated = job.terminate();

        if !terminated {
            let _ = child.kill();
        }

        let status = child.wait().expect("wait for terminated command");

        assert!(terminated, "job object should terminate its assigned command");
        assert!(!status.success(), "terminated command should not exit successfully");
    }
}
