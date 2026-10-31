use std::{io, sync::Mutex, sync::OnceLock};

use windows_sys::Win32::System::Services::*;

static MAIN: OnceLock<fn()> = OnceLock::new();
static SERVICE: OnceLock<Service> = OnceLock::new();

pub struct Service {
    handle: usize,
    status: Mutex<SERVICE_STATUS>,
    stop: Box<dyn Fn() + Send + Sync>,
}

pub fn dispatch(name: &str, main: fn()) -> io::Result<()> {
    MAIN.set(main).map_err(|_| io::Error::other("Service dispatcher already started."))?;
    let mut name = wide(name)?;
    let table = [
        SERVICE_TABLE_ENTRYW { lpServiceName: name.as_mut_ptr(), lpServiceProc: Some(entry) },
        SERVICE_TABLE_ENTRYW::default(),
    ];

    // SAFETY: The terminated table and service name remain alive until dispatch returns.
    if unsafe { StartServiceCtrlDispatcherW(table.as_ptr()) } == 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

unsafe extern "system" fn entry(_: u32, _: *mut *mut u16) {
    if let Some(main) = MAIN.get() {
        main();
    }
}

impl Service {
    pub fn register(
        name: &str,
        stop: impl Fn() + Send + Sync + 'static,
    ) -> io::Result<&'static Self> {
        let name = wide(name)?;

        // SAFETY: The name is terminated and the callback uses only process-lifetime state.
        let handle = unsafe {
            RegisterServiceCtrlHandlerExW(name.as_ptr(), Some(control), std::ptr::null())
        };

        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }

        SERVICE
            .set(Self {
                handle: handle as usize,
                status: Mutex::new(status(SERVICE_START_PENDING, 0)),
                stop: Box::new(stop),
            })
            .map_err(|_| io::Error::other("Service handler already registered."))?;
        let service = SERVICE.get().ok_or_else(|| io::Error::other("Service is unavailable."))?;
        service.report(None, 0)?;

        Ok(service)
    }

    pub fn running(&self) -> io::Result<()> {
        self.report(Some(SERVICE_RUNNING), 0)
    }

    pub fn stopped(&self, failed: bool) -> io::Result<()> {
        self.report(Some(SERVICE_STOPPED), u32::from(failed))
    }

    pub fn heartbeat(&self) -> io::Result<()> {
        self.report(None, 0)
    }

    fn report(&self, next: Option<u32>, exit: u32) -> io::Result<()> {
        let mut current =
            self.status.lock().map_err(|_| io::Error::other("Service lock failed."))?;

        if current.dwCurrentState == SERVICE_STOPPED
            || (current.dwCurrentState == SERVICE_STOP_PENDING && next == Some(SERVICE_RUNNING))
        {
            return Ok(());
        }

        if let Some(next) = next {
            *current = status(next, exit);
        } else if matches!(current.dwCurrentState, SERVICE_START_PENDING | SERVICE_STOP_PENDING) {
            current.dwCheckPoint = current.dwCheckPoint.saturating_add(1);
        }

        // SAFETY: SCM issued this status handle; the initialized status is locked for this call.
        if unsafe { SetServiceStatus(self.handle as _, &*current) } == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }
}

unsafe extern "system" fn control(
    event: u32,
    _: u32,
    _: *mut std::ffi::c_void,
    _: *mut std::ffi::c_void,
) -> u32 {
    let Some(service) = SERVICE.get() else {
        return 0;
    };

    match event {
        SERVICE_CONTROL_STOP | SERVICE_CONTROL_SHUTDOWN => {
            let _ = service.report(Some(SERVICE_STOP_PENDING), 0);
            (service.stop)();
            0
        }

        SERVICE_CONTROL_INTERROGATE => 0,
        _ => windows_sys::Win32::Foundation::ERROR_CALL_NOT_IMPLEMENTED,
    }
}

fn status(state: u32, exit: u32) -> SERVICE_STATUS {
    let pending = matches!(state, SERVICE_START_PENDING | SERVICE_STOP_PENDING);

    SERVICE_STATUS {
        dwServiceType: SERVICE_WIN32_OWN_PROCESS,
        dwCurrentState: state,
        dwControlsAccepted: if state == SERVICE_RUNNING {
            SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN
        } else {
            0
        },
        dwWin32ExitCode: exit,
        dwServiceSpecificExitCode: 0,
        dwCheckPoint: u32::from(pending),
        dwWaitHint: if pending { 10_000 } else { 0 },
    }
}

fn wide(name: &str) -> io::Result<Vec<u16>> {
    if name.contains('\0') {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Service name contains NUL."));
    }

    Ok(name.encode_utf16().chain(Some(0)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_status_accepts_controls_only_while_running() {
        for state in [SERVICE_START_PENDING, SERVICE_STOP_PENDING] {
            let status = status(state, 0);

            assert_eq!(status.dwControlsAccepted, 0);
            assert_eq!(status.dwCheckPoint, 1);
            assert_eq!(status.dwWaitHint, 10_000);
        }

        let running = status(SERVICE_RUNNING, 0);

        assert_eq!(running.dwControlsAccepted, SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN);
        assert_eq!(running.dwCheckPoint, 0);
        assert_eq!(running.dwWaitHint, 0);

        let stopped = status(SERVICE_STOPPED, 1);

        assert_eq!(stopped.dwControlsAccepted, 0);
        assert_eq!(stopped.dwWin32ExitCode, 1);
        assert_eq!(wide("Krabb").unwrap(), vec![75, 114, 97, 98, 98, 0]);
        assert!(wide("bad\0name").is_err());
    }
}
