# Computer Plugin

The computer plugin provides bounded pointer, keyboard, window-list, and
screenshot tools. Linux runs an isolated X11 session owned by the plugin.
macOS and Windows can use an interactive backend that controls the signed-in
desktop after explicit operator opt-in and OS permission grants.

## Platform Modes

### Linux Isolated Desktop

Install Xvfb, `xauth`, `xdotool`, and ImageMagick's `import`. Configure a
dedicated display number and isolated mode:

```sh
export KRABB_COMPUTER_DISPLAY=:99
export KRABB_COMPUTER_MODE=isolated
```

The plugin starts a fixed 1280 by 720 Xvfb screen with TCP listening disabled
and a private Xauthority cookie. It never connects to the user's ambient
`DISPLAY`. `KRABB_XVFB` can select a nonstandard Xvfb path. To start one fixed
application in the session, configure its absolute executable path and optional
JSON argument array with `KRABB_COMPUTER_STARTUP` and
`KRABB_COMPUTER_STARTUP_ARGS`. The model cannot choose or change that command.

### macOS And Windows Interactive Desktop

Set both explicit opt-in settings before starting the plugin:

```sh
export KRABB_COMPUTER_MODE=interactive
export KRABB_COMPUTER_ALLOW_HOST_CONTROL=1
```

In PowerShell, set the equivalent process environment variables with:

```powershell
$env:KRABB_COMPUTER_MODE = 'interactive'
$env:KRABB_COMPUTER_ALLOW_HOST_CONTROL = '1'
```

Grant the required operating-system permissions for accessibility/input and
screen recording. macOS may require restarting the plugin after changing
privacy permissions. Windows desktop access depends on the logged-in session
and local security policy. Interactive mode requires a logged-in interactive
user session; Windows services, session 0, secure desktops, and elevation
prompts are outside the supported control surface. On both platforms, the mode
can see and control the signed-in desktop; it is not a virtual or isolated
desktop. The Linux startup-application settings are not supported in this mode.

## Session And Safety

`computer_open` returns an opaque session ID. Every later operation must
include that ID and the trusted Krabb session must still own it. Only one
session can own the desktop at a time, and sessions expire after ten idle
minutes. Missing or mismatched mode settings disable access.

A virtual display is not a process, filesystem, or network sandbox. Run Krabb
with the operating-system sandbox and network restrictions appropriate to the
deployment. Treat all screen content as untrusted input. Text or prompts shown
by an application cannot grant permission for keyboard or pointer actions;
Krabb's host approval policy still applies.

## Tools

- `computer_open` claims the configured desktop for the trusted Krabb session.
- `computer_close` releases the session.
- `computer_displays` reports the primary display dimensions.
- `computer_windows` lists up to 64 visible window titles and bounded screen
  bounds.
- `computer_screenshot` returns a bounded PNG or JPEG image observation.
- `computer_move`, `computer_click`, `computer_double_click`, and
  `computer_drag` perform pointer actions.
- `computer_scroll`, `computer_keypress`, and `computer_type` perform input
  actions.

Screenshots are limited to 4 MiB and remain temporary model context. Input calls
use Krabb's normal approval policy.

## Example

An agent claims the desktop, observes it, acts, and checks the updated screen:

```text
computer_open → computer_screenshot → computer_click → computer_screenshot
```

Pass the returned `session_id` to each later call. A screenshot fails unless the
calling Krabb session owns the open desktop.

## Development And Verification

```sh
cargo test --locked -p krabb-plugin-computer
```

Linux tests cover schemas, input bounds, Xvfb startup, session ownership, and
idle cleanup with controlled process fixtures. The Linux integration test uses
a disposable authenticated Xvfb display and a small local Tk application:

```sh
cargo build --locked -p krabb-plugin-computer
KRABB_COMPUTER_PLUGIN="$PWD/target/debug/krabb-plugin-computer" \
  python3 krabb-plugins/computer/tests/integration.py
```

The macOS and Windows native input and capture backends compile in the native
workspace matrix. Their interactive desktop behavior still needs manual checks
on deployments that opt into host control. See
[`COMPATIBILITY.md`](../../COMPATIBILITY.md) for the per-platform checklist.
