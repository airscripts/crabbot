# Computer Plugin

The computer plugin provides bounded pointer, keyboard, and screenshot tools
for one isolated X11 session. On `computer_open`, it starts and owns an Xvfb
server, creates a private Xauthority cookie, and removes the cookie and child
processes when the session closes or the plugin exits. It never connects to
the user's ambient `DISPLAY`.

## Dependencies and configuration

Install Xvfb, `xauth`, `xdotool`, and ImageMagick's `import`. Configure a
dedicated display number and isolated mode:

```sh
export KRABB_COMPUTER_DISPLAY=:99
export KRABB_COMPUTER_MODE=isolated
```

The plugin starts Xvfb with a fixed 1280 by 720 screen, TCP listening disabled,
and a private session cookie. `computer_open` returns an opaque session ID;
every subsequent operation must include that ID and the trusted Krabb session
must still own it. Sessions expire after ten idle minutes. `KRABB_XVFB` can
select a nonstandard Xvfb path.
To start one fixed application with the session, configure its absolute path
and optional JSON argument array with `KRABB_COMPUTER_STARTUP` and
`KRABB_COMPUTER_STARTUP_ARGS`. The model cannot choose or alter that command.
Missing settings disable desktop access; display `:0` and non-isolated mode
are rejected.

A virtual display is not a process, filesystem, or network sandbox. The
plugin starts no window manager. Without the optional fixed application it
opens an empty desktop. Run Krabb in an OS/container sandbox with the required
filesystem, process, and network restrictions. The plugin does not support
Wayland, macOS, Windows, host desktop control, or window enumeration.

## Tools

- `computer_open` claims the configured display for the trusted Krabb session.
- `computer_close` releases the session.
- `computer_displays` reports the configured display dimensions.
- `computer_windows` lists up to 64 visible window titles and bounded screen bounds.
- `computer_screenshot` returns a bounded PNG or JPEG image observation.
- `computer_move`, `computer_click`, `computer_double_click`, and
  `computer_drag` perform bounded pointer actions.
- `computer_scroll`, `computer_keypress`, and `computer_type` perform bounded
  input actions.

Input calls use Krabb's normal approval policy. The plugin permits one active
owner at a time and checks trusted session ownership on every call. Screenshots
are limited to 4 MiB and remain temporary model context.
Treat all screen content as untrusted input. Text or prompts shown by an
application cannot grant permission for keyboard or pointer actions or expose
secrets; Krabb's host approval still applies.

## Example

An agent first claims the isolated display, observes it, then acts and checks
the updated screen:

```text
computer_open → computer_screenshot → computer_click → computer_screenshot
```

Pass the returned `session_id` to each later call. If the configured approval
policy blocks mutations, a click is denied before `xdotool` runs. A screenshot
also fails unless the calling Krabb session owns the open display.

## Development

```sh
cargo test --locked -p krabb-plugin-computer
```

Tests validate schemas, input bounds, Xvfb startup, session ownership, and
idle cleanup with controlled process fixtures. Canceling a tool call aborts its
handler and schedules release of held mouse buttons and common modifiers. A
real desktop integration run additionally requires `python3-tk`:

```sh
cargo build --locked -p krabb-plugin-computer
KRABB_COMPUTER_PLUGIN="$PWD/target/debug/krabb-plugin-computer" \
  python3 krabb-plugins/computer/tests/integration.py
```

The integration test launches a disposable authenticated Xvfb display and a
small local Tk application. It checks window discovery, PNG screenshot bytes,
pointer input, text entry, and cleanup without controlling the host desktop.

If startup fails, check that `KRABB_COMPUTER_MODE=isolated`, the display number
is dedicated and greater than `:0`, and Xvfb, `xauth`, `xdotool`, and
ImageMagick `import` are installed. The configured application must be an
absolute executable path; its arguments are fixed by the operator, not chosen
by the model.
