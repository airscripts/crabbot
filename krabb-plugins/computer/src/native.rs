use std::io::Cursor;

use base64::Engine;
use enigo::{Axis, Button, Coordinate, Direction, Enigo, Key, Keyboard, Mouse, Settings};
use image::{DynamicImage, ImageFormat};
use serde_json::{Value, json};
use xcap::{Monitor, Window};

use crate::{IMAGE_LIMIT, denied};

fn input() -> krabb_core::Result<Enigo> {
    Enigo::new(&Settings::default()).map_err(|error| {
        denied(format!(
            "Could not access desktop input. Grant the required OS accessibility permission: {error}"
        ))
    })
}

fn primary_monitor() -> krabb_core::Result<Monitor> {
    let monitors =
        Monitor::all().map_err(|error| denied(format!("Could not enumerate displays: {error}")))?;

    monitors
        .iter()
        .find(|monitor| monitor.is_primary().unwrap_or(false))
        .cloned()
        .or_else(|| monitors.into_iter().next())
        .ok_or_else(|| denied("No active display is available."))
}

pub(super) fn display_info() -> krabb_core::Result<Value> {
    let monitor = primary_monitor()?;

    let width = monitor
        .width()
        .map_err(|error| denied(format!("Could not read display width: {error}")))?;

    let height = monitor
        .height()
        .map_err(|error| denied(format!("Could not read display height: {error}")))?;

    Ok(json!({"display":"interactive","width":width,"height":height}))
}

pub(super) fn list_windows() -> krabb_core::Result<Value> {
    let all =
        Window::all().map_err(|error| denied(format!("Could not enumerate windows: {error}")))?;
    let windows = all
        .into_iter()
        .filter_map(|window| {
            let title = window.title().ok()?;

            if title.trim().is_empty() || window.is_minimized().unwrap_or(true) {
                return None;
            }

            let x = window.x().ok()?;
            let y = window.y().ok()?;
            let width = window.width().ok()?;
            let height = window.height().ok()?;

            if !(-4096..=4096).contains(&x)
                || !(-4096..=4096).contains(&y)
                || !(1..=4096).contains(&width)
                || !(1..=4096).contains(&height)
            {
                return None;
            }

            Some(json!({
                "title": title.trim().chars().take(512).collect::<String>(),
                "x": x,
                "y": y,
                "width": width,
                "height": height
            }))
        })
        .take(65)
        .collect::<Vec<_>>();
    let truncated = windows.len() > 64;

    Ok(json!({"windows":windows.into_iter().take(64).collect::<Vec<_>>(),"truncated":truncated}))
}

pub(super) fn capture_screen(format: &str) -> krabb_core::Result<Value> {
    let (image_format, mime) = match format {
        "png" => (ImageFormat::Png, "image/png"),
        "jpeg" => (ImageFormat::Jpeg, "image/jpeg"),
        _ => return Err(denied("format must be png or jpeg.")),
    };

    let monitor = primary_monitor()?;
    let width = monitor
        .width()
        .map_err(|error| denied(format!("Could not read display width: {error}")))?;
    let height = monitor
        .height()
        .map_err(|error| denied(format!("Could not read display height: {error}")))?;
    let image = monitor.capture_image().map_err(|error| {
        denied(format!("Screen capture failed. Check OS screen-recording permissions: {error}"))
    })?;
    let mut output = Cursor::new(Vec::new());

    let encoded = match image_format {
        ImageFormat::Jpeg => {
            DynamicImage::ImageRgba8(image).to_rgb8().write_to(&mut output, image_format)
        }

        ImageFormat::Png => {
            DynamicImage::ImageRgba8(image).to_rgba8().write_to(&mut output, image_format)
        }

        _ => unreachable!("image format was validated above"),
    };

    encoded.map_err(|error| denied(format!("Screen image encoding failed: {error}")))?;
    let bytes = output.into_inner();

    if bytes.len() > IMAGE_LIMIT {
        return Err(denied("Screen capture exceeded the 4 MiB limit."));
    }

    Ok(json!({
        "content": [
            {"kind":"text","text":"Interactive desktop screenshot captured."},
            {"kind":"image","uri":format!("data:{mime};base64,{}", base64::engine::general_purpose::STANDARD.encode(bytes)),"alt":"Interactive desktop screenshot"}
        ],
        "metadata": {
            "width": width,
            "height": height,
            "format": mime,
            "coordinate_space": "display",
            "scale": 1
        }
    }))
}

pub(super) fn move_pointer(x: i64, y: i64) -> krabb_core::Result<()> {
    input()?
        .move_mouse(x as i32, y as i32, Coordinate::Abs)
        .map_err(|error| denied(format!("Could not move the pointer: {error}")))
}

pub(super) fn click_pointer(x: i64, y: i64, button: &str, double: bool) -> krabb_core::Result<()> {
    let button = mouse_button(button)?;
    let mut enigo = input()?;
    enigo
        .move_mouse(x as i32, y as i32, Coordinate::Abs)
        .map_err(|error| denied(format!("Could not move the pointer: {error}")))?;

    let count = if double { 2 } else { 1 };

    for _ in 0..count {
        enigo
            .button(button, Direction::Click)
            .map_err(|error| denied(format!("Could not click the pointer: {error}")))?;
    }

    Ok(())
}

pub(super) fn drag_pointer(
    x: i64,
    y: i64,
    to_x: i64,
    to_y: i64,
    button: &str,
) -> krabb_core::Result<()> {
    let button = mouse_button(button)?;
    let mut enigo = input()?;
    enigo
        .move_mouse(x as i32, y as i32, Coordinate::Abs)
        .map_err(|error| denied(format!("Could not move the pointer: {error}")))?;
    enigo
        .button(button, Direction::Press)
        .map_err(|error| denied(format!("Could not press the mouse button: {error}")))?;
    let moved = enigo.move_mouse(to_x as i32, to_y as i32, Coordinate::Abs);
    let released = enigo.button(button, Direction::Release);

    moved.map_err(|error| denied(format!("Could not complete the drag: {error}")))?;
    released.map_err(|error| denied(format!("Could not release the mouse button: {error}")))
}

pub(super) fn scroll_pointer(x: i64, y: i64, amount: i64) -> krabb_core::Result<()> {
    let mut enigo = input()?;
    enigo
        .move_mouse(x as i32, y as i32, Coordinate::Abs)
        .map_err(|error| denied(format!("Could not move the pointer: {error}")))?;
    enigo
        .scroll(-(amount as i32), Axis::Vertical)
        .map_err(|error| denied(format!("Could not scroll the display: {error}")))
}

pub(super) fn press_key(value: &str) -> krabb_core::Result<()> {
    let mut parts = value.split('+').collect::<Vec<_>>();
    let key = parts.pop().ok_or_else(|| denied("key is empty."))?;
    let key = parse_key(key)?;
    let modifiers = parts.into_iter().map(parse_key).collect::<krabb_core::Result<Vec<_>>>()?;
    let mut enigo = input()?;

    let mut pressed_modifiers = Vec::new();

    for modifier in modifiers {
        if let Err(error) = enigo.key(modifier, Direction::Press) {
            for pressed_modifier in pressed_modifiers.into_iter().rev() {
                let _ = enigo.key(pressed_modifier, Direction::Release);
            }

            return Err(denied(format!("Could not press a keyboard modifier: {error}")));
        }

        pressed_modifiers.push(modifier);
    }

    let pressed = enigo.key(key, Direction::Click);

    for modifier in pressed_modifiers.into_iter().rev() {
        let _ = enigo.key(modifier, Direction::Release);
    }

    pressed.map_err(|error| denied(format!("Could not press the key: {error}")))
}

pub(super) fn type_text(text: &str) -> krabb_core::Result<()> {
    input()?.text(text).map_err(|error| denied(format!("Could not type text: {error}")))
}

fn mouse_button(value: &str) -> krabb_core::Result<Button> {
    match value {
        "left" => Ok(Button::Left),
        "middle" => Ok(Button::Middle),
        "right" => Ok(Button::Right),
        _ => Err(denied("button must be left, middle, or right.")),
    }
}

fn parse_key(value: &str) -> krabb_core::Result<Key> {
    let normalized = value.to_ascii_uppercase();
    let key = match normalized.as_str() {
        "CTRL" | "CONTROL" => Key::Control,
        "ALT" | "OPTION" => Key::Alt,
        "SHIFT" => Key::Shift,
        "META" | "CMD" | "COMMAND" | "SUPER" | "WINDOWS" => Key::Meta,
        "ENTER" | "RETURN" => Key::Return,
        "ESC" | "ESCAPE" => Key::Escape,
        "BACKSPACE" => Key::Backspace,
        "DELETE" => Key::Delete,
        "TAB" => Key::Tab,
        "SPACE" => Key::Space,
        "HOME" => Key::Home,
        "END" => Key::End,
        "PAGEUP" => Key::PageUp,
        "PAGEDOWN" => Key::PageDown,
        "LEFT" | "LEFTARROW" => Key::LeftArrow,
        "RIGHT" | "RIGHTARROW" => Key::RightArrow,
        "UP" | "UPARROW" => Key::UpArrow,
        "DOWN" | "DOWNARROW" => Key::DownArrow,
        _ if value.chars().count() == 1 => Key::Unicode(value.chars().next().unwrap_or_default()),
        value
            if value.strip_prefix('F').is_some_and(|number| {
                number.parse::<u8>().is_ok_and(|number| (1..=12).contains(&number))
            }) =>
        {
            match value[1..].parse::<u8>().unwrap_or_default() {
                1 => Key::F1,
                2 => Key::F2,
                3 => Key::F3,
                4 => Key::F4,
                5 => Key::F5,
                6 => Key::F6,
                7 => Key::F7,
                8 => Key::F8,
                9 => Key::F9,
                10 => Key::F10,
                11 => Key::F11,
                12 => Key::F12,
                _ => return Err(denied("Function keys above F12 are not supported yet.")),
            }
        }

        _ => return Err(denied("key must be a supported key name or combination.")),
    };

    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::parse_key;
    use enigo::Key;

    #[test]
    fn parses_common_keys_and_preserves_character_case() {
        assert!(matches!(parse_key("CTRL"), Ok(Key::Control)));
        assert!(matches!(parse_key("Return"), Ok(Key::Return)));
        assert!(matches!(parse_key("a"), Ok(Key::Unicode('a'))));
        assert!(matches!(parse_key("A"), Ok(Key::Unicode('A'))));
        assert!(matches!(parse_key("F12"), Ok(Key::F12)));
        assert!(parse_key("F13").is_err());
    }
}
