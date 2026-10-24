// 24 hours in one day.
const SECONDS_PER_DAY: u64 = 86_400;
// One Gregorian calendar era: 400 years, including 97 leap days.
const DAYS_PER_ERA: i64 = 146_097;
// Days from 0000-03-01 (the civil-date algorithm's epoch) to 1970-01-01.
const DAYS_BEFORE_UNIX_EPOCH: i64 = 719_468;

/// Parses an exact schedule time as RFC 3339 or a local date/time in a named
/// timezone. Local times use the system timezone when no timezone is provided.
pub fn parse_datetime(value: &str, timezone: Option<&str>) -> Result<u64, String> {
    if let Ok(timestamp) = value.parse::<jiff::Timestamp>() {
        return u64::try_from(timestamp.as_second())
            .map_err(|_| "The date and time must be after the Unix epoch.".into());
    }

    let datetime = value.parse::<jiff::civil::DateTime>().map_err(|_| {
        "Use YYYY-MM-DD HH:MM or an RFC 3339 date and time with an offset.".to_owned()
    })?;

    let timezone = match timezone {
        Some(timezone) => parse_timezone(timezone)
            .map_err(|error| format!("The schedule timezone is invalid: {error}."))?,
        None => jiff::tz::TimeZone::system(),
    };

    let zoned = datetime
        .to_zoned(timezone)
        .map_err(|error| format!("The local schedule time is invalid: {error}."))?;

    u64::try_from(zoned.timestamp().as_second())
        .map_err(|_| "The date and time must be after the Unix epoch.".into())
}

/// Parses a positive human-readable duration into seconds.
///
/// Months use 30 days and years use 365 days. Units can be abbreviated or
/// written out, for example `1m30s`, `1 mo`, or `1 month`.
pub fn parse_duration(value: &str) -> Result<u64, String> {
    let normalized = value.trim().to_ascii_lowercase();

    let mut characters = normalized.chars().peekable();
    let mut seconds = 0_u64;
    let mut found = false;

    while characters.peek().is_some() {
        while characters.peek().is_some_and(|character| character.is_whitespace()) {
            characters.next();
        }

        if characters.peek().is_none() {
            break;
        }

        let mut amount = 0_u64;
        let mut digits = 0;

        while characters.peek().is_some_and(|character| character.is_ascii_digit()) {
            amount = amount
                .checked_mul(10)
                .and_then(|value| value.checked_add(characters.next()?.to_digit(10)? as u64))
                .ok_or_else(|| "Duration is too large.".to_owned())?;

            digits += 1;
        }

        if digits == 0 {
            return Err("Use a number followed by a duration unit, such as 5m or 1 month.".into());
        }

        while characters.peek().is_some_and(|character| character.is_whitespace()) {
            characters.next();
        }

        let mut unit = String::new();

        while characters.peek().is_some_and(|character| character.is_ascii_alphabetic()) {
            unit.push(characters.next().unwrap_or_default());
        }

        let multiplier = match unit.as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => 1,
            "m" | "min" | "mins" | "minute" | "minutes" => 60,
            "h" | "hr" | "hrs" | "hour" | "hours" => 60 * 60,
            "d" | "day" | "days" => SECONDS_PER_DAY,
            "w" | "week" | "weeks" => 7 * SECONDS_PER_DAY,
            "mo" | "month" | "months" => 30 * SECONDS_PER_DAY,
            "y" | "yr" | "yrs" | "year" | "years" => 365 * SECONDS_PER_DAY,
            _ => return Err(format!("Unknown duration unit {unit:?}.")),
        };

        seconds = seconds
            .checked_add(amount.checked_mul(multiplier).ok_or("Duration is too large.")?)
            .ok_or("Duration is too large.")?;

        found = true;
    }

    if found && seconds > 0 {
        Ok(seconds)
    } else {
        Err("Duration must be greater than zero.".into())
    }
}

/// Formats seconds as a short English duration, such as `1 minute and 30 seconds`.
pub fn format_duration(mut seconds: u64) -> String {
    let units = [
        (365 * SECONDS_PER_DAY, "year"),
        (30 * SECONDS_PER_DAY, "month"),
        (7 * SECONDS_PER_DAY, "week"),
        (SECONDS_PER_DAY, "day"),
        (60 * 60, "hour"),
        (60, "minute"),
        (1, "second"),
    ];

    let mut parts = Vec::new();

    for (size, name) in units {
        let count = seconds / size;

        if count > 0 {
            parts.push(format!("{count} {name}{}", if count == 1 { "" } else { "s" }));
            seconds %= size;
        }
    }

    match parts.len() {
        0 => "0 seconds".into(),
        1 => parts.remove(0),

        _ => {
            let last = parts.pop().unwrap_or_default();
            format!("{} and {last}", parts.join(", "))
        }
    }
}

struct CivilDate {
    year: i64,
    month: i64,
    day: i64,
}

impl CivilDate {
    fn format(self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// Formats Unix timestamp seconds as a UTC calendar date.
pub fn format_date(timestamp: u64) -> String {
    let days_since_unix_epoch = (timestamp / SECONDS_PER_DAY) as i64;
    let date = civil_date_from_unix_days(days_since_unix_epoch);

    date.format()
}

/// Formats Unix timestamp seconds as a UTC date and time.
pub fn format_datetime(timestamp: u64) -> String {
    let date = format_date(timestamp);
    let seconds = timestamp % SECONDS_PER_DAY;
    let hour = seconds / 3_600;
    let minute = seconds % 3_600 / 60;
    let second = seconds % 60;

    format!("{date} {hour:02}:{minute:02}:{second:02} UTC")
}

/// Formats a timestamp in a configured timezone and includes its UTC equivalent.
pub fn format_datetime_in_timezone(timestamp: u64, timezone: &str) -> String {
    if timezone == "UTC" {
        return format_datetime(timestamp);
    }

    let Ok(timestamp_second) = i64::try_from(timestamp) else {
        return format_datetime(timestamp);
    };

    let Ok(timestamp_value) = jiff::Timestamp::from_second(timestamp_second) else {
        return format_datetime(timestamp);
    };

    let Ok(zone) = parse_timezone(timezone) else {
        return format_datetime(timestamp);
    };

    let local = timestamp_value.to_zoned(zone);
    let local_time = local.strftime("%Y-%m-%d %H:%M:%S %:z");

    format!("{local_time} ({timezone}; {})", format_datetime(timestamp))
}

fn parse_timezone(value: &str) -> Result<jiff::tz::TimeZone, String> {
    if let Some(offset) = value.strip_prefix('+').or_else(|| value.strip_prefix('-')) {
        if offset.len() != 5 || offset.as_bytes().get(2) != Some(&b':') {
            return Err("UTC offset must use the form +HH:MM or -HH:MM".into());
        }

        let hours = offset[..2].parse::<i32>().map_err(|error| error.to_string())?;
        let minutes = offset[3..].parse::<i32>().map_err(|error| error.to_string())?;

        if hours > 23 || minutes > 59 {
            return Err("UTC offset is outside the valid range".into());
        }

        let sign = if value.starts_with('-') { -1 } else { 1 };

        let seconds = sign * (hours * 3_600 + minutes * 60);

        return jiff::tz::Offset::from_seconds(seconds)
            .map(jiff::tz::TimeZone::fixed)
            .map_err(|error| error.to_string());
    }

    jiff::tz::TimeZone::get(value).map_err(|error| error.to_string())
}

fn civil_date_from_unix_days(days_since_unix_epoch: i64) -> CivilDate {
    let days_since_civil_epoch = days_since_unix_epoch + DAYS_BEFORE_UNIX_EPOCH;
    let era = days_since_civil_epoch / DAYS_PER_ERA;
    let day_of_era = days_since_civil_epoch - era * DAYS_PER_ERA;
    let year_of_era = year_within_era(day_of_era);
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - days_before_year_within_era(year_of_era);
    let (month, day) = month_and_day(day_of_year);

    if month <= 2 {
        year += 1;
    }

    CivilDate { year, month, day }
}

fn year_within_era(day_of_era: i64) -> i64 {
    // Correct for Gregorian leap years: every 4 years, except centuries, unless divisible by 400.
    (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365
}

fn days_before_year_within_era(year: i64) -> i64 {
    365 * year + year / 4 - year / 100
}

fn month_and_day(day_of_year: i64) -> (i64, i64) {
    let march_based_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * march_based_month + 2) / 5 + 1;
    let month = march_based_month + if march_based_month < 10 { 3 } else { -9 };

    (month, day)
}

#[cfg(test)]
mod tests {
    use super::{
        format_date, format_datetime, format_datetime_in_timezone, format_duration, parse_datetime,
        parse_duration,
    };

    #[test]
    fn formats_epoch_leap_days_and_year_boundaries() {
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(1_709_164_800), "2024-02-29");
        assert_eq!(format_date(951_782_400), "2000-02-29");
        assert_eq!(format_date(1_704_067_199), "2023-12-31");
        assert_eq!(format_date(1_704_067_200), "2024-01-01");
    }

    #[test]
    fn formats_the_full_unsigned_timestamp_range() {
        assert!(!format_date(u64::MAX).is_empty());
    }

    #[test]
    fn formats_utc_date_and_time() {
        assert_eq!(format_datetime(0), "1970-01-01 00:00:00 UTC");
        assert_eq!(format_datetime(1_709_164_800), "2024-02-29 00:00:00 UTC");
        assert_eq!(format_datetime(1_700_000_000), "2023-11-14 22:13:20 UTC");
    }

    #[test]
    fn formats_named_timezone_with_utc_reference() {
        assert_eq!(
            format_datetime_in_timezone(1_700_000_000, "Europe/Rome"),
            "2023-11-14 23:13:20 +01:00 (Europe/Rome; 2023-11-14 22:13:20 UTC)"
        );

        assert_eq!(format_datetime_in_timezone(1_700_000_000, "UTC"), "2023-11-14 22:13:20 UTC");
        assert_eq!(
            format_datetime_in_timezone(1_700_000_000, "+03:00"),
            "2023-11-15 01:13:20 +03:00 (+03:00; 2023-11-14 22:13:20 UTC)"
        );

        let first_fall_occurrence =
            "2024-11-03T05:30:00Z".parse::<jiff::Timestamp>().unwrap().as_second() as u64;

        let second_fall_occurrence =
            "2024-11-03T06:30:00Z".parse::<jiff::Timestamp>().unwrap().as_second() as u64;

        assert_eq!(
            format_datetime_in_timezone(first_fall_occurrence, "America/New_York"),
            "2024-11-03 01:30:00 -04:00 (America/New_York; 2024-11-03 05:30:00 UTC)"
        );

        assert_eq!(
            format_datetime_in_timezone(second_fall_occurrence, "America/New_York"),
            "2024-11-03 01:30:00 -05:00 (America/New_York; 2024-11-03 06:30:00 UTC)"
        );
    }

    #[test]
    fn parses_compact_and_written_durations() {
        assert_eq!(parse_duration("5m").unwrap(), 300);
        assert_eq!(parse_duration("60s").unwrap(), 60);
        assert_eq!(parse_duration("1y").unwrap(), 31_536_000);
        assert_eq!(parse_duration("1m30s").unwrap(), 90);
        assert_eq!(parse_duration("1mo").unwrap(), 2_592_000);
        assert_eq!(parse_duration("1 month").unwrap(), 2_592_000);
        assert_eq!(parse_duration("1 hour 30 minutes").unwrap(), 5_400);
    }

    #[test]
    fn rejects_invalid_durations_and_formats_english_units() {
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("1 fortnight").is_err());
        assert!(parse_duration("0s").is_err());
        assert_eq!(format_duration(90), "1 minute and 30 seconds");
        assert_eq!(format_duration(2_592_000), "1 month");
    }

    #[test]
    fn parses_schedule_times_with_offsets_or_a_local_timezone() {
        assert_eq!(
            parse_datetime("2030-10-20T09:00:00+02:00", None).unwrap(),
            "2030-10-20T07:00:00Z".parse::<jiff::Timestamp>().unwrap().as_second() as u64
        );

        assert_eq!(
            parse_datetime("2030-10-20 09:00", Some("UTC")).unwrap(),
            "2030-10-20T09:00:00Z".parse::<jiff::Timestamp>().unwrap().as_second() as u64
        );

        assert_eq!(
            parse_datetime("2030-10-20 09:00", Some("+02:00")).unwrap(),
            "2030-10-20T07:00:00Z".parse::<jiff::Timestamp>().unwrap().as_second() as u64
        );

        assert!(parse_datetime("tomorrow morning", Some("UTC")).is_err());
        assert!(parse_datetime("2030-10-20 09:00", Some("Mars/Olympus")).is_err());
    }
}
