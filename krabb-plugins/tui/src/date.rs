pub(super) fn format_datetime(timestamp: u64) -> Option<String> {
    let timestamp = i64::try_from(timestamp).ok()?;
    let timestamp = jiff::Timestamp::from_second(timestamp).ok()?;
    let zoned = timestamp.to_zoned(jiff::tz::TimeZone::system());

    Some(zoned.strftime("%Y-%m-%d at %H:%M").to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn separates_local_date_and_time_with_at() {
        let formatted = super::format_datetime(1_709_164_800).unwrap();
        let (date, time) = formatted.split_once(" at ").unwrap();

        assert_eq!(date.len(), 10);
        assert_eq!(time.len(), 5);
    }
}
