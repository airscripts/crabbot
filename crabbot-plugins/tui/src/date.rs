// 24 hours in one day.
const SECONDS_PER_DAY: u64 = 86_400;
// One Gregorian calendar era: 400 years, including 97 leap days.
const DAYS_PER_ERA: i64 = 146_097;
// Days from 0000-03-01 (the civil-date algorithm's epoch) to 1970-01-01.
const DAYS_BEFORE_UNIX_EPOCH: i64 = 719_468;

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

pub(super) fn format_date(timestamp: u64) -> Option<String> {
    let days_since_unix_epoch = i64::try_from(timestamp / SECONDS_PER_DAY).ok()?;
    let date = civil_date_from_unix_days(days_since_unix_epoch);

    Some(date.format())
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
    use super::format_date;

    #[test]
    fn formats_epoch_and_leap_day_dates() {
        assert_eq!(format_date(0).as_deref(), Some("1970-01-01"));
        assert_eq!(format_date(1_709_164_800).as_deref(), Some("2024-02-29")); // 2024-02-29 UTC.
        assert_eq!(format_date(951_782_400).as_deref(), Some("2000-02-29")); // 2000-02-29 UTC.
    }

    #[test]
    fn formats_year_boundaries_and_large_timestamps() {
        assert_eq!(format_date(1_704_067_199).as_deref(), Some("2023-12-31")); // 2023-12-31 UTC.
        assert_eq!(format_date(1_704_067_200).as_deref(), Some("2024-01-01")); // 2024-01-01 UTC.
        assert!(format_date(u64::MAX).is_some());
    }
}
