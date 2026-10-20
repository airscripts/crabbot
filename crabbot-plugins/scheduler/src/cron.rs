use jiff::{
    Timestamp,
    tz::{Offset, TimeZone},
};

pub(super) struct CronSchedule {
    minutes: Vec<u32>,
    hours: Vec<u32>,
    days: Vec<u32>,
    months: Vec<u32>,
    weekdays: Vec<u32>,
    day_any: bool,
    weekday_any: bool,
}

impl CronSchedule {
    pub(super) fn parse(value: &str) -> crabbot_core::Result<Self> {
        let fields = value.split_whitespace().collect::<Vec<_>>();

        if fields.len() != 5 {
            return Err(crabbot_core::Error::Denied("Cron must contain five fields.".into()));
        }

        let [minutes, hours, days, months, weekdays] = [
            parse_field(fields[0], 0, 59)?,
            parse_field(fields[1], 0, 23)?,
            parse_field(fields[2], 1, 31)?,
            parse_field(fields[3], 1, 12)?,
            parse_field(fields[4], 0, 7)?,
        ];

        Ok(Self {
            minutes,
            hours,
            days,
            months,
            weekdays,
            day_any: wildcard_field(fields[2]),
            weekday_any: wildcard_field(fields[4]),
        })
    }

    pub(super) fn matches(&self, fields: &[u32; 5]) -> bool {
        let minute = self.minutes.binary_search(&fields[0]).is_ok();
        let hour = self.hours.binary_search(&fields[1]).is_ok();
        let month = self.months.binary_search(&fields[3]).is_ok();
        let day = self.days.binary_search(&fields[2]).is_ok();
        let weekday = self.weekdays.binary_search(&fields[4]).is_ok()
            || (fields[4] == 0 && self.weekdays.binary_search(&7).is_ok());
        let calendar = match (self.day_any, self.weekday_any) {
            (true, true) => true,
            (true, false) => weekday,
            (false, true) => day,
            (false, false) => day || weekday,
        };

        minute && hour && month && calendar
    }

    pub(super) fn minutes_until_next(&self, current: u32) -> u64 {
        let next = self.minutes.partition_point(|minute| *minute <= current);

        if let Some(minute) = self.minutes.get(next) {
            u64::from(minute - current)
        } else {
            u64::from(60 - current + self.minutes[0])
        }
    }
}

fn wildcard_field(value: &str) -> bool {
    value == "*"
        || value == "?"
        || value.split(',').any(|item| {
            item.split_once('/').is_some_and(|(base, step)| {
                (base == "*" || base == "?") && step.parse::<u32>().ok() == Some(1)
            })
        })
}

pub(super) fn validate_cron(value: &str) -> crabbot_core::Result<()> {
    CronSchedule::parse(value).map(|_| ())
}

pub(super) fn parse_field(
    value: &str,
    minimum: u32,
    maximum: u32,
) -> crabbot_core::Result<Vec<u32>> {
    let mut values = Vec::new();

    for item in value.split(',') {
        let (base, step) = item
            .split_once('/')
            .map_or((item, 1), |(base, step)| (base, step.parse::<u32>().unwrap_or(0)));

        if step == 0 {
            return Err(crabbot_core::Error::Denied("Cron step must be positive.".into()));
        }

        let (start, end) = if base == "*" || base == "?" {
            (minimum, maximum)
        } else if let Some((start, end)) = base.split_once('-') {
            let start = start.parse::<u32>().map_err(|_| {
                crabbot_core::Error::Denied("Cron range endpoint is invalid.".into())
            })?;

            let end = end.parse::<u32>().map_err(|_| {
                crabbot_core::Error::Denied("Cron range endpoint is invalid.".into())
            })?;

            (start, end)
        } else if item.contains('/') {
            let point = base.parse::<u32>().unwrap_or(u32::MAX);
            (point, maximum)
        } else {
            let point = base.parse::<u32>().unwrap_or(u32::MAX);
            (point, point)
        };

        if start < minimum || end > maximum || start > end {
            return Err(crabbot_core::Error::Denied(
                "Cron field is outside its valid range.".into(),
            ));
        }

        values.extend((start..=end).step_by(step as usize));
    }

    if values.is_empty() {
        return Err(crabbot_core::Error::Denied("Cron field cannot be empty.".into()));
    }

    values.sort_unstable();
    values.dedup();
    Ok(values)
}

pub(super) fn timezone(value: &str) -> crabbot_core::Result<TimeZone> {
    if value == "UTC" {
        return Ok(TimeZone::UTC);
    }

    if let Some(offset) = value.strip_prefix('+').or_else(|| value.strip_prefix('-')) {
        if offset.len() != 5 || offset.as_bytes().get(2) != Some(&b':') {
            return Err(crabbot_core::Error::Denied("Scheduler zone offset is invalid.".into()));
        }

        let hours = offset[..2]
            .parse::<i32>()
            .map_err(|_| crabbot_core::Error::Denied("Scheduler zone offset is invalid.".into()))?;

        let minutes = offset[3..]
            .parse::<i32>()
            .map_err(|_| crabbot_core::Error::Denied("Scheduler zone offset is invalid.".into()))?;

        if hours > 23 || minutes > 59 {
            return Err(crabbot_core::Error::Denied("Scheduler zone offset is invalid.".into()));
        }

        let sign = if value.starts_with('-') { -1 } else { 1 };

        let seconds = sign * (hours * 3_600 + minutes * 60);
        return Offset::from_seconds(seconds).map(TimeZone::fixed).map_err(|error| {
            crabbot_core::Error::Denied(format!("Scheduler zone is invalid: {error}."))
        });
    }

    TimeZone::get(value).map_err(|error| {
        crabbot_core::Error::Denied(format!("Scheduler zone is invalid: {error}."))
    })
}

pub(super) fn validate_zone(value: &str) -> crabbot_core::Result<()> {
    timezone(value).map(|_| ())
}

pub(super) fn next(cron: &str, zone: &str, after: u64) -> crabbot_core::Result<u64> {
    let schedule = CronSchedule::parse(cron)?;
    let zone = timezone(zone)?;
    let start = after.saturating_add(60 - after % 60);

    if start > i64::MAX as u64 {
        return Ok(u64::MAX);
    }

    let search_end = start.saturating_add(8_u64 * 366 * 24 * 60 * 60);
    let mut seconds = start;

    while seconds < search_end {
        let timestamp = Timestamp::from_second(i64::try_from(seconds).map_err(|_| {
            crabbot_core::Error::Denied("Scheduler timestamp exceeded its supported range.".into())
        })?)
        .map_err(|error| {
            crabbot_core::Error::Denied(format!("Scheduler timestamp is invalid: {error}."))
        })?;

        let local = timestamp.to_zoned(zone.clone());
        let weekday = local.weekday().to_sunday_zero_offset();
        let values = [
            u32::try_from(local.minute()).unwrap_or_default(),
            u32::try_from(local.hour()).unwrap_or_default(),
            u32::try_from(local.day()).unwrap_or_default(),
            u32::try_from(local.month()).unwrap_or_default(),
            u32::try_from(weekday).unwrap_or_default(),
        ];

        if schedule.matches(&values) {
            return Ok(seconds);
        }

        seconds = seconds.saturating_add(schedule.minutes_until_next(values[0]).saturating_mul(60));
    }

    Err(crabbot_core::Error::Denied("Cron has no occurrence in the search window.".into()))
}
