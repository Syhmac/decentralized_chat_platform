pub mod utils {
    use serde::{Deserialize, Serialize};
    use time::{OffsetDateTime, UtcDateTime, UtcOffset, format_description};

    #[derive(Serialize, Deserialize, Clone)]
    pub struct AuthRequired {
        pub requires_auth: bool,
    }

    #[derive(Serialize, Deserialize, Clone)]
    pub struct ChatMessage {
        pub message_id: i64,
        pub user_id: i64,
        pub channel_id: i64,
        pub content: String,
        pub timestamp: i64,
    }

    #[derive(Serialize, Deserialize, Clone)]
    pub struct ChatMessageUnAuth {
        pub content: String,
        pub timestamp: i64,
        pub username: String,
    }

    #[derive(Serialize, Deserialize, Clone)]
    pub struct MessageRequest {
        pub channel_id: i64,
        pub authentication: bool,
        pub older_than: i64,
        pub count: u64,
    }

    #[derive(Serialize, Deserialize, Clone)]
    pub struct User {
        pub user_id: i64,
        pub display_name: String,
    }

    pub fn get_timestamp(sys_time: OffsetDateTime) -> i64 {
        sys_time.unix_timestamp()
    }

    pub fn get_string_time(timestamp: i64, offset: UtcOffset) -> String {
        let utc_time = OffsetDateTime::from_unix_timestamp(timestamp);
        if let Ok(mut utc_time) = utc_time {
            utc_time = utc_time.to_offset(offset);
            let format = format_description::parse("[year]-[month]-[day] [hour]:[minute]").unwrap();
            utc_time.format(&format).unwrap()
        } else {
            "Invalid date".to_string()
        }
    }

    pub fn convert_str_time_to_timestamp(time_str: &str, offset: UtcOffset) -> Option<i64> {
        let format =
            format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]").unwrap();
        match UtcDateTime::parse(time_str, &format) {
            Ok(dt) => {
                let dt_utc = dt.to_offset(offset);
                Some(dt_utc.unix_timestamp())
            }
            Err(e) => {
                eprintln!("Error parsing time string: {}", e);
                None
            }
        }
    }
}
