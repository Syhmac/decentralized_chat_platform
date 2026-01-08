pub mod utils {
    use serde::{Serialize, Deserialize};
    use time::{OffsetDateTime, UtcOffset, format_description};

    #[derive(Serialize, Deserialize, Clone)]
    pub struct ChatMessage {
        //pub message_id: i64,
        //pub user_id: i64,
        //pub channel_id: i64,
        pub content: String,
        pub timestamp: i64,
        pub username: String,
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
            let format = format_description::parse(
              "[year]-[month]-[day] [hour]:[minute]",
            ).unwrap();
            utc_time.format(&format).unwrap()
        } else {
            "Invalid date".to_string()
        }
    }
}