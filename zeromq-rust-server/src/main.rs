use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use zmq;

fn get_iso8601_time() -> String {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap();
    format_time(duration)
}

fn format_time(duration: Duration) -> String {
    let secs = duration.as_secs();
    let micros = duration.subsec_micros();

    let (year, month, day, hour, min, sec) = seconds_to_datetime(secs);

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
          year, month, day, hour, min, sec, millis
    )
}

fn seconds_to_datetime(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    const LEAPOCH: i64 = 946684800 + 86400 * (31 + 29);
    const DAYS_PER_400Y: i64 = 365 * 400 + 97;
    const DAYS_PER_100Y: i64 = 365 * 100 + 24;
    const DAYS_PER_4Y: i64 = 365 * 4 + 1;

    let days = (secs / 86400) as i64;
    let secs = secs % 86400;

    let mut qc_cycles = days / DAYS_PER_400Y;
    let mut rem_days = days % DAYS_PER_400Y;

    if rem_days < 0 {
        rem_days += DAYS_PER_400Y;
        qc_cycles -= 1;
    }

    let mut c_cycles = rem_days / DAYS_PER_100Y;
    rem_days %= DAYS_PER_100Y;

    if c_cycles == 4 {
        c_cycles -= 1;
        rem_days += DAYS_PER_100Y;
    }

    let mut q_cycles = rem_days / DAYS_PER_4Y;
    rem_days %= DAYS_PER_4Y;

    if q_cycles == 25 {
        q_cycles -= 1;
        rem_days += DAYS_PER_4Y;
    }

    let mut rem_years = rem_days / 365;
    rem_days %= 365;

    if rem_years == 4 {
        rem_years -= 1;
        rem_days += 365;
    }

    let mut year = 2000 + rem_years + 4 * q_cycles + 100 * c_cycles + 400 * qc_cycles;

    let months = [31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 29];
    let mut mon = 0;
    for &mlen in &months {
        if rem_days < mlen {
            break;
        }
        rem_days -= mlen;
        mon += 1;
    }

    let mday = rem_days + 1;
    let mon = mon + 1;

    let hour = secs / 3600;
    let min = (secs % 3600) / 60;
    let sec = secs % 60;

    (year as u32, mon as u32, mday as u32, hour as u32, min as u32, sec as u32)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let context = zmq::Context::new();
    let responder = context.socket(zmq::REP)?;
    responder.bind("tcp://*:5555")?;

    let mut store = HashMap::new();

    println!("{} - Server started", get_iso8601_time());

    loop {
        let message = responder.recv_string(0)?;
        let start_time = SystemTime::now();
        
        if let Ok(request) = message {
            let parts: Vec<&str> = request.split_whitespace().collect();
            
            if parts.len() < 2 {
                let response = "Invalid request format";
                responder.send(response, 0)?;
                continue;
            }

            let command = parts[0];
            let key = parts[1];

            let response = match command {
                "SET" => {
                    if parts.len() < 3 {
                        "Invalid SET format".to_string()
                    } else {
                        let value = parts[2..].join(" ");
                        store.insert(key.to_string(), value);
                        "OK".to_string()
                    }
                },
                "GET" => {
                    match store.get(key) {
                        Some(value) => value.clone(),
                        None => "Key not found".to_string(),
                    }
                },
                _ => "Unknown command".to_string(),
            };

            let end_time = SystemTime::now();
            let duration = end_time.duration_since(start_time).unwrap();

            println!("{} - Processed request: {} {} ({}µs)", 
                     get_iso8601_time(), command, key, duration.as_micros());

            responder.send(&response, 0)?;
        }
    }

}
