use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const VERSION: u8 = 1;
const HEADER_SIZE: usize = 22;
pub const MAX_RECORD_PAYLOAD: usize = 8 * 1024;
const MAX_PAGE_PAYLOAD: usize = 32 * 1024;
const MAX_PAGE_RECORDS: usize = 100;
const SYNC_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LogStream {
    Build = 0,
    Stdout = 1,
    Stderr = 2,
}

impl LogStream {
    fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }

    fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Build),
            1 => Some(Self::Stdout),
            2 => Some(Self::Stderr),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct BuildLog {
    writer: Arc<Mutex<LogWriter>>,
}

struct LogWriter {
    file: File,
    last_sync: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub timestamp_ms: u64,
    pub step_index: Option<u32>,
    pub stream: &'static str,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub records: Vec<LogRecord>,
    pub next_cursor: String,
    pub has_more: bool,
    pub integrity: &'static str,
    pub warning: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadLogError {
    InvalidCursor,
    Io(String),
}

impl BuildLog {
    pub fn open_append(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed to create build log directory {}: {err}",
                    parent.display()
                )
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|err| format!("failed to open build log {}: {err}", path.display()))?;
        Ok(Self {
            writer: Arc::new(Mutex::new(LogWriter {
                file,
                last_sync: Instant::now(),
            })),
        })
    }

    pub fn append(
        &self,
        step_index: Option<u32>,
        stream: LogStream,
        payload: &[u8],
    ) -> Result<(), String> {
        if payload.len() > MAX_RECORD_PAYLOAD {
            return Err(format!(
                "build log record is {} bytes; maximum is {MAX_RECORD_PAYLOAD}",
                payload.len()
            ));
        }
        let length = u32::try_from(payload.len())
            .map_err(|err| format!("build log record length is invalid: {err}"))?;
        let timestamp_ms: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| format!("system clock is before Unix epoch: {err}"))?
            .as_millis()
            .try_into()
            .map_err(|err| format!("build log timestamp is outside supported range: {err}"))?;
        let step = step_index.unwrap_or(u32::MAX);

        let mut record = Vec::with_capacity(HEADER_SIZE + payload.len());
        record.push(VERSION);
        record.extend_from_slice(&length.to_le_bytes());
        record.extend_from_slice(&timestamp_ms.to_le_bytes());
        record.extend_from_slice(&step.to_le_bytes());
        record.push(stream as u8);
        record.extend_from_slice(&0u32.to_le_bytes());
        record.extend_from_slice(payload);
        let checksum = crc32(
            &record[..18]
                .iter()
                .chain(payload)
                .copied()
                .collect::<Vec<_>>(),
        );
        record[18..22].copy_from_slice(&checksum.to_le_bytes());

        let mut writer = self
            .writer
            .lock()
            .map_err(|_| "build log writer lock is poisoned".to_string())?;
        writer
            .file
            .write_all(&record)
            .and_then(|()| writer.file.flush())
            .map_err(|err| format!("failed to append build log record: {err}"))?;
        if writer.last_sync.elapsed() >= SYNC_INTERVAL {
            writer
                .file
                .sync_data()
                .map_err(|err| format!("failed to sync build log: {err}"))?;
            writer.last_sync = Instant::now();
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<(), String> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| "build log writer lock is poisoned".to_string())?;
        writer
            .file
            .sync_data()
            .map_err(|err| format!("failed to sync build log: {err}"))?;
        writer.last_sync = Instant::now();
        Ok(())
    }
}

pub fn read_page(
    path: &Path,
    cursor: Option<&str>,
    limit: usize,
    recover_tail: bool,
) -> Result<Option<LogPage>, ReadLogError> {
    if limit == 0 || limit > MAX_PAGE_RECORDS {
        return Err(ReadLogError::InvalidCursor);
    }
    let requested_offset = parse_cursor(cursor)?;
    let mut file = match OpenOptions::new().read(true).write(recover_tail).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && requested_offset == 0 => {
            return Ok(None);
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(ReadLogError::InvalidCursor);
        }
        Err(err) => {
            return Err(ReadLogError::Io(format!(
                "failed to open build log {}: {err}",
                path.display()
            )));
        }
    };
    let file_len = file
        .metadata()
        .map_err(|err| ReadLogError::Io(format!("failed to inspect build log: {err}")))?
        .len();
    let mut offset = 0u64;
    let mut page_next_offset = requested_offset;
    let mut cursor_is_boundary = requested_offset == 0;
    let mut records = Vec::new();
    let mut payload_bytes = 0usize;
    let mut has_more = false;
    let mut integrity = "valid";
    let mut warning = None;

    while offset < file_len {
        let record_start = offset;
        let remaining = file_len - offset;
        if remaining < HEADER_SIZE as u64 {
            integrity = "recovered_tail";
            break;
        }
        let mut header = [0u8; HEADER_SIZE];
        file.seek(SeekFrom::Start(offset))
            .and_then(|_| file.read_exact(&mut header))
            .map_err(|err| ReadLogError::Io(format!("failed to read build log: {err}")))?;
        let payload_len = u32::from_le_bytes(header[1..5].try_into().unwrap()) as usize;
        let total_len = HEADER_SIZE
            .checked_add(payload_len)
            .ok_or_else(|| ReadLogError::Io("build log record length overflow".to_string()))?;
        if header[0] != VERSION
            || payload_len > MAX_RECORD_PAYLOAD
            || LogStream::from_byte(header[17]).is_none()
        {
            integrity = "corrupt";
            warning = Some("log_corrupt");
            break;
        }
        if total_len as u64 > remaining {
            integrity = "recovered_tail";
            break;
        }
        let mut payload = vec![0; payload_len];
        file.read_exact(&mut payload)
            .map_err(|err| ReadLogError::Io(format!("failed to read build log payload: {err}")))?;
        let expected_checksum = u32::from_le_bytes(header[18..22].try_into().unwrap());
        let actual_checksum = crc32(
            &header[..18]
                .iter()
                .chain(payload.iter())
                .copied()
                .collect::<Vec<_>>(),
        );
        if actual_checksum != expected_checksum {
            integrity = "corrupt";
            warning = Some("log_corrupt");
            break;
        }
        offset += total_len as u64;
        if requested_offset == record_start || requested_offset == offset {
            cursor_is_boundary = true;
        }
        if record_start >= requested_offset {
            if records.len() == limit || payload_bytes + payload.len() > MAX_PAGE_PAYLOAD {
                has_more = true;
            } else if !has_more {
                payload_bytes += payload.len();
                let stream = LogStream::from_byte(header[17]).unwrap().as_str();
                let step = u32::from_le_bytes(header[13..17].try_into().unwrap());
                records.push(LogRecord {
                    timestamp_ms: u64::from_le_bytes(header[5..13].try_into().unwrap()),
                    step_index: (step != u32::MAX).then_some(step),
                    stream,
                    payload,
                });
                page_next_offset = offset;
            }
        }
    }

    if !cursor_is_boundary {
        return Err(ReadLogError::InvalidCursor);
    }
    if integrity == "recovered_tail" {
        if recover_tail {
            file.set_len(offset)
                .and_then(|()| file.sync_data())
                .map_err(|err| {
                    ReadLogError::Io(format!(
                        "failed to recover incomplete build log tail: {err}"
                    ))
                })?;
        } else {
            integrity = "valid";
        }
    }
    Ok(Some(LogPage {
        records,
        next_cursor: encode_cursor(page_next_offset),
        has_more,
        integrity,
        warning,
    }))
}

fn parse_cursor(cursor: Option<&str>) -> Result<u64, ReadLogError> {
    match cursor {
        None => Ok(0),
        Some(cursor)
            if cursor.len() == 16 && cursor.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
        {
            u64::from_str_radix(cursor, 16).map_err(|_| ReadLogError::InvalidCursor)
        }
        Some(_) => Err(ReadLogError::InvalidCursor),
    }
}

fn encode_cursor(offset: u64) -> String {
    format!("{offset:016x}")
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb88320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

pub fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = *chunk.get(1).unwrap_or(&0);
        let third = *chunk.get(2).unwrap_or(&0);
        encoded.push(TABLE[(first >> 2) as usize] as char);
        encoded.push(TABLE[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        encoded.push(if chunk.len() > 1 {
            TABLE[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            TABLE[(third & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn reads_pages_only_at_complete_record_boundaries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("build.rkl");
        assert_eq!(
            read_page(&path, Some("0000000000000001"), 10, true),
            Err(ReadLogError::InvalidCursor)
        );
        let log = BuildLog::open_append(&path).unwrap();
        log.append(None, LogStream::Build, b"started").unwrap();
        log.append(Some(0), LogStream::Stdout, b"hello").unwrap();

        let first = read_page(&path, None, 1, true).unwrap().unwrap();
        assert_eq!(first.records.len(), 1);
        assert_eq!(first.records[0].payload, b"started");
        assert!(first.has_more);

        let second = read_page(&path, Some(&first.next_cursor), 10, true)
            .unwrap()
            .unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].stream, "stdout");
        assert_eq!(second.records[0].step_index, Some(0));
        assert!(!second.has_more);

        assert_eq!(
            read_page(&path, Some("0000000000000001"), 10, true),
            Err(ReadLogError::InvalidCursor)
        );
    }

    #[test]
    fn recovers_incomplete_tail_and_reports_interior_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("build.rkl");
        let log = BuildLog::open_append(&path).unwrap();
        log.append(None, LogStream::Build, b"complete").unwrap();
        drop(log);
        let valid_len = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[VERSION, 1])
            .unwrap();

        let recovered = read_page(&path, None, 10, true).unwrap().unwrap();
        assert_eq!(recovered.integrity, "recovered_tail");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER_SIZE] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        let corrupted = read_page(&path, None, 10, true).unwrap().unwrap();
        assert_eq!(corrupted.integrity, "corrupt");
        assert_eq!(corrupted.warning, Some("log_corrupt"));
        assert!(corrupted.records.is_empty());
    }

    #[test]
    fn base64_encodes_binary_payloads() {
        assert_eq!(encode_base64(b"hello"), "aGVsbG8=");
        assert_eq!(encode_base64(&[0, 255]), "AP8=");
        assert_eq!(crc32(b"123456789"), 0xcbf43926);
    }
}
