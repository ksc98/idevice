//! Sysmontap service - System monitoring tap for processes and system stats

use std::collections::VecDeque;

use plist::{Dictionary, Value};

use super::message::AuxValue;
use super::remote_server::{Channel, RemoteServerClient};
use crate::{IdeviceError, ReadWrite, obf};

/// Configuration for sysmontap sampling
#[derive(Debug, Clone)]
pub struct SysmontapConfig {
    /// Sampling interval in milliseconds
    pub interval_ms: u32,
    /// Process attributes to collect (from DeviceInfoClient::sysmon_process_attributes)
    pub process_attributes: Vec<String>,
    /// System attributes to collect (from DeviceInfoClient::sysmon_system_attributes)
    pub system_attributes: Vec<String>,
}

impl Default for SysmontapConfig {
    fn default() -> Self {
        Self {
            interval_ms: 500,
            process_attributes: Vec::new(),
            system_attributes: Vec::new(),
        }
    }
}

/// A sysmontap sample row
#[derive(Debug, Clone)]
pub struct SysmontapSample {
    /// Per-process data keyed by PID string
    pub processes: Option<Dictionary>,
    /// System-wide attribute array (order matches SysmontapConfig::system_attributes)
    pub system: Option<Vec<Value>>,
    /// CPU usage summary
    pub system_cpu_usage: Option<Dictionary>,
}

/// Client for system monitoring tap
#[derive(Debug)]
pub struct SysmontapClient<'a, R: ReadWrite> {
    channel: Channel<'a, R>,
    /// Rows from the last pushed array that have not been returned yet
    pending: VecDeque<Dictionary>,
}

impl<'a, R: ReadWrite> SysmontapClient<'a, R> {
    pub async fn new(client: &'a mut RemoteServerClient<R>) -> Result<Self, IdeviceError> {
        let channel = client
            .make_channel(obf!("com.apple.instruments.server.services.sysmontap"))
            .await?;
        Ok(Self {
            channel,
            pending: VecDeque::new(),
        })
    }

    /// Sends the configuration to the device. No reply expected.
    pub async fn set_config(&mut self, config: &SysmontapConfig) -> Result<(), IdeviceError> {
        let mut cfg = Dictionary::new();
        cfg.insert(
            "ur".into(),
            Value::Integer((config.interval_ms as i64).into()),
        );
        cfg.insert("bm".into(), Value::Integer(0i64.into()));
        cfg.insert(
            "procAttrs".into(),
            Value::Array(
                config
                    .process_attributes
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        );
        cfg.insert(
            "sysAttrs".into(),
            Value::Array(
                config
                    .system_attributes
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        );
        cfg.insert("cpuUsage".into(), Value::Boolean(true));
        cfg.insert("physFootprint".into(), Value::Boolean(true));
        cfg.insert(
            "sampleInterval".into(),
            Value::Integer(((config.interval_ms as i64) * 1_000_000).into()),
        );

        self.channel
            .call_method(
                Some(Value::String("setConfig:".into())),
                Some(vec![AuxValue::archived_value(Value::Dictionary(cfg))]),
                false,
            )
            .await
    }

    /// Starts sampling. No reply expected.
    /// After start, the device pushes an initial ack message that must be consumed.
    pub async fn start(&mut self) -> Result<(), IdeviceError> {
        self.channel
            .call_method(Some(Value::String("start".into())), None, false)
            .await?;
        // Consume the initial ack
        self.channel.read_message().await?;
        Ok(())
    }

    /// Stops sampling. No reply expected.
    pub async fn stop(&mut self) -> Result<(), IdeviceError> {
        self.channel
            .call_method(Some(Value::String("stop".into())), None, false)
            .await
    }

    /// Reads the next sysmontap data row.
    /// The device pushes arrays of row dicts (typically a system row and a
    /// process row together); every data row is returned, one per call.
    pub async fn next_sample(&mut self) -> Result<SysmontapSample, IdeviceError> {
        loop {
            if let Some(dict) = self.pending.pop_front() {
                return Ok(parse_sample_dict(dict));
            }

            let msg = self.channel.read_message().await?;
            let Some(decoded) = msg.data else { continue };

            // The tap pushes an Array of row dicts
            let rows: Vec<Value> = match decoded {
                Value::Array(arr) => arr,
                Value::Dictionary(d) => vec![Value::Dictionary(d)],
                _ => continue,
            };

            self.pending.extend(rows.into_iter().filter_map(|row| {
                row.into_dictionary().filter(|dict| {
                    dict.contains_key("Processes")
                        || dict.contains_key("System")
                        || dict.contains_key("SystemCPUUsage")
                })
            }));
        }
    }
}

fn parse_sample_dict(dict: Dictionary) -> SysmontapSample {
    let processes = dict
        .get("Processes")
        .and_then(|v| v.as_dictionary())
        .cloned();
    let system = dict.get("System").and_then(|v| v.as_array()).cloned();
    let system_cpu_usage = dict
        .get("SystemCPUUsage")
        .and_then(|v| v.as_dictionary())
        .cloned();
    SysmontapSample {
        processes,
        system,
        system_cpu_usage,
    }
}
