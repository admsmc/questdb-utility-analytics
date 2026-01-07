use std::{
    marker::PhantomData,
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use futures::StreamExt;
use rust_client::domain::{GenerationOutput, MeterUsage};
use time::OffsetDateTime;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::pipeline::{Envelope, PipelineError, Sink};

/// Escape measurement/tag keys/tag values/field keys for ILP.
///
/// ILP requires escaping commas, spaces and equals with a backslash.
fn ilp_escape_ident(s: &str, out: &mut String) {
    for ch in s.chars() {
        match ch {
            ',' | ' ' | '=' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
}

fn push_tag(out: &mut String, key: &str, value: &str) {
    out.push(',');
    ilp_escape_ident(key, out);
    out.push('=');
    ilp_escape_ident(value, out);
}

fn push_field_f64(out: &mut String, first: &mut bool, key: &str, value: f64) {
    if *first {
        *first = false;
    } else {
        out.push(',');
    }

    ilp_escape_ident(key, out);
    out.push('=');
    // For performance we keep to numeric fields only.
    out.push_str(&value.to_string());
}

fn ts_to_unix_nanos(ts: OffsetDateTime) -> i128 {
    ts.unix_timestamp_nanos()
}

fn hash_str(hasher: &mut blake3::Hasher, s: &str) {
    let len = s.len() as u32;
    hasher.update(&len.to_le_bytes());
    hasher.update(s.as_bytes());
}

fn hash_opt_str(hasher: &mut blake3::Hasher, s: &Option<String>) {
    match s {
        Some(v) => {
            hasher.update(&[1]);
            hash_str(hasher, v);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_f64(hasher: &mut blake3::Hasher, v: f64) {
    hasher.update(&v.to_bits().to_le_bytes());
}

fn hash_opt_f64(hasher: &mut blake3::Hasher, v: Option<f64>) {
    match v {
        Some(x) => {
            hasher.update(&[1]);
            hash_f64(hasher, x);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn event_id_meter_usage(m: &MeterUsage) -> String {
    let mut h = blake3::Hasher::new();
    h.update(&ts_to_unix_nanos(m.ts).to_le_bytes());
    hash_str(&mut h, &m.meter_id);
    hash_opt_str(&mut h, &m.premise_id);
    hash_f64(&mut h, m.kwh);
    hash_opt_f64(&mut h, m.kvarh);
    hash_opt_f64(&mut h, m.kva_demand);
    hash_opt_str(&mut h, &m.quality_flag);
    hash_opt_str(&mut h, &m.source_system);
    h.finalize().to_hex().to_string()
}

fn event_id_generation(g: &GenerationOutput) -> String {
    let mut h = blake3::Hasher::new();
    h.update(&ts_to_unix_nanos(g.ts).to_le_bytes());
    hash_str(&mut h, &g.plant_id);
    hash_opt_str(&mut h, &g.unit_id);
    hash_f64(&mut h, g.mw);
    hash_opt_f64(&mut h, g.mvar);
    hash_opt_str(&mut h, &g.status);
    hash_opt_str(&mut h, &g.fuel_type);
    h.finalize().to_hex().to_string()
}

pub trait IlpEncode {
    fn write_ilp_line(&self, out: &mut String);
}

impl IlpEncode for MeterUsage {
    fn write_ilp_line(&self, out: &mut String) {
        // measurement
        out.push_str("meter_usage");

        // tags (SYMBOL columns)
        let event_id = event_id_meter_usage(self);
        push_tag(out, "event_id", &event_id);
        push_tag(out, "meter_id", &self.meter_id);
        if let Some(premise_id) = &self.premise_id {
            push_tag(out, "premise_id", premise_id);
        }
        if let Some(q) = &self.quality_flag {
            push_tag(out, "quality_flag", q);
        }
        if let Some(src) = &self.source_system {
            push_tag(out, "source_system", src);
        }

        // fields (numeric metrics)
        out.push(' ');
        let mut first = true;
        push_field_f64(out, &mut first, "kwh", self.kwh);
        if let Some(v) = self.kvarh {
            push_field_f64(out, &mut first, "kvarh", v);
        }
        if let Some(v) = self.kva_demand {
            push_field_f64(out, &mut first, "kva_demand", v);
        }

        // timestamp (nanos)
        out.push(' ');
        out.push_str(&ts_to_unix_nanos(self.ts).to_string());
    }
}

impl IlpEncode for GenerationOutput {
    fn write_ilp_line(&self, out: &mut String) {
        out.push_str("generation_output");

        // tags
        let event_id = event_id_generation(self);
        push_tag(out, "event_id", &event_id);
        push_tag(out, "plant_id", &self.plant_id);
        if let Some(unit_id) = &self.unit_id {
            push_tag(out, "unit_id", unit_id);
        }
        if let Some(status) = &self.status {
            push_tag(out, "status", status);
        }
        if let Some(fuel) = &self.fuel_type {
            push_tag(out, "fuel_type", fuel);
        }

        // fields
        out.push(' ');
        let mut first = true;
        push_field_f64(out, &mut first, "mw", self.mw);
        if let Some(v) = self.mvar {
            push_field_f64(out, &mut first, "mvar", v);
        }

        // timestamp (nanos)
        out.push(' ');
        out.push_str(&ts_to_unix_nanos(self.ts).to_string());
    }
}

pub struct QuestDbIlpSink<T> {
    addr: SocketAddr,
    batch_size: usize,
    max_retries: u32,
    retry_backoff: Duration,
    _marker: PhantomData<fn() -> T>,
}

impl<T> QuestDbIlpSink<T> {
    pub fn new(addr: SocketAddr, batch_size: usize, max_retries: u32, retry_backoff: Duration) -> Self {
        Self {
            addr,
            batch_size,
            max_retries,
            retry_backoff,
            _marker: PhantomData,
        }
    }

    async fn connect(&self) -> Result<TcpStream, PipelineError> {
        let stream = TcpStream::connect(self.addr)
            .await
            .map_err(|e| PipelineError::Sink(format!("failed to connect to QuestDB ILP: {e}")))?;
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }
}

impl<T> QuestDbIlpSink<T>
where
    T: IlpEncode,
{
    fn encode_batch(&self, batch: &[Envelope<T>]) -> Vec<u8> {
        // Heuristic capacity: ~160 bytes per line.
        let mut s = String::with_capacity(batch.len().saturating_mul(160));
        for env in batch {
            env.payload.write_ilp_line(&mut s);
            s.push('\n');
        }
        s.into_bytes()
    }

    async fn flush_batch(&self, stream: &mut TcpStream, batch: &[Envelope<T>]) -> Result<(), PipelineError> {
        if batch.is_empty() {
            return Ok(());
        }

        let payload = self.encode_batch(batch);

        let mut attempt: u32 = 0;
        loop {
            match stream.write_all(&payload).await {
                Ok(()) => {
                    metrics::counter!("questdb_ingested_records_total").increment(batch.len() as u64);
                    metrics::counter!("questdb_ilp_bytes_total").increment(payload.len() as u64);

                    if let Some(min_received) = batch.iter().map(|e| e.received_at).min() {
                        if let Ok(dur) = SystemTime::now().duration_since(min_received) {
                            metrics::histogram!("ingest_end_to_end_latency_seconds").record(dur.as_secs_f64());
                        }
                    }

                    return Ok(());
                }
                Err(e) if attempt < self.max_retries => {
                    attempt += 1;
                    let sleep_for = self.retry_backoff * attempt;
                    tracing::warn!(
                        error = %e,
                        attempt,
                        "QuestDB ILP flush failed, reconnecting and retrying"
                    );
                    metrics::counter!("questdb_ilp_retry_total").increment(1);

                    tokio::time::sleep(sleep_for).await;
                    *stream = self.connect().await?;
                }
                Err(e) => {
                    tracing::error!(error = %e, "QuestDB ILP flush failed, giving up");
                    metrics::counter!("questdb_ilp_sink_errors_total").increment(1);
                    return Err(PipelineError::Sink(format!("ilp write failed: {e}")));
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl<T> Sink<T> for QuestDbIlpSink<T>
where
    T: IlpEncode + Send + Sync + 'static,
{
    async fn run<S>(&self, mut input: S) -> Result<(), PipelineError>
    where
        S: futures::Stream<Item = Result<Envelope<T>, PipelineError>> + Send + Unpin + 'static,
    {
        let mut stream = self.connect().await?;
        let mut buffer: Vec<Envelope<T>> = Vec::with_capacity(self.batch_size);

        while let Some(item) = input.next().await {
            let env = match item {
                Ok(env) => env,
                Err(e) => {
                    tracing::error!(error = %e, "error in upstream pipeline for QuestDbIlpSink");
                    continue;
                }
            };

            buffer.push(env);
            if buffer.len() >= self.batch_size {
                self.flush_batch(&mut stream, &buffer).await?;
                buffer.clear();
            }
        }

        if !buffer.is_empty() {
            self.flush_batch(&mut stream, &buffer).await?;
        }

        // Best-effort flush.
        let _ = stream.shutdown().await;

        Ok(())
    }
}

trait ShardKey {
    fn shard_key(&self) -> &str;
}

impl ShardKey for MeterUsage {
    fn shard_key(&self) -> &str {
        &self.meter_id
    }
}

impl ShardKey for GenerationOutput {
    fn shard_key(&self) -> &str {
        &self.plant_id
    }
}

fn shard_index(key: &str, workers: usize) -> usize {
    use std::hash::{Hash, Hasher};

    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % workers.max(1)
}

pub struct QuestDbIlpParallelSink<T> {
    addr: SocketAddr,
    batch_size: usize,
    max_retries: u32,
    retry_backoff: Duration,
    workers: usize,
    _marker: PhantomData<fn() -> T>,
}

impl<T> QuestDbIlpParallelSink<T> {
    pub fn new(
        addr: SocketAddr,
        batch_size: usize,
        max_retries: u32,
        retry_backoff: Duration,
        workers: usize,
    ) -> Self {
        Self {
            addr,
            batch_size,
            max_retries,
            retry_backoff,
            workers: workers.max(1),
            _marker: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<T> Sink<T> for QuestDbIlpParallelSink<T>
where
    T: IlpEncode + ShardKey + Send + Sync + 'static,
{
    async fn run<S>(&self, mut input: S) -> Result<(), PipelineError>
    where
        S: futures::Stream<Item = Result<Envelope<T>, PipelineError>> + Send + Unpin + 'static,
    {
        let mut txs = Vec::with_capacity(self.workers);
        let mut joins = Vec::with_capacity(self.workers);

        for _ in 0..self.workers {
            let (tx, rx) = tokio::sync::mpsc::channel::<Envelope<T>>(self.batch_size.saturating_mul(2));
            txs.push(tx);

            let sink = QuestDbIlpSink::<T>::new(self.addr, self.batch_size, self.max_retries, self.retry_backoff);
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok);

            joins.push(tokio::spawn(async move { sink.run(stream).await }));
        }

        while let Some(item) = input.next().await {
            let env = match item {
                Ok(env) => env,
                Err(e) => {
                    tracing::error!(error = %e, "error in upstream pipeline for QuestDbIlpParallelSink");
                    continue;
                }
            };

            let idx = shard_index(env.payload.shard_key(), self.workers);
            if let Err(_e) = txs[idx].send(env).await {
                return Err(PipelineError::Sink("ILP worker channel closed".to_string()));
            }
        }

        drop(txs);

        for j in joins {
            match j.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(PipelineError::Sink(format!("ILP worker join error: {e}"))),
            }
        }

        Ok(())
    }
}

pub type QuestDbIlpMeterUsageSink = QuestDbIlpParallelSink<MeterUsage>;
pub type QuestDbIlpGenerationSink = QuestDbIlpParallelSink<GenerationOutput>;

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn ilp_escape_ident_escapes_commas_spaces_and_equals() {
        let mut out = String::new();
        ilp_escape_ident("a b,c=d", &mut out);
        assert_eq!(out, "a\\ b\\,c\\=d");
    }

    #[test]
    fn event_id_is_present_and_deterministic_for_meter_usage() {
        let m = MeterUsage {
            ts: datetime!(2024-01-01 00:00:00 UTC),
            meter_id: "m-1".to_string(),
            premise_id: Some("p-1".to_string()),
            kwh: 1.25,
            kvarh: Some(0.1),
            kva_demand: None,
            quality_flag: None,
            source_system: None,
        };

        let mut a = String::new();
        m.write_ilp_line(&mut a);
        let mut b = String::new();
        m.write_ilp_line(&mut b);

        assert!(a.contains("event_id="));
        assert_eq!(a, b);
    }

    #[test]
    fn meter_usage_ilp_line_includes_required_fields_and_tags() {
        let m = MeterUsage {
            ts: datetime!(2024-01-01 00:00:00 UTC),
            meter_id: "m 1".to_string(),
            premise_id: Some("p,1".to_string()),
            kwh: 1.25,
            kvarh: None,
            kva_demand: Some(2.0),
            quality_flag: Some("ok".to_string()),
            source_system: None,
        };

        let mut line = String::new();
        m.write_ilp_line(&mut line);

        assert!(line.starts_with("meter_usage,"));
        assert!(line.contains("meter_id=m\\ 1"));
        assert!(line.contains("premise_id=p\\,1"));
        assert!(line.contains("quality_flag=ok"));
        assert!(line.contains(" kwh=1.25"));
        assert!(line.contains(",kva_demand=2"));

        // Timestamp should be nanos.
        let ts_nanos = ts_to_unix_nanos(m.ts).to_string();
        assert!(line.ends_with(&ts_nanos));
    }

    #[test]
    fn generation_output_ilp_line_omits_missing_optional_tags_and_fields() {
        let g = GenerationOutput {
            ts: datetime!(2024-01-01 00:00:00 UTC),
            plant_id: "plant".to_string(),
            unit_id: None,
            mw: 10.0,
            mvar: None,
            status: None,
            fuel_type: Some("gas".to_string()),
        };

        let mut line = String::new();
        g.write_ilp_line(&mut line);

        assert!(line.starts_with("generation_output,"));
        assert!(line.contains("plant_id=plant"));
        assert!(!line.contains("unit_id="));
        assert!(!line.contains("status="));
        assert!(line.contains("fuel_type=gas"));
        assert!(line.contains(" mw=10"));
        assert!(!line.contains("mvar="));
    }
}
