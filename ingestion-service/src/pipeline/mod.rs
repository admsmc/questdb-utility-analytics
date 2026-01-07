use std::{pin::Pin, sync::Arc, time::SystemTime};

use futures::{Stream, StreamExt};

#[derive(Debug, Clone)]
pub struct Envelope<T> {
    pub payload: T,
    pub received_at: SystemTime,
}

#[derive(thiserror::Error, Debug)]
pub enum PipelineError {
    #[error("source error: {0}")]
    Source(String),
    #[error("transform error: {0}")]
    Transform(String),
    #[error("sink error: {0}")]
    Sink(String),
}

#[async_trait::async_trait]
pub trait Source<T>: Send + Sync {
    async fn stream(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<Envelope<T>, PipelineError>> + Send>>;
}

#[async_trait::async_trait]
pub trait Transform<I, O>: Send + Sync {
    async fn apply(&self, input: Envelope<I>) -> Result<Envelope<O>, PipelineError>;
}

#[async_trait::async_trait]
pub trait Sink<T>: Send + Sync {
    async fn run<S>(&self, input: S) -> Result<(), PipelineError>
    where
        S: Stream<Item = Result<Envelope<T>, PipelineError>> + Send + Unpin + 'static;
}

pub struct Pipeline<S, T, K> {
    pub source: S,
    pub transforms: Vec<Arc<dyn Transform<T, T> + Send + Sync>>, // same-type transforms chain
    pub sink: K,
}

impl<T, S, K> Pipeline<S, T, K>
where
    T: Send + 'static,
    S: Source<T> + Send + Sync + 'static,
    K: Sink<T> + Send + Sync + 'static,
{
    pub async fn run(self) -> Result<(), PipelineError> {
        let mut stream = self.source.stream().await;

        // Apply transforms in sequence (if any).
        for t in self.transforms {
            let t_arc = t.clone();
            stream = Box::pin(stream.then(move |item| {
                let t_inner = t_arc.clone();
                async move {
                    match item {
                        Ok(env) => t_inner.apply(env).await,
                        Err(e) => Err(e),
                    }
                }
            }));
        }

        self.sink.run(stream).await
    }
}
