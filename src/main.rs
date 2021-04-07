use std::{sync::Arc, time::Duration};

use futures_util::{pin_mut, stream, StreamExt};
use tracing::{info, info_span, Instrument as _, Level};
use tracing_subscriber::{filter::EnvFilter, filter::LevelFilter, fmt::Subscriber};

use mqtt_publication_storage::*;

#[tokio::main]
async fn main() {
    let log_level = EnvFilter::from_default_env().add_directive(LevelFilter::DEBUG.into());

    let subscriber = Subscriber::builder()
        .with_max_level(Level::TRACE)
        .with_env_filter(log_level)
        .with_target(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let store = Arc::new(MemoryStore::new());

    let sender = store.clone();
    tokio::spawn(async move {
        let span = info_span!("ingress");
        async {
            for id in 0..5 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                sender.push(Publication { id });
            }
        }
        .instrument(span)
        .await
    });

    let egress1 = tokio::spawn(egress(1, store.clone()));
    let egress2 = tokio::spawn(egress(2, store));

    egress1.await.unwrap();
    egress2.await.unwrap();
}

#[tracing::instrument(skip(store))]
async fn egress(id: usize, store: Arc<MemoryStore<Publication>>) {
    loop {
        let mut batch = store.batch(10).await.unwrap();

        let publications = stream::iter(batch.take_items())
            .filter_map(|publication| async { Some(process(publication)) })
            .buffered(2);

        pin_mut!(publications);

        while publications.next().await.is_some() {}

        store.commit(batch);
    }
}

async fn process(publication: Publication) {
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("processed: {}", publication.id);
}
