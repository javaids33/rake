//! Worker discovery mechanisms for distributed clusters.
//!
//! Supports three discovery methods:
//! - **Static**: workers listed in config (for bare-metal / docker-compose)
//! - **Register**: workers self-register via Flight RPC (default)
//! - **Kubernetes**: DNS SRV-based discovery via headless services

use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use rustlake_core::config::{ClusterConfig, DiscoveryMethod, K8sDiscoveryConfig};

use crate::coordinator::{Coordinator, WorkerRegistration};

/// Discover and register worker nodes based on the configured discovery method.
pub async fn discover_workers(
    coordinator: Arc<Coordinator>,
    cluster_config: &ClusterConfig,
    k8s_config: &K8sDiscoveryConfig,
) {
    match &cluster_config.discovery {
        DiscoveryMethod::Static => {
            tracing::info!("Using static worker discovery from config");
            coordinator.seed_static_workers().await;
        }
        DiscoveryMethod::Register => {
            tracing::info!("Using self-registration worker discovery — workers register via Flight RPC");
            // Workers register themselves; nothing to do here.
        }
        DiscoveryMethod::Kubernetes => {
            tracing::info!(
                namespace = %k8s_config.namespace,
                service = %k8s_config.service_name,
                "Using Kubernetes DNS-based worker discovery"
            );
            discover_k8s_workers(coordinator, k8s_config).await;
        }
    }
}

/// Discover workers via Kubernetes headless service DNS SRV records.
///
/// Resolves `<service>.<namespace>.svc.cluster.local` to get pod IPs,
/// then registers each as a worker with the coordinator.
async fn discover_k8s_workers(
    coordinator: Arc<Coordinator>,
    config: &K8sDiscoveryConfig,
) {
    let dns_name = format!(
        "{}.{}.svc.cluster.local",
        config.service_name, config.namespace
    );

    tracing::info!(dns = %dns_name, "Resolving K8s headless service");

    // DNS resolution (blocking, so run on blocking thread).
    let dns_name_clone = dns_name.clone();
    let _port_name = config.port_name.clone();
    let addresses = tokio::task::spawn_blocking(move || {
        let lookup = format!("{}:0", dns_name_clone);
        match lookup.to_socket_addrs() {
            Ok(addrs) => {
                let ips: Vec<String> = addrs
                    .map(|a| a.ip().to_string())
                    .collect();
                Ok(ips)
            }
            Err(e) => Err(format!("DNS resolution failed for {}: {}", dns_name_clone, e)),
        }
    })
    .await;

    match addresses {
        Ok(Ok(ips)) => {
            if ips.is_empty() {
                tracing::warn!(dns = %dns_name, "No worker IPs resolved from K8s DNS");
                return;
            }

            tracing::info!(
                dns = %dns_name,
                workers = ips.len(),
                "Resolved K8s worker IPs"
            );

            // Default Flight port for workers.
            let flight_port = 50051;

            for ip in &ips {
                let endpoint = format!("http://{}:{}", ip, flight_port);
                let reg = WorkerRegistration {
                    endpoint,
                    label: Some(format!("k8s-{}", ip)),
                    cpu_cores: 0,
                    memory_bytes: 0,
                };
                coordinator.register_worker(reg).await;
            }
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "K8s worker discovery failed");
        }
        Err(e) => {
            tracing::warn!(error = %e, "K8s discovery task panicked");
        }
    }
}

/// Start a background loop that periodically re-discovers K8s workers.
///
/// Useful when pods are added/removed — the coordinator's worker list stays
/// in sync with the current set of pods behind the headless service.
pub async fn k8s_discovery_loop(
    coordinator: Arc<Coordinator>,
    config: K8sDiscoveryConfig,
    cancel: tokio::sync::watch::Receiver<bool>,
) {
    let interval = Duration::from_secs(config.poll_interval_secs);
    let mut ticker = tokio::time::interval(interval);

    loop {
        ticker.tick().await;
        if *cancel.borrow() {
            tracing::info!("K8s discovery loop shutting down");
            break;
        }
        discover_k8s_workers(coordinator.clone(), &config).await;
    }
}
