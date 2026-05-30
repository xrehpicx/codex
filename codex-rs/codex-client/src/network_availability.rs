use std::io;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::time::Duration;

use tokio::time::sleep;

const NETWORK_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkAvailability {
    Available,
    Unavailable,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkAvailabilityWait {
    pub availability: NetworkAvailability,
    pub waited: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteProbe {
    Available,
    Unavailable,
    Unknown,
}

pub fn current_network_availability() -> NetworkAvailability {
    availability_from_route_probes(&[
        probe_udp_route(
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
            SocketAddr::from(([1, 1, 1, 1], 443)),
        ),
        probe_udp_route(
            SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
            SocketAddr::from(([0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111], 443)),
        ),
    ])
}

pub async fn wait_for_network_availability() -> NetworkAvailabilityWait {
    wait_for_network_availability_with(current_network_availability, NETWORK_POLL_INTERVAL).await
}

async fn wait_for_network_availability_with(
    mut current_availability: impl FnMut() -> NetworkAvailability,
    poll_interval: Duration,
) -> NetworkAvailabilityWait {
    let mut waited = false;
    loop {
        let availability = current_availability();
        match availability {
            NetworkAvailability::Available | NetworkAvailability::Unknown => {
                return NetworkAvailabilityWait {
                    availability,
                    waited,
                };
            }
            NetworkAvailability::Unavailable => {
                waited = true;
                sleep(poll_interval).await;
            }
        }
    }
}

fn probe_udp_route(bind_addr: SocketAddr, target_addr: SocketAddr) -> RouteProbe {
    let socket = match UdpSocket::bind(bind_addr) {
        Ok(socket) => socket,
        Err(err) if is_unavailable_route_error(&err) => return RouteProbe::Unavailable,
        Err(_) => return RouteProbe::Unknown,
    };

    match socket.connect(target_addr) {
        Ok(()) => match socket.local_addr() {
            Ok(local_addr) if !local_addr.ip().is_unspecified() => RouteProbe::Available,
            Ok(_) | Err(_) => RouteProbe::Unknown,
        },
        Err(err) if is_unavailable_route_error(&err) => RouteProbe::Unavailable,
        Err(_) => RouteProbe::Unknown,
    }
}

fn availability_from_route_probes(probes: &[RouteProbe]) -> NetworkAvailability {
    if probes.contains(&RouteProbe::Available) {
        return NetworkAvailability::Available;
    }
    if probes.contains(&RouteProbe::Unknown) {
        return NetworkAvailability::Unknown;
    }
    NetworkAvailability::Unavailable
}

fn is_unavailable_route_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    #[test]
    fn availability_prefers_available_route() {
        assert_eq!(
            availability_from_route_probes(&[RouteProbe::Unavailable, RouteProbe::Available]),
            NetworkAvailability::Available
        );
    }

    #[test]
    fn availability_treats_unknown_as_non_blocking() {
        assert_eq!(
            availability_from_route_probes(&[RouteProbe::Unavailable, RouteProbe::Unknown]),
            NetworkAvailability::Unknown
        );
    }

    #[test]
    fn availability_is_unavailable_only_when_all_routes_are_unavailable() {
        assert_eq!(
            availability_from_route_probes(&[RouteProbe::Unavailable, RouteProbe::Unavailable]),
            NetworkAvailability::Unavailable
        );
    }

    #[tokio::test]
    async fn wait_for_network_availability_returns_unknown_without_waiting() {
        let outcome = wait_for_network_availability_with(
            || NetworkAvailability::Unknown,
            Duration::from_millis(1),
        )
        .await;

        assert_eq!(
            outcome,
            NetworkAvailabilityWait {
                availability: NetworkAvailability::Unknown,
                waited: false,
            }
        );
    }

    #[tokio::test]
    async fn wait_for_network_availability_waits_until_available() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_probe = Arc::clone(&calls);

        let outcome = wait_for_network_availability_with(
            move || {
                if calls_for_probe.fetch_add(1, Ordering::SeqCst) < 2 {
                    NetworkAvailability::Unavailable
                } else {
                    NetworkAvailability::Available
                }
            },
            Duration::from_millis(1),
        )
        .await;

        assert_eq!(
            outcome,
            NetworkAvailabilityWait {
                availability: NetworkAvailability::Available,
                waited: true,
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
