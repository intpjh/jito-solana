//! The `net_utils` module assists with networking
#![allow(clippy::arithmetic_side_effects)]
#[cfg(feature = "dev-context-only-utils")]
use tokio::net::UdpSocket as TokioUdpSocket;
use {
    crossbeam_channel::unbounded,
    log::*,
    rand::{thread_rng, Rng},
    socket2::{Domain, SockAddr, Socket, Type},
    std::{
        collections::{BTreeMap, HashSet},
        io::{self, Read, Write},
        net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket},
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
    url::Url,
};

mod ip_echo_server;
pub use ip_echo_server::{
    ip_echo_server, IpEchoServer, DEFAULT_IP_ECHO_SERVER_THREADS, MAX_PORT_COUNT_PER_MESSAGE,
    MINIMUM_IP_ECHO_SERVER_THREADS,
};
use ip_echo_server::{IpEchoServerMessage, IpEchoServerResponse};

/// A data type representing a public Udp socket
pub struct UdpSocketPair {
    pub addr: SocketAddr,    // Public address of the socket
    pub receiver: UdpSocket, // Locally bound socket that can receive from the public address
    pub sender: UdpSocket,   // Locally bound socket to send via public address
}

pub type PortRange = (u16, u16);

pub const VALIDATOR_PORT_RANGE: PortRange = (8000, 10_000);
pub const MINIMUM_VALIDATOR_PORT_RANGE_WIDTH: u16 = 17; // VALIDATOR_PORT_RANGE must be at least this wide

pub(crate) const HEADER_LENGTH: usize = 4;
pub(crate) const IP_ECHO_SERVER_RESPONSE_LENGTH: usize = HEADER_LENGTH + 23;

fn ip_echo_server_request(
    ip_echo_server_addr: &SocketAddr,
    msg: IpEchoServerMessage,
) -> Result<IpEchoServerResponse, String> {
    let timeout = Duration::new(5, 0);
    TcpStream::connect_timeout(ip_echo_server_addr, timeout)
        .and_then(|mut stream| {
            // Start with HEADER_LENGTH null bytes to avoid looking like an HTTP GET/POST request
            let mut bytes = vec![0; HEADER_LENGTH];

            bytes.append(&mut bincode::serialize(&msg).expect("serialize IpEchoServerMessage"));

            // End with '\n' to make this request look HTTP-ish and tickle an error response back
            // from an HTTP server
            bytes.push(b'\n');

            stream.set_read_timeout(Some(Duration::new(10, 0)))?;
            stream.write_all(&bytes)?;
            stream.shutdown(std::net::Shutdown::Write)?;
            let mut data = vec![0u8; IP_ECHO_SERVER_RESPONSE_LENGTH];
            let _ = stream.read(&mut data[..])?;
            Ok(data)
        })
        .and_then(|data| {
            // It's common for users to accidentally confuse the validator's gossip port and JSON
            // RPC port.  Attempt to detect when this occurs by looking for the standard HTTP
            // response header and provide the user with a helpful error message
            if data.len() < HEADER_LENGTH {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Response too short, received {} bytes", data.len()),
                ));
            }

            let response_header: String =
                data[0..HEADER_LENGTH].iter().map(|b| *b as char).collect();
            if response_header != "\0\0\0\0" {
                if response_header == "HTTP" {
                    let http_response = data.iter().map(|b| *b as char).collect::<String>();
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Invalid gossip entrypoint. {ip_echo_server_addr} looks to be an HTTP port: {http_response}"
                        ),
                    ));
                }
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "Invalid gossip entrypoint. {ip_echo_server_addr} provided an invalid response header: '{response_header}'"
                    ),
                ));
            }

            bincode::deserialize(&data[HEADER_LENGTH..]).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to deserialize: {err:?}"),
                )
            })
        })
        .map_err(|err| err.to_string())
}

/// Determine the public IP address of this machine by asking an ip_echo_server at the given
/// address
pub fn get_public_ip_addr(ip_echo_server_addr: &SocketAddr) -> Result<IpAddr, String> {
    let resp = ip_echo_server_request(ip_echo_server_addr, IpEchoServerMessage::default())?;
    Ok(resp.address)
}

pub fn get_cluster_shred_version(ip_echo_server_addr: &SocketAddr) -> Result<u16, String> {
    let resp = ip_echo_server_request(ip_echo_server_addr, IpEchoServerMessage::default())?;
    resp.shred_version
        .ok_or_else(|| String::from("IP echo server does not return a shred-version"))
}

// Checks if any of the provided TCP/UDP ports are not reachable by the machine at
// `ip_echo_server_addr`
const DEFAULT_TIMEOUT_SECS: u64 = 5;
const DEFAULT_RETRY_COUNT: usize = 5;

fn do_verify_reachable_ports(
    ip_echo_server_addr: &SocketAddr,
    tcp_listeners: Vec<(u16, TcpListener)>,
    udp_sockets: &[&UdpSocket],
    timeout: u64,
    udp_retry_count: usize,
) -> bool {
    info!(
        "Checking that tcp ports {:?} are reachable from {:?}",
        tcp_listeners, ip_echo_server_addr
    );

    let tcp_ports: Vec<_> = tcp_listeners.iter().map(|(port, _)| *port).collect();
    let _ = ip_echo_server_request(
        ip_echo_server_addr,
        IpEchoServerMessage::new(&tcp_ports, &[]),
    )
    .map_err(|err| warn!("ip_echo_server request failed: {}", err));

    let mut ok = true;
    let timeout = Duration::from_secs(timeout);

    // Wait for a connection to open on each TCP port
    for (port, tcp_listener) in tcp_listeners {
        let (sender, receiver) = unbounded();
        let listening_addr = tcp_listener.local_addr().unwrap();
        let thread_handle = std::thread::Builder::new()
            .name(format!("solVrfyTcp{port:05}"))
            .spawn(move || {
                debug!("Waiting for incoming connection on tcp/{}", port);
                match tcp_listener.incoming().next() {
                    Some(_) => sender
                        .send(())
                        .unwrap_or_else(|err| warn!("send failure: {}", err)),
                    None => warn!("tcp incoming failed"),
                }
            })
            .unwrap();
        match receiver.recv_timeout(timeout) {
            Ok(_) => {
                info!("tcp/{} is reachable", port);
            }
            Err(err) => {
                error!(
                    "Received no response at tcp/{}, check your port configuration: {}",
                    port, err
                );
                // Ugh, std rustc doesn't provide accepting with timeout or restoring original
                // nonblocking-status of sockets because of lack of getter, only the setter...
                // So, to close the thread cleanly, just connect from here.
                // ref: https://github.com/rust-lang/rust/issues/31615
                TcpStream::connect_timeout(&listening_addr, timeout).unwrap();
                ok = false;
            }
        }
        // ensure to reap the thread
        thread_handle.join().unwrap();
    }

    if !ok {
        // No retries for TCP, abort on the first failure
        return ok;
    }

    let mut udp_ports: BTreeMap<_, _> = BTreeMap::new();
    udp_sockets.iter().for_each(|udp_socket| {
        let port = udp_socket.local_addr().unwrap().port();
        udp_ports
            .entry(port)
            .or_insert_with(Vec::new)
            .push(udp_socket);
    });
    let udp_ports: Vec<_> = udp_ports.into_iter().collect();

    info!(
        "Checking that udp ports {:?} are reachable from {:?}",
        udp_ports.iter().map(|(port, _)| port).collect::<Vec<_>>(),
        ip_echo_server_addr
    );

    'outer: for checked_ports_and_sockets in udp_ports.chunks(MAX_PORT_COUNT_PER_MESSAGE) {
        ok = false;

        for udp_remaining_retry in (0_usize..udp_retry_count).rev() {
            let (checked_ports, checked_socket_iter) = (
                checked_ports_and_sockets
                    .iter()
                    .map(|(port, _)| *port)
                    .collect::<Vec<_>>(),
                checked_ports_and_sockets
                    .iter()
                    .flat_map(|(_, sockets)| sockets),
            );

            let _ = ip_echo_server_request(
                ip_echo_server_addr,
                IpEchoServerMessage::new(&[], &checked_ports),
            )
            .map_err(|err| warn!("ip_echo_server request failed: {}", err));

            // Spawn threads at once!
            let reachable_ports = Arc::new(RwLock::new(HashSet::new()));
            let thread_handles: Vec<_> = checked_socket_iter
                .map(|udp_socket| {
                    let port = udp_socket.local_addr().unwrap().port();
                    let udp_socket = udp_socket.try_clone().expect("Unable to clone udp socket");
                    let reachable_ports = reachable_ports.clone();

                    std::thread::Builder::new()
                        .name(format!("solVrfyUdp{port:05}"))
                        .spawn(move || {
                            let start = Instant::now();

                            let original_read_timeout = udp_socket.read_timeout().unwrap();
                            udp_socket
                                .set_read_timeout(Some(Duration::from_millis(250)))
                                .unwrap();
                            loop {
                                if reachable_ports.read().unwrap().contains(&port)
                                    || Instant::now().duration_since(start) >= timeout
                                {
                                    break;
                                }

                                let recv_result = udp_socket.recv(&mut [0; 1]);
                                debug!(
                                    "Waited for incoming datagram on udp/{}: {:?}",
                                    port, recv_result
                                );

                                if recv_result.is_ok() {
                                    reachable_ports.write().unwrap().insert(port);
                                    break;
                                }
                            }
                            udp_socket.set_read_timeout(original_read_timeout).unwrap();
                        })
                        .unwrap()
                })
                .collect();

            // Now join threads!
            // Separate from the above by collect()-ing as an intermediately step to make the iterator
            // eager not lazy so that joining happens here at once after creating bunch of threads
            // at once.
            for thread in thread_handles {
                thread.join().unwrap();
            }

            let reachable_ports = reachable_ports.read().unwrap().clone();
            if reachable_ports.len() == checked_ports.len() {
                info!(
                    "checked udp ports: {:?}, reachable udp ports: {:?}",
                    checked_ports, reachable_ports
                );
                ok = true;
                break;
            } else if udp_remaining_retry > 0 {
                // Might have lost a UDP packet, retry a couple times
                error!(
                    "checked udp ports: {:?}, reachable udp ports: {:?}",
                    checked_ports, reachable_ports
                );
                error!("There are some udp ports with no response!! Retrying...");
            } else {
                error!("Maximum retry count is reached....");
                break 'outer;
            }
        }
    }

    ok
}

pub fn verify_reachable_ports(
    ip_echo_server_addr: &SocketAddr,
    tcp_listeners: Vec<(u16, TcpListener)>,
    udp_sockets: &[&UdpSocket],
) -> bool {
    do_verify_reachable_ports(
        ip_echo_server_addr,
        tcp_listeners,
        udp_sockets,
        DEFAULT_TIMEOUT_SECS,
        DEFAULT_RETRY_COUNT,
    )
}

pub fn parse_port_or_addr(optstr: Option<&str>, default_addr: SocketAddr) -> SocketAddr {
    if let Some(addrstr) = optstr {
        if let Ok(port) = addrstr.parse() {
            let mut addr = default_addr;
            addr.set_port(port);
            addr
        } else if let Ok(addr) = addrstr.parse() {
            addr
        } else {
            default_addr
        }
    } else {
        default_addr
    }
}

pub fn parse_port_range(port_range: &str) -> Option<PortRange> {
    let ports: Vec<&str> = port_range.split('-').collect();
    if ports.len() != 2 {
        return None;
    }

    let start_port = ports[0].parse();
    let end_port = ports[1].parse();

    if start_port.is_err() || end_port.is_err() {
        return None;
    }
    let start_port = start_port.unwrap();
    let end_port = end_port.unwrap();
    if end_port < start_port {
        return None;
    }
    Some((start_port, end_port))
}

pub fn parse_host(host: &str) -> Result<IpAddr, String> {
    // First, check if the host syntax is valid. This check is needed because addresses
    // such as `("localhost:1234", 0)` will resolve to IPs on some networks.
    let parsed_url = Url::parse(&format!("http://{host}")).map_err(|e| e.to_string())?;
    if parsed_url.port().is_some() {
        return Err(format!("Expected port in URL: {host}"));
    }

    // Next, check to see if it resolves to an IP address
    let ips: Vec<_> = (host, 0)
        .to_socket_addrs()
        .map_err(|err| err.to_string())?
        .map(|socket_address| socket_address.ip())
        .collect();
    if ips.is_empty() {
        Err(format!("Unable to resolve host: {host}"))
    } else {
        Ok(ips[0])
    }
}

pub fn is_host(string: String) -> Result<(), String> {
    parse_host(&string).map(|_| ())
}

pub fn parse_host_port(host_port: &str) -> Result<SocketAddr, String> {
    let addrs: Vec<_> = host_port
        .to_socket_addrs()
        .map_err(|err| format!("Unable to resolve host {host_port}: {err}"))?
        .collect();
    if addrs.is_empty() {
        Err(format!("Unable to resolve host: {host_port}"))
    } else {
        Ok(addrs[0])
    }
}

pub fn is_host_port(string: String) -> Result<(), String> {
    parse_host_port(&string).map(|_| ())
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SocketConfig {
    reuseport: bool,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
}

impl SocketConfig {
    pub fn reuseport(mut self, reuseport: bool) -> Self {
        self.reuseport = reuseport;
        self
    }

    /// Sets the receive buffer size for the socket (no effect on windows/ios).
    ///
    /// **Note:** On Linux the kernel will double the value you specify.
    /// For example, if you specify `16MB`, the kernel will configure the
    /// socket to use `32MB`.
    /// See: https://man7.org/linux/man-pages/man7/socket.7.html: SO_RCVBUF
    pub fn recv_buffer_size(mut self, size: usize) -> Self {
        self.recv_buffer_size = Some(size);
        self
    }

    /// Sets the send buffer size for the socket (no effect on windows/ios)
    ///
    /// **Note:** On Linux the kernel will double the value you specify.
    /// For example, if you specify `16MB`, the kernel will configure the
    /// socket to use `32MB`.
    /// See: https://man7.org/linux/man-pages/man7/socket.7.html: SO_SNDBUF
    pub fn send_buffer_size(mut self, size: usize) -> Self {
        self.send_buffer_size = Some(size);
        self
    }
}

#[cfg(any(windows, target_os = "ios"))]
fn udp_socket_with_config(_config: SocketConfig) -> io::Result<Socket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    Ok(sock)
}

#[cfg(not(any(windows, target_os = "ios")))]
fn udp_socket_with_config(config: SocketConfig) -> io::Result<Socket> {
    use nix::sys::socket::{setsockopt, sockopt::ReusePort};
    let SocketConfig {
        reuseport,
        recv_buffer_size,
        send_buffer_size,
    } = config;

    let sock = Socket::new(Domain::IPV4, Type::DGRAM, None)?;

    // Set buffer sizes
    if let Some(recv_buffer_size) = recv_buffer_size {
        sock.set_recv_buffer_size(recv_buffer_size)?;
    }

    if let Some(send_buffer_size) = send_buffer_size {
        sock.set_send_buffer_size(send_buffer_size)?;
    }

    if reuseport {
        setsockopt(&sock, ReusePort, &true).ok();
    }

    Ok(sock)
}

// Find a port in the given range with a socket config that is available for both TCP and UDP
pub fn bind_common_in_range_with_config(
    ip_addr: IpAddr,
    range: PortRange,
    config: SocketConfig,
) -> io::Result<(u16, (UdpSocket, TcpListener))> {
    for port in range.0..range.1 {
        if let Ok((sock, listener)) = bind_common_with_config(ip_addr, port, config) {
            return Result::Ok((sock.local_addr().unwrap().port(), (sock, listener)));
        }
    }

    Err(io::Error::new(
        io::ErrorKind::Other,
        format!("No available TCP/UDP ports in {range:?}"),
    ))
}

// Find a port in the given range that is available for both TCP and UDP
#[deprecated(
    since = "2.2.0",
    note = "use `bind_common_in_range_with_config` instead"
)]
pub fn bind_common_in_range(
    ip_addr: IpAddr,
    range: PortRange,
) -> io::Result<(u16, (UdpSocket, TcpListener))> {
    bind_common_in_range_with_config(ip_addr, range, SocketConfig::default())
}

pub fn bind_in_range(ip_addr: IpAddr, range: PortRange) -> io::Result<(u16, UdpSocket)> {
    let config = SocketConfig::default();
    bind_in_range_with_config(ip_addr, range, config)
}

pub fn bind_in_range_with_config(
    ip_addr: IpAddr,
    range: PortRange,
    config: SocketConfig,
) -> io::Result<(u16, UdpSocket)> {
    let sock = udp_socket_with_config(config)?;

    for port in range.0..range.1 {
        let addr = SocketAddr::new(ip_addr, port);

        if sock.bind(&SockAddr::from(addr)).is_ok() {
            let sock: UdpSocket = sock.into();
            return Result::Ok((sock.local_addr().unwrap().port(), sock));
        }
    }

    Err(io::Error::new(
        io::ErrorKind::Other,
        format!("No available UDP ports in {range:?}"),
    ))
}

pub fn bind_with_any_port_with_config(
    ip_addr: IpAddr,
    config: SocketConfig,
) -> io::Result<UdpSocket> {
    let sock = udp_socket_with_config(config)?;
    let addr = SocketAddr::new(ip_addr, 0);
    match sock.bind(&SockAddr::from(addr)) {
        Ok(_) => Result::Ok(sock.into()),
        Err(err) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("No available UDP port: {err}"),
        )),
    }
}

#[deprecated(since = "2.2.0", note = "use `bind_with_any_port_with_config` instead")]
pub fn bind_with_any_port(ip_addr: IpAddr) -> io::Result<UdpSocket> {
    bind_with_any_port_with_config(ip_addr, SocketConfig::default())
}

// binds many sockets to the same port in a range with config
pub fn multi_bind_in_range_with_config(
    ip_addr: IpAddr,
    range: PortRange,
    config: SocketConfig,
    mut num: usize,
) -> io::Result<(u16, Vec<UdpSocket>)> {
    if !config.reuseport {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "SocketConfig.reuseport must be true for multi_bind_in_range_with_config",
        ));
    }
    if cfg!(windows) && num != 1 {
        // See https://github.com/solana-labs/solana/issues/4607
        warn!(
            "multi_bind_in_range_with_config() only supports 1 socket in windows ({} requested)",
            num
        );
        num = 1;
    }
    let mut sockets = Vec::with_capacity(num);

    const NUM_TRIES: usize = 100;
    let mut port = 0;
    let mut error = None;
    for _ in 0..NUM_TRIES {
        port = {
            let (port, _) = bind_in_range(ip_addr, range)?;
            port
        }; // drop the probe, port should be available... briefly.

        for _ in 0..num {
            let sock = bind_to_with_config(ip_addr, port, config);
            if let Ok(sock) = sock {
                sockets.push(sock);
            } else {
                error = Some(sock);
                break;
            }
        }
        if sockets.len() == num {
            break;
        } else {
            sockets.clear();
        }
    }
    if sockets.len() != num {
        error.unwrap()?;
    }
    Ok((port, sockets))
}

// binds many sockets to the same port in a range
// Note: The `mut` modifier for `num` is unused but kept for compatibility with the public API.
#[deprecated(
    since = "2.2.0",
    note = "use `multi_bind_in_range_with_config` instead"
)]
#[allow(unused_mut)]
pub fn multi_bind_in_range(
    ip_addr: IpAddr,
    range: PortRange,
    mut num: usize,
) -> io::Result<(u16, Vec<UdpSocket>)> {
    let config = SocketConfig::default().reuseport(true);
    multi_bind_in_range_with_config(ip_addr, range, config, num)
}

pub fn bind_to(ip_addr: IpAddr, port: u16, reuseport: bool) -> io::Result<UdpSocket> {
    let config = SocketConfig::default().reuseport(reuseport);
    bind_to_with_config(ip_addr, port, config)
}

#[cfg(feature = "dev-context-only-utils")]
pub async fn bind_to_async(
    ip_addr: IpAddr,
    port: u16,
    reuseport: bool,
) -> io::Result<TokioUdpSocket> {
    let config = SocketConfig::default().reuseport(reuseport);
    let socket = bind_to_with_config_non_blocking(ip_addr, port, config)?;
    TokioUdpSocket::from_std(socket)
}

pub fn bind_to_localhost() -> io::Result<UdpSocket> {
    bind_to(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        /*port:*/ 0,
        /*reuseport:*/ false,
    )
}

#[cfg(feature = "dev-context-only-utils")]
pub async fn bind_to_localhost_async() -> io::Result<TokioUdpSocket> {
    bind_to_async(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        /*port:*/ 0,
        /*reuseport:*/ false,
    )
    .await
}

pub fn bind_to_unspecified() -> io::Result<UdpSocket> {
    bind_to(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        /*port:*/ 0,
        /*reuseport:*/ false,
    )
}

#[cfg(feature = "dev-context-only-utils")]
pub async fn bind_to_unspecified_async() -> io::Result<TokioUdpSocket> {
    bind_to_async(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        /*port:*/ 0,
        /*reuseport:*/ false,
    )
    .await
}

pub fn bind_to_with_config(
    ip_addr: IpAddr,
    port: u16,
    config: SocketConfig,
) -> io::Result<UdpSocket> {
    let sock = udp_socket_with_config(config)?;

    let addr = SocketAddr::new(ip_addr, port);

    sock.bind(&SockAddr::from(addr)).map(|_| sock.into())
}

pub fn bind_to_with_config_non_blocking(
    ip_addr: IpAddr,
    port: u16,
    config: SocketConfig,
) -> io::Result<UdpSocket> {
    let sock = udp_socket_with_config(config)?;

    let addr = SocketAddr::new(ip_addr, port);

    sock.bind(&SockAddr::from(addr))?;
    sock.set_nonblocking(true)?;
    Ok(sock.into())
}

// binds both a UdpSocket and a TcpListener
pub fn bind_common(ip_addr: IpAddr, port: u16) -> io::Result<(UdpSocket, TcpListener)> {
    let config = SocketConfig::default();
    bind_common_with_config(ip_addr, port, config)
}

// binds both a UdpSocket and a TcpListener
pub fn bind_common_with_config(
    ip_addr: IpAddr,
    port: u16,
    config: SocketConfig,
) -> io::Result<(UdpSocket, TcpListener)> {
    let sock = udp_socket_with_config(config)?;

    let addr = SocketAddr::new(ip_addr, port);
    let sock_addr = SockAddr::from(addr);
    sock.bind(&sock_addr)
        .and_then(|_| TcpListener::bind(addr).map(|listener| (sock.into(), listener)))
}

pub fn bind_two_in_range_with_offset(
    ip_addr: IpAddr,
    range: PortRange,
    offset: u16,
) -> io::Result<((u16, UdpSocket), (u16, UdpSocket))> {
    let sock1_config = SocketConfig::default();
    let sock2_config = SocketConfig::default();
    bind_two_in_range_with_offset_and_config(ip_addr, range, offset, sock1_config, sock2_config)
}

pub fn bind_two_in_range_with_offset_and_config(
    ip_addr: IpAddr,
    range: PortRange,
    offset: u16,
    sock1_config: SocketConfig,
    sock2_config: SocketConfig,
) -> io::Result<((u16, UdpSocket), (u16, UdpSocket))> {
    if range.1.saturating_sub(range.0) < offset {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "range too small to find two ports with the correct offset".to_string(),
        ));
    }
    for port in range.0..range.1 {
        if let Ok(first_bind) = bind_to_with_config(ip_addr, port, sock1_config) {
            if range.1.saturating_sub(port) >= offset {
                if let Ok(second_bind) = bind_to_with_config(ip_addr, port + offset, sock2_config) {
                    return Ok((
                        (first_bind.local_addr().unwrap().port(), first_bind),
                        (second_bind.local_addr().unwrap().port(), second_bind),
                    ));
                }
            } else {
                break;
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::Other,
        "couldn't find two ports with the correct offset in range".to_string(),
    ))
}

pub fn find_available_port_in_range(ip_addr: IpAddr, range: PortRange) -> io::Result<u16> {
    let (start, end) = range;
    let mut tries_left = end - start;
    let mut rand_port = thread_rng().gen_range(start..end);
    loop {
        match bind_common(ip_addr, rand_port) {
            Ok(_) => {
                break Ok(rand_port);
            }
            Err(err) => {
                if tries_left == 0 {
                    return Err(err);
                }
            }
        }
        rand_port += 1;
        if rand_port == end {
            rand_port = start;
        }
        tries_left -= 1;
    }
}

pub fn bind_more_with_config(
    socket: UdpSocket,
    num: usize,
    config: SocketConfig,
) -> io::Result<Vec<UdpSocket>> {
    let addr = socket.local_addr().unwrap();
    let ip = addr.ip();
    let port = addr.port();
    std::iter::once(Ok(socket))
        .chain((1..num).map(|_| bind_to_with_config(ip, port, config)))
        .collect()
}

#[cfg(test)]
mod tests {
    use {super::*, std::net::Ipv4Addr};

    #[test]
    fn test_response_length() {
        let resp = IpEchoServerResponse {
            address: IpAddr::from([u16::MAX; 8]), // IPv6 variant
            shred_version: Some(u16::MAX),
        };
        let resp_size = bincode::serialized_size(&resp).unwrap();
        assert_eq!(
            IP_ECHO_SERVER_RESPONSE_LENGTH,
            HEADER_LENGTH + resp_size as usize
        );
    }

    // Asserts that an old client can parse the response from a new server.
    #[test]
    fn test_backward_compat() {
        let address = IpAddr::from([
            525u16, 524u16, 523u16, 522u16, 521u16, 520u16, 519u16, 518u16,
        ]);
        let response = IpEchoServerResponse {
            address,
            shred_version: Some(42),
        };
        let mut data = vec![0u8; IP_ECHO_SERVER_RESPONSE_LENGTH];
        bincode::serialize_into(&mut data[HEADER_LENGTH..], &response).unwrap();
        data.truncate(HEADER_LENGTH + 20);
        assert_eq!(
            bincode::deserialize::<IpAddr>(&data[HEADER_LENGTH..]).unwrap(),
            address
        );
    }

    // Asserts that a new client can parse the response from an old server.
    #[test]
    fn test_forward_compat() {
        let address = IpAddr::from([
            525u16, 524u16, 523u16, 522u16, 521u16, 520u16, 519u16, 518u16,
        ]);
        let mut data = [0u8; IP_ECHO_SERVER_RESPONSE_LENGTH];
        bincode::serialize_into(&mut data[HEADER_LENGTH..], &address).unwrap();
        let response: Result<IpEchoServerResponse, _> =
            bincode::deserialize(&data[HEADER_LENGTH..]);
        assert_eq!(
            response.unwrap(),
            IpEchoServerResponse {
                address,
                shred_version: None,
            }
        );
    }

    #[test]
    fn test_parse_port_or_addr() {
        let p1 = parse_port_or_addr(Some("9000"), SocketAddr::from(([1, 2, 3, 4], 1)));
        assert_eq!(p1.port(), 9000);
        let p2 = parse_port_or_addr(Some("127.0.0.1:7000"), SocketAddr::from(([1, 2, 3, 4], 1)));
        assert_eq!(p2.port(), 7000);
        let p2 = parse_port_or_addr(Some("hi there"), SocketAddr::from(([1, 2, 3, 4], 1)));
        assert_eq!(p2.port(), 1);
        let p3 = parse_port_or_addr(None, SocketAddr::from(([1, 2, 3, 4], 1)));
        assert_eq!(p3.port(), 1);
    }

    #[test]
    fn test_parse_port_range() {
        assert_eq!(parse_port_range("garbage"), None);
        assert_eq!(parse_port_range("1-"), None);
        assert_eq!(parse_port_range("1-2"), Some((1, 2)));
        assert_eq!(parse_port_range("1-2-3"), None);
        assert_eq!(parse_port_range("2-1"), None);
    }

    #[test]
    fn test_parse_host() {
        parse_host("localhost:1234").unwrap_err();
        parse_host("localhost").unwrap();
        parse_host("127.0.0.0:1234").unwrap_err();
        parse_host("127.0.0.0").unwrap();
    }

    #[test]
    fn test_parse_host_port() {
        parse_host_port("localhost:1234").unwrap();
        parse_host_port("localhost").unwrap_err();
        parse_host_port("127.0.0.0:1234").unwrap();
        parse_host_port("127.0.0.0").unwrap_err();
    }

    #[test]
    fn test_is_host_port() {
        assert!(is_host_port("localhost:1234".to_string()).is_ok());
        assert!(is_host_port("localhost".to_string()).is_err());
    }

    #[test]
    fn test_bind() {
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        assert_eq!(bind_in_range(ip_addr, (2000, 2001)).unwrap().0, 2000);
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let config = SocketConfig::default().reuseport(true);
        let x = bind_to_with_config(ip_addr, 2002, config).unwrap();
        let y = bind_to_with_config(ip_addr, 2002, config).unwrap();
        assert_eq!(
            x.local_addr().unwrap().port(),
            y.local_addr().unwrap().port()
        );
        bind_to(ip_addr, 2002, false).unwrap_err();
        bind_in_range(ip_addr, (2002, 2003)).unwrap_err();

        let (port, v) = multi_bind_in_range_with_config(ip_addr, (2010, 2110), config, 10).unwrap();
        for sock in &v {
            assert_eq!(port, sock.local_addr().unwrap().port());
        }
    }

    #[test]
    fn test_bind_with_any_port() {
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let config = SocketConfig::default();
        let x = bind_with_any_port_with_config(ip_addr, config).unwrap();
        let y = bind_with_any_port_with_config(ip_addr, config).unwrap();
        assert_ne!(
            x.local_addr().unwrap().port(),
            y.local_addr().unwrap().port()
        );
    }

    #[test]
    fn test_bind_in_range_nil() {
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        bind_in_range(ip_addr, (2000, 2000)).unwrap_err();
        bind_in_range(ip_addr, (2000, 1999)).unwrap_err();
    }

    #[test]
    fn test_find_available_port_in_range() {
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        assert_eq!(
            find_available_port_in_range(ip_addr, (3000, 3001)).unwrap(),
            3000
        );
        let port = find_available_port_in_range(ip_addr, (3000, 3050)).unwrap();
        assert!((3000..3050).contains(&port));

        let _socket = bind_to(ip_addr, port, false).unwrap();
        find_available_port_in_range(ip_addr, (port, port + 1)).unwrap_err();
    }

    #[test]
    fn test_bind_common_in_range() {
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let config = SocketConfig::default();
        let (port, _sockets) =
            bind_common_in_range_with_config(ip_addr, (3100, 3150), config).unwrap();
        assert!((3100..3150).contains(&port));

        bind_common_in_range_with_config(ip_addr, (port, port + 1), config).unwrap_err();
    }

    #[test]
    fn test_get_public_ip_addr_none() {
        solana_logger::setup();
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let config = SocketConfig::default();
        let (_server_port, (server_udp_socket, server_tcp_listener)) =
            bind_common_in_range_with_config(ip_addr, (3200, 3250), config).unwrap();

        let _runtime = ip_echo_server(
            server_tcp_listener,
            DEFAULT_IP_ECHO_SERVER_THREADS,
            /*shred_version=*/ Some(42),
        );

        let server_ip_echo_addr = server_udp_socket.local_addr().unwrap();
        assert_eq!(
            get_public_ip_addr(&server_ip_echo_addr),
            parse_host("127.0.0.1"),
        );
        assert_eq!(get_cluster_shred_version(&server_ip_echo_addr), Ok(42));
        assert!(verify_reachable_ports(&server_ip_echo_addr, vec![], &[],));
    }

    #[test]
    fn test_get_public_ip_addr_reachable() {
        solana_logger::setup();
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let config = SocketConfig::default();
        let (_server_port, (server_udp_socket, server_tcp_listener)) =
            bind_common_in_range_with_config(ip_addr, (3200, 3250), config).unwrap();
        let (client_port, (client_udp_socket, client_tcp_listener)) =
            bind_common_in_range_with_config(ip_addr, (3200, 3250), config).unwrap();

        let _runtime = ip_echo_server(
            server_tcp_listener,
            DEFAULT_IP_ECHO_SERVER_THREADS,
            /*shred_version=*/ Some(65535),
        );

        let ip_echo_server_addr = server_udp_socket.local_addr().unwrap();
        assert_eq!(
            get_public_ip_addr(&ip_echo_server_addr),
            parse_host("127.0.0.1"),
        );
        assert_eq!(get_cluster_shred_version(&ip_echo_server_addr), Ok(65535));
        assert!(verify_reachable_ports(
            &ip_echo_server_addr,
            vec![(client_port, client_tcp_listener)],
            &[&client_udp_socket],
        ));
    }

    #[test]
    fn test_get_public_ip_addr_tcp_unreachable() {
        solana_logger::setup();
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let config = SocketConfig::default();
        let (_server_port, (server_udp_socket, _server_tcp_listener)) =
            bind_common_in_range_with_config(ip_addr, (3200, 3250), config).unwrap();

        // make the socket unreachable by not running the ip echo server!

        let server_ip_echo_addr = server_udp_socket.local_addr().unwrap();

        let (correct_client_port, (_client_udp_socket, client_tcp_listener)) =
            bind_common_in_range_with_config(ip_addr, (3200, 3250), config).unwrap();

        assert!(!do_verify_reachable_ports(
            &server_ip_echo_addr,
            vec![(correct_client_port, client_tcp_listener)],
            &[],
            2,
            3,
        ));
    }

    #[test]
    fn test_get_public_ip_addr_udp_unreachable() {
        solana_logger::setup();
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let config = SocketConfig::default();
        let (_server_port, (server_udp_socket, _server_tcp_listener)) =
            bind_common_in_range_with_config(ip_addr, (3200, 3250), config).unwrap();

        // make the socket unreachable by not running the ip echo server!

        let server_ip_echo_addr = server_udp_socket.local_addr().unwrap();

        let (_correct_client_port, (client_udp_socket, _client_tcp_listener)) =
            bind_common_in_range_with_config(ip_addr, (3200, 3250), config).unwrap();

        assert!(!do_verify_reachable_ports(
            &server_ip_echo_addr,
            vec![],
            &[&client_udp_socket],
            2,
            3,
        ));
    }

    #[test]
    fn test_bind_two_in_range_with_offset() {
        solana_logger::setup();
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let offset = 6;
        if let Ok(((port1, _), (port2, _))) =
            bind_two_in_range_with_offset(ip_addr, (1024, 65535), offset)
        {
            assert!(port2 == port1 + offset);
        }
        let offset = 42;
        if let Ok(((port1, _), (port2, _))) =
            bind_two_in_range_with_offset(ip_addr, (1024, 65535), offset)
        {
            assert!(port2 == port1 + offset);
        }
        assert!(bind_two_in_range_with_offset(ip_addr, (1024, 1044), offset).is_err());
    }

    #[test]
    fn test_multi_bind_in_range_with_config_reuseport_disabled() {
        let ip_addr: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let config = SocketConfig::default(); //reuseport is false by default

        let result = multi_bind_in_range_with_config(ip_addr, (2010, 2110), config, 2);

        assert!(
            result.is_err(),
            "Expected an error when reuseport is not set to true"
        );
    }
}
