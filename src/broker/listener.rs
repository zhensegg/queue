//! Socket setup: SO_REUSEPORT listener, per-connection tuning, core affinity.

use std::net::TcpListener;

/// Bind a TCP listener with SO_REUSEPORT + Linux socket tuning.
#[cfg(target_os = "linux")]
pub fn bind_reuse_port(addr: &str) -> std::io::Result<TcpListener> {
    use std::os::unix::io::AsRawFd;
    use socket2::{Domain, SockAddr, Socket, Type};

    let std_addr: std::net::SocketAddr = addr.parse().expect("invalid addr");
    let domain = Domain::for_address(std_addr);
    let socket = Socket::new(domain, Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    // SO_REUSEPORT via libc (socket2 0.5 set_reuse_port may not be available)
    let optval: libc::c_int = 1;
    let ret = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of_val(&optval) as libc::socklen_t,
        )
    };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // buffers on listener (accepted sockets inherit) + busy poll best-effort
    let bufsz: libc::c_int = 1 << 20;
    unsafe {
        let _ = libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let _ = libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let busy: libc::c_int = 50;
        let _ = libc::setsockopt(socket.as_raw_fd(), libc::SOL_SOCKET, libc::SO_BUSY_POLL, &busy as *const _ as *const libc::c_void, std::mem::size_of_val(&busy) as libc::socklen_t);
    }
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(std_addr))?;
    socket.listen(4096)?;
    Ok(socket.into())
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
pub fn bind_reuse_port(addr: &str) -> std::io::Result<TcpListener> {
    let std_addr: std::net::SocketAddr = addr.parse().expect("invalid addr");
    let std_listener = TcpListener::bind(std_addr)?;
    std_listener.set_nonblocking(true)?;
    Ok(std_listener)
}

/// Per-connection socket tuning (NODELAY, QUICKACK, buffers, busy poll).
#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub fn tune_socket(fd: std::os::unix::io::RawFd) {
    unsafe {
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, &one as *const _ as *const libc::c_void, std::mem::size_of_val(&one) as libc::socklen_t);
        let _ = libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_QUICKACK, &one as *const _ as *const libc::c_void, std::mem::size_of_val(&one) as libc::socklen_t);
        let bufsz: libc::c_int = 1 << 20;
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &bufsz as *const _ as *const libc::c_void, std::mem::size_of_val(&bufsz) as libc::socklen_t);
        let busy: libc::c_int = 50;
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_BUSY_POLL, &busy as *const _ as *const libc::c_void, std::mem::size_of_val(&busy) as libc::socklen_t);
    }
}

/// Pin the current thread to a CPU core.
#[cfg(target_os = "linux")]
pub fn core_affinity_attempt(core_id: usize) -> std::io::Result<()> {
    use std::mem;
    unsafe {
        let mut set: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core_id, &mut set);
        let ret = libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &set);
        if ret != 0 { return Err(std::io::Error::last_os_error()); }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
pub fn core_affinity_attempt(_core_id: usize) -> std::io::Result<()> {
    Ok(())
}


/// Bind a tokio TCP listener, preferring SO_REUSEPORT on Linux.
pub async fn bind_listener(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    use tokio::net::TcpListener;
    #[cfg(target_os = "linux")]
    {
        match bind_reuse_port(addr) {
            Ok(std_listener) => Ok(TcpListener::from_std(std_listener).expect("from_std")),
            Err(e) => {
                tracing::warn!(error = %e, "reuse_port bind failed, fallback to bind");
                Ok(TcpListener::bind(addr).await.expect("bind tokio"))
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let l = TcpListener::bind(addr).await.expect("bind tokio");
        Ok(l)
    }
}
