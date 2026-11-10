//! Spawns the real `lightcycle` binary as a standalone single-node cluster (plan 03-15): the CLI
//! flags reach `NodeConfig`, the node bootstraps itself, becomes leader and writes WAL files.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Kills and reaps the child on every exit path, including a failed assertion.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// `<16 lowercase hex digits>.wal`
fn is_wal_name(name: &str) -> bool {
    name.strip_suffix(".wal")
        .is_some_and(|stem| stem.len() == 16 && stem.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
}

#[test]
fn standalone_binary_becomes_leader_and_creates_wal() {
    let dir = tempfile::tempdir().unwrap();
    // The child's stderr goes to a file so a startup failure is reported with its cause instead
    // of as a bare "no leader line".
    let logs = tempfile::tempdir().unwrap();
    let stderr_path = logs.path().join("stderr.log");
    let stderr_text = || std::fs::read_to_string(&stderr_path).unwrap_or_default();
    let mut child = Command::new(env!("CARGO_BIN_EXE_lightcycle"))
        .args(["--node-id", "1", "--bind", "127.0.0.1:0", "--data-dir"])
        .arg(dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).unwrap()))
        .stdin(Stdio::null())
        .spawn()
        .expect("spawn lightcycle binary");
    let stdout = child.stdout.take().unwrap();
    let guard = ChildGuard(child);

    // Read stdout on a thread so the deadline below cannot be defeated by a blocking read.
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut seen = Vec::new();
    let mut listening = false;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) => {
                listening |= line.contains("[Lightcycle] node 1 listening on 127.0.0.1:");
                let leader = line.contains("[Lightcycle] node 1 is leader (term ");
                seen.push(line);
                if leader {
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                "no leader line within 15 s; stdout so far: {seen:#?}; stderr: {}",
                stderr_text()
            ),
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!(
                "the node closed its stdout (exited?) before a leader line; stdout: {seen:#?}; \
                 stderr: {}",
                stderr_text()
            ),
        }
    }
    assert!(listening, "the listening line precedes the leader line: {seen:#?}");

    drop(guard);

    let wal_dir = dir.path().join("wal");
    let wal_files: Vec<String> = std::fs::read_dir(&wal_dir)
        .expect("wal directory exists")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| is_wal_name(n))
        .collect();
    assert!(!wal_files.is_empty(), "a 16-hex-digit .wal segment exists in {wal_dir:?}");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&wal_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "wal directory mode");
    }
}
