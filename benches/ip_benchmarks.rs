use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ipnet::IpNet;
use iptrie::{set::RTrieSet, IpPrefix};
use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

// Benchmark IP address parsing and validation
fn bench_ip_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("ip_parsing");

    let ipv4_samples = vec![
        "8.8.8.8",
        "192.168.1.1",
        "172.16.0.1",
        "10.0.0.1",
        "255.255.255.255",
    ];

    let ipv6_samples = vec![
        "2606:4700::1111",
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "::1",
        "fe80::1",
        "ff02::1",
    ];

    group.throughput(Throughput::Elements(ipv4_samples.len() as u64));
    group.bench_function("parse_ipv4", |b| {
        b.iter(|| {
            for ip_str in &ipv4_samples {
                let _ = black_box(Ipv4Addr::from_str(ip_str));
            }
        });
    });

    group.throughput(Throughput::Elements(ipv6_samples.len() as u64));
    group.bench_function("parse_ipv6", |b| {
        b.iter(|| {
            for ip_str in &ipv6_samples {
                let _ = black_box(Ipv6Addr::from_str(ip_str));
            }
        });
    });

    group.throughput(Throughput::Elements(
        (ipv4_samples.len() + ipv6_samples.len()) as u64,
    ));
    group.bench_function("parse_mixed", |b| {
        b.iter(|| {
            for ip_str in ipv4_samples.iter().chain(ipv6_samples.iter()) {
                let _ = black_box(IpAddr::from_str(ip_str));
            }
        });
    });

    group.finish();
}

// Benchmark IP validation (is_valid operation)
fn bench_ip_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("ip_validation");

    let valid_ips = vec!["8.8.8.8", "192.168.1.1", "2606:4700::1111"];

    let invalid_ips = vec!["999.abc.def.123", "not.an.ip", "256.256.256.256"];

    group.throughput(Throughput::Elements(valid_ips.len() as u64));
    group.bench_function("validate_valid_ips", |b| {
        b.iter(|| {
            for ip_str in &valid_ips {
                let _ = black_box(IpAddr::from_str(ip_str).is_ok());
            }
        });
    });

    group.throughput(Throughput::Elements(invalid_ips.len() as u64));
    group.bench_function("validate_invalid_ips", |b| {
        b.iter(|| {
            for ip_str in &invalid_ips {
                let _ = black_box(IpAddr::from_str(ip_str).is_ok());
            }
        });
    });

    group.finish();
}

// Benchmark is_private check
fn bench_is_private(c: &mut Criterion) {
    let mut group = c.benchmark_group("is_private");

    let private_ips = vec![
        Ipv4Addr::new(192, 168, 1, 1),
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(172, 16, 0, 1),
    ];

    let public_ips = vec![
        Ipv4Addr::new(8, 8, 8, 8),
        Ipv4Addr::new(1, 1, 1, 1),
        Ipv4Addr::new(172, 34, 5, 5),
    ];

    group.throughput(Throughput::Elements(private_ips.len() as u64));
    group.bench_function("check_private", |b| {
        b.iter(|| {
            for ip in &private_ips {
                let _ = black_box(ip.is_private());
            }
        });
    });

    group.throughput(Throughput::Elements(public_ips.len() as u64));
    group.bench_function("check_public", |b| {
        b.iter(|| {
            for ip in &public_ips {
                let _ = black_box(ip.is_private());
            }
        });
    });

    group.finish();
}

// Benchmark IPv4 to numeric conversion
fn bench_ipv4_to_numeric(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipv4_to_numeric");

    let ips = vec![
        Ipv4Addr::new(8, 8, 8, 8),
        Ipv4Addr::new(192, 168, 1, 1),
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(172, 16, 0, 1),
        Ipv4Addr::new(255, 255, 255, 255),
    ];

    group.throughput(Throughput::Elements(ips.len() as u64));
    group.bench_function("conversion", |b| {
        b.iter(|| {
            for ip in &ips {
                let _ = black_box(u32::from(*ip));
            }
        });
    });

    group.finish();
}

// Benchmark numeric to IPv4 conversion
fn bench_numeric_to_ipv4(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_to_ipv4");

    let nums = vec![
        134744072u32,  // 8.8.8.8
        3232235777u32, // 192.168.1.1
        167772161u32,  // 10.0.0.1
        2886729729u32, // 172.16.0.1
        4294967295u32, // 255.255.255.255
    ];

    group.throughput(Throughput::Elements(nums.len() as u64));
    group.bench_function("conversion", |b| {
        b.iter(|| {
            for num in &nums {
                let _ = black_box(Ipv4Addr::from(*num));
            }
        });
    });

    group.finish();
}

// Benchmark trie construction and lookup (is_in operation)
fn bench_trie_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie_operations");

    // Network ranges to insert into trie
    let networks = vec![
        "8.8.8.0/24",
        "192.168.0.0/16",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "2606:4700::/32",
    ];

    // Test different trie sizes
    for size in [10, 100, 1000].iter() {
        // Build IPv4 networks
        let mut ipv4_networks: Vec<String> = Vec::new();
        for i in 0..*size {
            ipv4_networks.push(format!("10.{}.0.0/16", i % 256));
        }

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("build_ipv4_trie", size), size, |b, _| {
            b.iter(|| {
                let mut rtrie: RTrieSet<ipnet::Ipv4Net> = RTrieSet::with_capacity(*size);
                for net_str in &ipv4_networks {
                    if let Ok(IpNet::V4(net)) = IpNet::from_str(net_str) {
                        rtrie.insert(net);
                    }
                }
                let _ = black_box(rtrie.compress());
            });
        });
    }

    // Benchmark lookup performance
    let mut rtrie: RTrieSet<ipnet::Ipv4Net> = RTrieSet::with_capacity(networks.len());
    for net_str in &networks {
        if let Ok(IpNet::V4(net)) = IpNet::from_str(net_str) {
            rtrie.insert(net);
        }
    }
    let lctrie = rtrie.compress();

    let lookup_ips = vec![
        Ipv4Addr::new(8, 8, 8, 8),     // Should match
        Ipv4Addr::new(192, 168, 1, 1), // Should match
        Ipv4Addr::new(1, 1, 1, 1),     // Should not match
    ];

    group.throughput(Throughput::Elements(lookup_ips.len() as u64));
    group.bench_function("lookup_in_trie", |b| {
        b.iter(|| {
            for ip in &lookup_ips {
                let _ = black_box(lctrie.lookup(ip).len() > 0);
            }
        });
    });

    group.finish();
}

// Benchmark trie with and without pre-allocation
fn bench_trie_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie_allocation");

    let networks: Vec<String> = (0..1000)
        .map(|i| format!("10.{}.0.0/16", i % 256))
        .collect();

    group.throughput(Throughput::Elements(networks.len() as u64));

    // Without pre-allocation (current code for IPv6)
    group.bench_function("without_capacity", |b| {
        b.iter(|| {
            let mut rtrie: RTrieSet<ipnet::Ipv4Net> = RTrieSet::new();
            for net_str in &networks {
                if let Ok(IpNet::V4(net)) = IpNet::from_str(net_str) {
                    rtrie.insert(net);
                }
            }
            black_box(rtrie);
        });
    });

    // With pre-allocation (recommended)
    group.bench_function("with_capacity", |b| {
        b.iter(|| {
            let mut rtrie: RTrieSet<ipnet::Ipv4Net> = RTrieSet::with_capacity(networks.len());
            for net_str in &networks {
                if let Ok(IpNet::V4(net)) = IpNet::from_str(net_str) {
                    rtrie.insert(net);
                }
            }
            black_box(rtrie);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ip_parsing,
    bench_ip_validation,
    bench_is_private,
    bench_ipv4_to_numeric,
    bench_numeric_to_ipv4,
    bench_trie_operations,
    bench_trie_allocation,
);
criterion_main!(benches);
