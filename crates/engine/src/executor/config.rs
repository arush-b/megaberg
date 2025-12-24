use parquet::basic::Compression;
use serde::{Deserialize, Serialize};

/// S3-compatible storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 endpoint URL (e.g., http://localhost:9000 for MinIO)
    pub endpoint: String,
    /// Access key ID
    pub access_key: String,
    /// Secret access key
    pub secret_key: String,
    /// Bucket name
    pub bucket: String,
    /// AWS region
    pub region: String,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9000".to_string(),
            access_key: "admin".to_string(),
            secret_key: "password".to_string(),
            bucket: "warehouse".to_string(),
            region: "us-east-1".to_string(),
        }
    }
}

/// Compaction-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Target output file size in bytes (default: 256 MB)
    pub target_file_size_bytes: u64,
    /// Number of rows per row group (default: 100,000)
    pub row_group_size: usize,
    /// Multipart upload chunk size in bytes (default: 8 MB)
    pub multipart_chunk_bytes: usize,
    /// Parquet compression codec
    #[serde(with = "compression_serde")]
    pub compression: Compression,
    /// DataFusion batch size (default: 8192)
    pub batch_size: usize,
    /// Memory limit per worker in bytes (default: 512 MB)
    pub memory_limit_bytes: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 256 * 1024 * 1024, // 256 MB
            row_group_size: 100_000,
            multipart_chunk_bytes: 8 * 1024 * 1024, // 8 MB
            compression: Compression::ZSTD(Default::default()),
            batch_size: 8192,
            memory_limit_bytes: 512 * 1024 * 1024, // 512 MB
        }
    }
}

/// Full executor configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    /// Number of worker tasks to spawn
    pub worker_count: usize,
    /// S3 storage configuration
    pub s3: S3Config,
    /// Compaction tuning parameters
    pub compaction: CompactionConfig,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            worker_count: num_cpus(),
            s3: S3Config::default(),
            compaction: CompactionConfig::default(),
        }
    }
}

impl ExecutorConfig {
    /// Create a new executor config with the given worker count.
    pub fn with_worker_count(mut self, count: usize) -> Self {
        self.worker_count = count;
        self
    }

    /// Set S3 configuration.
    pub fn with_s3(mut self, s3: S3Config) -> Self {
        self.s3 = s3;
        self
    }

    /// Set compaction configuration.
    pub fn with_compaction(mut self, compaction: CompactionConfig) -> Self {
        self.compaction = compaction;
        self
    }
}

/// Get number of CPUs (fallback to 4 if detection fails).
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

/// Serde helper for Compression enum.
mod compression_serde {
    use parquet::basic::Compression;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    enum CompressionType {
        Uncompressed,
        Snappy,
        Gzip,
        Lz4,
        Zstd,
    }

    pub fn serialize<S>(compression: &Compression, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ct = match compression {
            Compression::UNCOMPRESSED => CompressionType::Uncompressed,
            Compression::SNAPPY => CompressionType::Snappy,
            Compression::GZIP(_) => CompressionType::Gzip,
            Compression::LZ4 => CompressionType::Lz4,
            Compression::ZSTD(_) => CompressionType::Zstd,
            _ => CompressionType::Uncompressed,
        };
        ct.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Compression, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ct = CompressionType::deserialize(deserializer)?;
        Ok(match ct {
            CompressionType::Uncompressed => Compression::UNCOMPRESSED,
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Gzip => Compression::GZIP(Default::default()),
            CompressionType::Lz4 => Compression::LZ4,
            CompressionType::Zstd => Compression::ZSTD(Default::default()),
        })
    }
}
