use std::path::{Path, PathBuf};

use crates_index::{Crate, IndexConfig, Version};
use futures::stream::StreamExt;
use reqwest::Client;
use tokio::{fs::{remove_file, self}, runtime::Runtime, io::AsyncReadExt};
use clap::Parser;

/// Simple downloader to mirror crates.io
#[derive(Parser, Debug)]
#[clap(about, version)]
struct Config {
    /// Local destination folder for crates.io mirror
    #[clap(short, long)]
    dest: PathBuf,

    /// When local files already exist, don't checksum
    #[clap(long)]
    dont_check_local_hashes: bool,

    /// Concurrency factor. The number of concurrent downloads
    #[clap(short, long, default_value_t = 25)]
    concurrency: usize,
}

fn main() -> Result<(), Error> {
    let config = Config::parse();

    let index = crates_index::Index::new_cargo_default()?;
    let rt = Runtime::new().unwrap();

    rt.block_on(async move {
        let client = Client::new();

        let index_config = IndexConfig {
            // documented here: https://www.pietroalbini.org/blog/downloading-crates-io/
            dl: "https://static.crates.io/crates/{crate}/{crate}-{version}.crate".to_string(),
            api: None,
        };

        let gets = tokio_stream::iter(
            index
                .crates()
                .map(|crate_| download_crate(&config, &client, crate_, &index_config)),
        )
        .buffer_unordered(config.concurrency)
        .count();
        gets.await;
    });

    Ok(())
}

#[derive(Debug)]
#[derive(thiserror::Error)]
pub enum Error {
    #[error("hash did not match")]
    Hash,
    #[error("filesystem error")]
    Fs(#[from] std::io::Error),
    #[error("network error")]
    Internet(#[from] reqwest::Error),
    #[error("could not fetch crate index")]
    Index(#[from] crates_index::Error),
    #[error("could not build url")]
    Url,
}

async fn download_crate(
    config: &Config,
    client: &Client,
    crate_: Crate,
    index_config: &IndexConfig,
) -> Result<(), Error> {
    // configure the url schema

    // create the crate's folder
    let mut crate_dir = config.dest.clone();
    crate_dir.push(crate_.name());
    fs::create_dir_all(&crate_dir).await?;

    let mut file_path = crate_dir;
    for version in crate_.versions() {
        // create filename
        file_path.push(format!(
            "{crate}-{version}.crate",
            crate = version.name(),
            version = version.version()
        ));

        if !config.dont_check_local_hashes && file_path.exists() {
            // read crate file and verify checksum
            if let Ok(hash) = sha256_file(&file_path).await {
                if &hash == version.checksum() {
                    // restore file_path before next loop
                    file_path.pop();
                    continue;
                } else {
                    // checksum mismatch, try to remove the file
                    let _ = remove_file(&file_path).await;
                }
            } else {
                // something read related failed, try to remove the file
                let _ = remove_file(&file_path).await;
            }
        }

        let bytes = download_single_version(client, version, index_config).await?;

        // verify the sha256
        let hash = sha256(&bytes);

        if &hash != version.checksum() {
            println!(
                "hash doesn't match! {}-{}",
                version.name(),
                version.version()
            );
            //return Err(Error::Hash);
            // restore file_path before next loop
            file_path.pop();
            continue;
        }

        // write crate to file
        fs::write(&file_path, &bytes).await?;

        // restore file_path before next loop
        file_path.pop();
    }

    Ok(())
}

async fn download_single_version(
    client: &Client,
    version: &Version,
    index_config: &IndexConfig,
) -> Result<Vec<u8>, Error> {
    let url = version.download_url(&index_config).ok_or(Error::Url)?;
    Ok(client.get(&url).send().await?.bytes().await?.to_vec())
}

/// Hash a slice of bytes
fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(&bytes);
    hasher.finalize().into()
}

/// Read asynchronously from the file provided, and hash the bytes as available.
///
/// Should be faster than reading all bytes, then hashing all bytes
async fn sha256_file(file: impl AsRef<Path>) -> Result<[u8; 32], Error> {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();

    let mut file = fs::File::open(file.as_ref()).await?;
    let mut buf = bytes::BytesMut::with_capacity(4096);

    loop {
        let bytes_read = file.read_buf(&mut buf).await?;
        if bytes_read == 0 {
            break;
        }

        hasher.update(&buf[..bytes_read]);
        buf.clear();
    }

    Ok(hasher.finalize().into())
}
