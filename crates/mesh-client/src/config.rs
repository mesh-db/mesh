use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub profiles: Vec<Profile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub name: String,
    pub kind: BackendKind,
    pub uri: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub graph: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Mesh,
    Neo4j,
    Falkor,
}

impl BackendKind {
    pub fn label(self) -> &'static str {
        match self {
            BackendKind::Mesh => "mesh",
            BackendKind::Neo4j => "neo4j",
            BackendKind::Falkor => "falkordb",
        }
    }
}

impl Config {
    pub fn default_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("mesh-client")
            .join("config.toml")
    }

    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::seeded());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config at {}", path.display()))?;
        let cfg: Self = toml::from_str(&text).context("parsing config")?;
        Ok(cfg)
    }

    fn seeded() -> Self {
        Self {
            profiles: vec![
                Profile {
                    name: "mesh-local".into(),
                    kind: BackendKind::Mesh,
                    uri: "bolt://127.0.0.1:7687".into(),
                    username: Some("neo4j".into()),
                    password: Some("password".into()),
                    database: None,
                    graph: None,
                },
                Profile {
                    name: "neo4j-local".into(),
                    kind: BackendKind::Neo4j,
                    uri: "bolt://127.0.0.1:7687".into(),
                    username: Some("neo4j".into()),
                    password: Some("password".into()),
                    database: Some("neo4j".into()),
                    graph: None,
                },
                Profile {
                    name: "falkor-local".into(),
                    kind: BackendKind::Falkor,
                    uri: "redis://127.0.0.1:6379".into(),
                    username: None,
                    password: None,
                    database: None,
                    graph: Some("default".into()),
                },
            ],
        }
    }
}
