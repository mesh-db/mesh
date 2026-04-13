use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(pub u64);

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: PeerId,
    pub address: String,
}

impl Peer {
    pub fn new(id: PeerId, address: impl Into<String>) -> Self {
        Self {
            id,
            address: address.into(),
        }
    }
}

/// Static membership list: peer id -> network address.
#[derive(Debug, Clone)]
pub struct Membership {
    peers: BTreeMap<PeerId, String>,
}

impl Membership {
    pub fn new<I: IntoIterator<Item = Peer>>(peers: I) -> Self {
        let peers = peers.into_iter().map(|p| (p.id, p.address)).collect();
        Self { peers }
    }

    pub fn address(&self, id: PeerId) -> Option<&str> {
        self.peers.get(&id).map(String::as_str)
    }

    pub fn contains(&self, id: PeerId) -> bool {
        self.peers.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn peer_ids(&self) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.keys().copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (PeerId, &str)> {
        self.peers.iter().map(|(id, addr)| (*id, addr.as_str()))
    }
}
