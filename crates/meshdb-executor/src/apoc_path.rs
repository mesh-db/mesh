//! Shared traversal primitives for `apoc.path.*` procedures.
//!
//! `apoc.path.expand`, `apoc.path.expandConfig`, `subgraphAll`,
//! `subgraphNodes`, and `spanningTree` all share the same
//! underlying walker — what differs is the filter/config surface
//! and the emission shape. This module owns the shared pieces so
//! the individual procedure implementations only worry about
//! coercing arguments and shaping output.
//!
//! # Surfaces
//!
//! * [`RelFilter`] — parses APOC's relationshipFilter mini-DSL
//!   (e.g. `">KNOWS|<LIKES|FRIENDS"`) into a per-type direction
//!   map so the walker can accept / reject each candidate edge
//!   without re-parsing.
//! * [`LabelFilter`] — parses the labelFilter DSL (`"+A|-B|>C|/D"`)
//!   into whitelist / blacklist / end-node / terminator sets.
//!   Captures both *can the traversal pass through this node*
//!   and *is this node a valid end-of-path*.
//! * [`Uniqueness`] + [`UniquenessTracker`] — implements all seven
//!   Neo4j uniqueness modes (`NODE_GLOBAL`, `NODE_LEVEL`,
//!   `NODE_PATH`, `RELATIONSHIP_GLOBAL`, `RELATIONSHIP_LEVEL`,
//!   `RELATIONSHIP_PATH`, `NONE`). The tracker owns the
//!   traversal-global and per-level state; per-path state lives
//!   on the frontier entries since it must fork when the
//!   traversal branches.

use crate::error::{Error, Result};
use meshdb_core::{EdgeId, NodeId};
use std::collections::HashSet;

/// Per-edge traversal direction used when deciding whether an
/// adjacency edge matches a [`RelFilter`] clause. The walker
/// reports the direction it's traversing *out of* the current
/// node — `Outgoing` means the edge is `(current)-[e]->(other)`,
/// `Incoming` means `(current)<-[e]-(other)` (i.e. reached via
/// the reverse adjacency CF).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

impl Direction {
    /// Does a filter allowing `self` admit a traversal going
    /// `candidate`? `Both` accepts either; otherwise the two must
    /// match exactly.
    pub fn allows(self, candidate: Direction) -> bool {
        match self {
            Direction::Both => true,
            _ => self == candidate,
        }
    }
}

/// Parsed relationshipFilter. Each entry binds an edge type to the
/// direction(s) the walker is allowed to traverse edges of that
/// type. A `wildcard` slot covers bare `>` / `<` / `` (empty), which
/// accept any type in the given direction.
#[derive(Debug, Clone, Default)]
pub struct RelFilter {
    /// Map of type name → allowed direction for that type.
    by_type: std::collections::HashMap<String, Direction>,
    /// Direction allowed for edge types not mentioned in `by_type`.
    /// `None` means a closed filter — unlisted types are rejected.
    /// `Some(dir)` means any type is allowed in `dir` (the `>`,
    /// `<`, or empty-string DSL forms).
    wildcard: Option<Direction>,
}

impl RelFilter {
    /// Parse APOC's relationshipFilter syntax. The filter is a
    /// pipe-separated list where each item is:
    ///
    /// * `">TYPE"` or `"TYPE>"` — outgoing edges of `TYPE`
    /// * `"<TYPE"` or `"TYPE<"` — incoming edges of `TYPE`
    /// * `"TYPE"` — edges of `TYPE` in either direction
    /// * `">"` — any type, outgoing
    /// * `"<"` — any type, incoming
    /// * `""` (empty string / null) — any type, any direction
    ///
    /// An empty input parses as fully-permissive. A non-empty
    /// input with no wildcard and an empty parsed map is a
    /// "match nothing" filter; that's legal input but means the
    /// walker will find no matches.
    pub fn parse(s: &str) -> Result<Self> {
        let mut filter = RelFilter::default();
        let trimmed = s.trim();
        if trimmed.is_empty() {
            filter.wildcard = Some(Direction::Both);
            return Ok(filter);
        }
        for raw in trimmed.split('|') {
            let item = raw.trim();
            if item.is_empty() {
                continue;
            }
            // Detect prefix (> / <) or suffix (> / <) direction marker.
            // Neo4j APOC accepts both forms; strip whichever is present.
            let (dir, type_name) = if let Some(rest) = item.strip_prefix('>') {
                (Direction::Outgoing, rest.trim())
            } else if let Some(rest) = item.strip_prefix('<') {
                (Direction::Incoming, rest.trim())
            } else if let Some(rest) = item.strip_suffix('>') {
                (Direction::Outgoing, rest.trim())
            } else if let Some(rest) = item.strip_suffix('<') {
                (Direction::Incoming, rest.trim())
            } else {
                (Direction::Both, item)
            };
            if type_name.is_empty() {
                // Bare `>` or `<` or empty — wildcard slot.
                // If a wildcard is already set, widen to the
                // union; mismatched wildcards collapse to Both.
                filter.wildcard = match filter.wildcard {
                    None => Some(dir),
                    Some(existing) if existing == dir => Some(existing),
                    _ => Some(Direction::Both),
                };
            } else {
                // If the same type appears twice with different
                // directions the filter widens to Both (Neo4j
                // behaviour — `"KNOWS>|KNOWS<"` == `"KNOWS"`).
                let entry = filter.by_type.entry(type_name.to_string()).or_insert(dir);
                if *entry != dir {
                    *entry = Direction::Both;
                }
            }
        }
        Ok(filter)
    }

    /// True when the walker may traverse an edge of `edge_type`
    /// in `direction`. A closed filter (no wildcard, type not in
    /// the map) rejects.
    pub fn accepts(&self, edge_type: &str, direction: Direction) -> bool {
        if let Some(allowed) = self.by_type.get(edge_type) {
            allowed.allows(direction)
        } else if let Some(wildcard) = self.wildcard {
            wildcard.allows(direction)
        } else {
            false
        }
    }

    /// `true` when the filter allows *outgoing* traversal for any
    /// type — used by the walker to skip an adjacency lookup when
    /// nothing matches.
    pub fn allows_any_outgoing(&self) -> bool {
        self.wildcard
            .map(|d| d.allows(Direction::Outgoing))
            .unwrap_or(false)
            || self.by_type.values().any(|d| d.allows(Direction::Outgoing))
    }

    /// Symmetric counterpart of [`Self::allows_any_outgoing`].
    pub fn allows_any_incoming(&self) -> bool {
        self.wildcard
            .map(|d| d.allows(Direction::Incoming))
            .unwrap_or(false)
            || self.by_type.values().any(|d| d.allows(Direction::Incoming))
    }
}

/// Parsed labelFilter. Splits the pipe-separated items into four
/// disjoint sets based on their prefix:
///
/// * whitelist (`+Label` or bare `Label`) — at least one such
///   label is required on a visitable node (unless only end /
///   terminator markers are present, in which case those grant
///   visitability too).
/// * blacklist (`-Label`) — a node with any of these labels is
///   never visited, never emitted.
/// * end_nodes (`>Label`) — paths ending at nodes with these
///   labels are yielded, but traversal continues past them.
/// * terminators (`/Label`) — paths ending at nodes with these
///   labels are yielded, and traversal does *not* continue past
///   them.
///
/// An empty / null labelFilter produces an all-permissive filter:
/// every reached node is visitable and a valid endpoint.
#[derive(Debug, Clone, Default)]
pub struct LabelFilter {
    whitelist: HashSet<String>,
    blacklist: HashSet<String>,
    end_nodes: HashSet<String>,
    terminators: HashSet<String>,
}

impl LabelFilter {
    pub fn parse(s: &str) -> Result<Self> {
        let mut filter = LabelFilter::default();
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Ok(filter);
        }
        for raw in trimmed.split('|') {
            let item = raw.trim();
            if item.is_empty() {
                continue;
            }
            let (set, label) = match item.as_bytes()[0] {
                b'+' => (&mut filter.whitelist, item[1..].trim()),
                b'-' => (&mut filter.blacklist, item[1..].trim()),
                b'>' => (&mut filter.end_nodes, item[1..].trim()),
                b'/' => (&mut filter.terminators, item[1..].trim()),
                _ => (&mut filter.whitelist, item),
            };
            if label.is_empty() {
                return Err(Error::Procedure(format!(
                    "labelFilter item '{item}' has no label after the prefix"
                )));
            }
            set.insert(label.to_string());
        }
        Ok(filter)
    }

    /// `true` when the filter has no entries at all — every node
    /// is visitable, every reached node is a valid endpoint.
    pub fn is_permissive(&self) -> bool {
        self.whitelist.is_empty()
            && self.blacklist.is_empty()
            && self.end_nodes.is_empty()
            && self.terminators.is_empty()
    }

    /// `true` if the walker is allowed to visit (and therefore
    /// potentially emit / expand from) a node with these labels.
    /// Blacklist always rejects; whitelist gate fires only when at
    /// least one positive marker exists.
    pub fn is_visitable(&self, labels: &[String]) -> bool {
        if labels.iter().any(|l| self.blacklist.contains(l)) {
            return false;
        }
        if self.whitelist.is_empty() && self.end_nodes.is_empty() && self.terminators.is_empty() {
            return true;
        }
        labels.iter().any(|l| {
            self.whitelist.contains(l) || self.end_nodes.contains(l) || self.terminators.contains(l)
        })
    }

    /// `true` if traversal may continue past a node with these
    /// labels. Terminators (`/Label`) stop expansion; everything
    /// else (that passed `is_visitable`) continues.
    pub fn can_continue(&self, labels: &[String]) -> bool {
        !labels.iter().any(|l| self.terminators.contains(l))
    }

    /// `true` if a node with these labels is a valid end-of-path
    /// for emission. When any `>` or `/` marker is present, only
    /// nodes carrying one of those labels qualify; otherwise every
    /// visitable node qualifies. Callers check `is_visitable`
    /// first, so this method assumes the node is already
    /// visitable.
    pub fn is_endpoint(&self, labels: &[String]) -> bool {
        if self.end_nodes.is_empty() && self.terminators.is_empty() {
            return true;
        }
        labels
            .iter()
            .any(|l| self.end_nodes.contains(l) || self.terminators.contains(l))
    }
}

/// APOC uniqueness mode. Governs how the walker de-duplicates
/// node / relationship visits. Defaults to `NodeGlobal` at the
/// Neo4j APOC call site (a node is visited at most once across
/// the entire traversal) — callers that want looser semantics
/// must set `uniqueness` explicitly in the config map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Uniqueness {
    NodeGlobal,
    NodeLevel,
    NodePath,
    RelationshipGlobal,
    RelationshipLevel,
    RelationshipPath,
    None,
}

impl Uniqueness {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "NODE_GLOBAL" => Ok(Self::NodeGlobal),
            "NODE_LEVEL" => Ok(Self::NodeLevel),
            "NODE_PATH" => Ok(Self::NodePath),
            "RELATIONSHIP_GLOBAL" => Ok(Self::RelationshipGlobal),
            "RELATIONSHIP_LEVEL" => Ok(Self::RelationshipLevel),
            "RELATIONSHIP_PATH" => Ok(Self::RelationshipPath),
            "NONE" => Ok(Self::None),
            other => Err(Error::Procedure(format!(
                "unknown uniqueness mode '{other}' — expected one of \
                 NODE_GLOBAL / NODE_LEVEL / NODE_PATH / \
                 RELATIONSHIP_GLOBAL / RELATIONSHIP_LEVEL / \
                 RELATIONSHIP_PATH / NONE"
            ))),
        }
    }

    pub fn is_node_scoped(self) -> bool {
        matches!(self, Self::NodeGlobal | Self::NodeLevel | Self::NodePath)
    }

    pub fn is_relationship_scoped(self) -> bool {
        matches!(
            self,
            Self::RelationshipGlobal | Self::RelationshipLevel | Self::RelationshipPath
        )
    }

    pub fn is_global(self) -> bool {
        matches!(self, Self::NodeGlobal | Self::RelationshipGlobal)
    }

    pub fn is_level(self) -> bool {
        matches!(self, Self::NodeLevel | Self::RelationshipLevel)
    }

    pub fn is_path(self) -> bool {
        matches!(self, Self::NodePath | Self::RelationshipPath)
    }
}

/// Traversal-scope uniqueness state. The tracker owns the global
/// and per-level sets; per-path sets live on the frontier entries
/// since they must fork when the walker branches. Callers check
/// admission (`try_visit_node` / `try_visit_edge`) before
/// enqueuing a new frontier entry.
#[derive(Debug, Clone, Default)]
pub struct UniquenessTracker {
    mode: Option<Uniqueness>,
    visited_nodes_global: HashSet<NodeId>,
    visited_edges_global: HashSet<EdgeId>,
    /// Nodes already visited at the current BFS level — reset on
    /// each [`Self::advance_level`] call.
    visited_nodes_level: HashSet<NodeId>,
    /// Edges already traversed at the current BFS level.
    visited_edges_level: HashSet<EdgeId>,
}

impl UniquenessTracker {
    pub fn new(mode: Uniqueness) -> Self {
        Self {
            mode: Some(mode),
            ..Default::default()
        }
    }

    /// Called by the walker when it moves to the next BFS level.
    /// Clears the per-level sets; global sets persist across
    /// levels.
    pub fn advance_level(&mut self) {
        self.visited_nodes_level.clear();
        self.visited_edges_level.clear();
    }

    /// Try to admit a node visit. Returns `true` if the visit is
    /// allowed (and records it in whatever scope applies);
    /// `false` means the walker should skip this node.
    /// `path_visited` is the per-path visited-node set carried on
    /// the frontier entry — consulted (and mutated) only when the
    /// mode is [`Uniqueness::NodePath`].
    pub fn try_visit_node(&mut self, node: NodeId, path_visited: &mut HashSet<NodeId>) -> bool {
        match self.mode {
            Some(Uniqueness::NodeGlobal) => self.visited_nodes_global.insert(node),
            Some(Uniqueness::NodeLevel) => self.visited_nodes_level.insert(node),
            Some(Uniqueness::NodePath) => path_visited.insert(node),
            _ => true,
        }
    }

    /// Try to admit an edge traversal. Same contract as
    /// [`Self::try_visit_node`] but for the relationship-scoped
    /// uniqueness modes. `path_visited` is the per-path edge set
    /// carried on the frontier entry.
    pub fn try_visit_edge(&mut self, edge: EdgeId, path_visited: &mut HashSet<EdgeId>) -> bool {
        match self.mode {
            Some(Uniqueness::RelationshipGlobal) => self.visited_edges_global.insert(edge),
            Some(Uniqueness::RelationshipLevel) => self.visited_edges_level.insert(edge),
            Some(Uniqueness::RelationshipPath) => path_visited.insert(edge),
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_filter_empty_is_permissive() {
        let f = RelFilter::parse("").unwrap();
        assert!(f.accepts("KNOWS", Direction::Outgoing));
        assert!(f.accepts("KNOWS", Direction::Incoming));
        assert!(f.accepts("ANY", Direction::Outgoing));
    }

    #[test]
    fn rel_filter_prefix_direction() {
        let f = RelFilter::parse(">KNOWS").unwrap();
        assert!(f.accepts("KNOWS", Direction::Outgoing));
        assert!(!f.accepts("KNOWS", Direction::Incoming));
        assert!(!f.accepts("LIKES", Direction::Outgoing));
    }

    #[test]
    fn rel_filter_suffix_direction() {
        let f = RelFilter::parse("KNOWS>").unwrap();
        assert!(f.accepts("KNOWS", Direction::Outgoing));
        assert!(!f.accepts("KNOWS", Direction::Incoming));
    }

    #[test]
    fn rel_filter_mixed_list() {
        let f = RelFilter::parse(">KNOWS|<LIKES|FRIENDS").unwrap();
        assert!(f.accepts("KNOWS", Direction::Outgoing));
        assert!(!f.accepts("KNOWS", Direction::Incoming));
        assert!(f.accepts("LIKES", Direction::Incoming));
        assert!(!f.accepts("LIKES", Direction::Outgoing));
        assert!(f.accepts("FRIENDS", Direction::Outgoing));
        assert!(f.accepts("FRIENDS", Direction::Incoming));
        assert!(!f.accepts("OTHER", Direction::Outgoing));
    }

    #[test]
    fn rel_filter_bare_direction_wildcard() {
        let f = RelFilter::parse(">").unwrap();
        assert!(f.accepts("KNOWS", Direction::Outgoing));
        assert!(f.accepts("LIKES", Direction::Outgoing));
        assert!(!f.accepts("KNOWS", Direction::Incoming));
    }

    #[test]
    fn rel_filter_same_type_both_directions_widens() {
        let f = RelFilter::parse("KNOWS>|KNOWS<").unwrap();
        assert!(f.accepts("KNOWS", Direction::Outgoing));
        assert!(f.accepts("KNOWS", Direction::Incoming));
    }

    #[test]
    fn label_filter_empty_permissive() {
        let f = LabelFilter::parse("").unwrap();
        assert!(f.is_permissive());
        assert!(f.is_visitable(&["A".into()]));
        assert!(f.is_endpoint(&["A".into()]));
        assert!(f.can_continue(&["A".into()]));
    }

    #[test]
    fn label_filter_blacklist_rejects() {
        let f = LabelFilter::parse("-Secret").unwrap();
        assert!(!f.is_visitable(&["Secret".into()]));
        assert!(f.is_visitable(&["Public".into()]));
    }

    #[test]
    fn label_filter_whitelist_admits_only_matching() {
        let f = LabelFilter::parse("+Person").unwrap();
        assert!(f.is_visitable(&["Person".into()]));
        assert!(!f.is_visitable(&["Company".into()]));
    }

    #[test]
    fn label_filter_bare_is_whitelist() {
        let f = LabelFilter::parse("Person").unwrap();
        assert!(f.is_visitable(&["Person".into()]));
        assert!(!f.is_visitable(&["Company".into()]));
    }

    #[test]
    fn label_filter_end_node_marker() {
        let f = LabelFilter::parse(">Admin").unwrap();
        // `>Admin` implicitly whitelists Admin (visitable).
        assert!(f.is_visitable(&["Admin".into()]));
        // Non-Admin nodes are not visitable when only `>` is set.
        assert!(!f.is_visitable(&["User".into()]));
        // Admin is a valid endpoint.
        assert!(f.is_endpoint(&["Admin".into()]));
        // Traversal continues past an end-node marker.
        assert!(f.can_continue(&["Admin".into()]));
    }

    #[test]
    fn label_filter_terminator_stops_expansion() {
        let f = LabelFilter::parse("/Leaf").unwrap();
        assert!(f.is_visitable(&["Leaf".into()]));
        assert!(f.is_endpoint(&["Leaf".into()]));
        // Terminator stops expansion past this node.
        assert!(!f.can_continue(&["Leaf".into()]));
    }

    #[test]
    fn label_filter_mixed() {
        // Whitelist: Person. Blacklist: Spam. End-node: Admin.
        // Terminator: Sink.
        let f = LabelFilter::parse("+Person|-Spam|>Admin|/Sink").unwrap();
        assert!(f.is_visitable(&["Person".into()]));
        assert!(f.is_visitable(&["Admin".into()]));
        assert!(f.is_visitable(&["Sink".into()]));
        assert!(!f.is_visitable(&["Spam".into()]));
        assert!(!f.is_visitable(&["Other".into()]));
        assert!(f.can_continue(&["Person".into()]));
        assert!(f.can_continue(&["Admin".into()]));
        assert!(!f.can_continue(&["Sink".into()]));
        // When any > or / marker is present, only those labels
        // are valid endpoints — Person alone doesn't qualify.
        assert!(!f.is_endpoint(&["Person".into()]));
        assert!(f.is_endpoint(&["Admin".into()]));
        assert!(f.is_endpoint(&["Sink".into()]));
    }

    #[test]
    fn uniqueness_parse_roundtrip() {
        assert_eq!(
            Uniqueness::parse("NODE_GLOBAL").unwrap(),
            Uniqueness::NodeGlobal
        );
        assert_eq!(
            Uniqueness::parse("relationship_path").unwrap(),
            Uniqueness::RelationshipPath
        );
        assert_eq!(Uniqueness::parse("NONE").unwrap(), Uniqueness::None);
        assert!(Uniqueness::parse("bogus").is_err());
    }

    #[test]
    fn uniqueness_tracker_node_global() {
        let mut t = UniquenessTracker::new(Uniqueness::NodeGlobal);
        let a = NodeId::new();
        let mut path = HashSet::new();
        assert!(t.try_visit_node(a, &mut path));
        assert!(!t.try_visit_node(a, &mut path));
    }

    #[test]
    fn uniqueness_tracker_node_level_resets() {
        let mut t = UniquenessTracker::new(Uniqueness::NodeLevel);
        let a = NodeId::new();
        let mut path = HashSet::new();
        assert!(t.try_visit_node(a, &mut path));
        assert!(!t.try_visit_node(a, &mut path));
        t.advance_level();
        assert!(t.try_visit_node(a, &mut path));
    }

    #[test]
    fn uniqueness_tracker_node_path_scoped_to_path() {
        let mut t = UniquenessTracker::new(Uniqueness::NodePath);
        let a = NodeId::new();
        let mut path_one = HashSet::new();
        let mut path_two = HashSet::new();
        assert!(t.try_visit_node(a, &mut path_one));
        assert!(!t.try_visit_node(a, &mut path_one));
        // Another path may visit the same node.
        assert!(t.try_visit_node(a, &mut path_two));
    }

    #[test]
    fn uniqueness_tracker_none_admits_everything() {
        let mut t = UniquenessTracker::new(Uniqueness::None);
        let a = NodeId::new();
        let mut path = HashSet::new();
        assert!(t.try_visit_node(a, &mut path));
        assert!(t.try_visit_node(a, &mut path));
    }
}
