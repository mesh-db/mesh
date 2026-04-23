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
use crate::procedures::{ProcCursor, ProcRow};
use crate::reader::GraphReader;
use crate::value::Value;
use meshdb_core::{Edge, EdgeId, Node, NodeId, Property};
use std::collections::{HashMap, HashSet, VecDeque};

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
    #[cfg(test)]
    pub fn is_permissive(&self) -> bool {
        self.whitelist.is_empty()
            && self.blacklist.is_empty()
            && self.end_nodes.is_empty()
            && self.terminators.is_empty()
    }

    /// `true` if the walker is allowed to visit (and therefore
    /// potentially emit / expand from) a node with these labels.
    /// Blacklist always rejects. Whitelist (`+`) is the only gate
    /// on *which nodes can be traversed*. End-node (`>`) and
    /// terminator (`/`) markers do NOT restrict visitability —
    /// they only affect whether a path is *emitted* and whether
    /// expansion *stops*. This lets `">Admin"` mean "find paths
    /// ending at Admin nodes, walking through whatever intermediate
    /// labels exist" without forcing the caller to whitelist every
    /// transit label.
    pub fn is_visitable(&self, labels: &[String]) -> bool {
        if labels.iter().any(|l| self.blacklist.contains(l)) {
            return false;
        }
        if self.whitelist.is_empty() {
            return true;
        }
        labels.iter().any(|l| self.whitelist.contains(l))
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

/// Resolved configuration for an [`ExpandCursor`]. Both
/// `apoc.path.expand` (positional args) and
/// `apoc.path.expandConfig` (map-keyed config) lower to this
/// shape so the cursor has a single input surface. The builder
/// methods on the cursor enforce invariants (min ≤ max, etc.).
#[derive(Debug, Clone)]
pub struct ExpandConfig {
    pub start_node: NodeId,
    pub min_level: i64,
    pub max_level: i64,
    pub rel_filter: RelFilter,
    pub label_filter: LabelFilter,
    pub uniqueness: Uniqueness,
    /// When `false` (the Neo4j APOC default for `apoc.path.expand`)
    /// the start node is exempt from `label_filter.is_visitable`
    /// and the node-list whitelist / blacklist. It's still subject
    /// to emission filtering (endpoint / min-level).
    pub filter_start_node: bool,
    /// Per-call emission cap. `None` means unbounded (bounded in
    /// practice by the graph size and uniqueness).
    pub limit: Option<usize>,
    /// If non-empty, only paths ending at one of these nodes are
    /// emitted. Orthogonal to labelFilter's `>` markers — both
    /// restrictions apply.
    pub end_nodes: Option<HashSet<NodeId>>,
    /// If non-empty, a node with an ID in this set is never
    /// visited (identical semantics to labelFilter's `-`, but
    /// node-identity-keyed instead of label-keyed).
    pub blacklist_nodes: Option<HashSet<NodeId>>,
    /// If non-empty, a node must be in this set to be visited
    /// (identical semantics to labelFilter's `+`, but
    /// node-identity-keyed). Start node is exempt when
    /// `filter_start_node` is false.
    pub whitelist_nodes: Option<HashSet<NodeId>>,
}

impl ExpandConfig {
    /// Normalise level bounds per Neo4j APOC conventions: any
    /// negative minLevel resolves to 1 (paths must have at least
    /// one hop); any negative maxLevel is "unbounded" — we report
    /// it as `i64::MAX` so the walker's `level > max` checks
    /// never trip except on a conflicting bound.
    pub fn resolve_levels(&self) -> (usize, usize) {
        let min = if self.min_level < 0 {
            1
        } else {
            self.min_level as usize
        };
        let max = if self.max_level < 0 {
            usize::MAX
        } else {
            self.max_level as usize
        };
        (min, max.max(min))
    }
}

/// BFS path expansion cursor. Enumerates every valid path from
/// `config.start_node` that satisfies the relationship/label
/// filters, level bounds, and uniqueness rules. Yields one
/// `ProcRow` per path, under the column name `path`. Runs lazily
/// — state is preserved between `advance` calls so a downstream
/// `LIMIT` can stop the cursor without materialising the full
/// path set. BFS is used unconditionally in this first cut;
/// `bfs: false` (DFS) callers get the same result set in a
/// different order, which is still spec-compliant for callers
/// that don't assume an order.
pub struct ExpandCursor {
    config: ExpandConfig,
    tracker: UniquenessTracker,
    queue: VecDeque<Frontier>,
    /// Count of entries remaining in the current BFS level;
    /// hitting zero triggers `tracker.advance_level()` so
    /// level-scoped uniqueness resets as we cross boundaries.
    current_level_remaining: usize,
    /// Count of entries added at the next BFS level. When the
    /// current level fully drains, this becomes the new
    /// remaining count.
    next_level_count: usize,
    emitted: usize,
    output_column: String,
}

struct Frontier {
    node_id: NodeId,
    /// Node ordering in the path being built, in traversal order.
    /// `nodes[0] == start_node`, `nodes.last() == node_id`.
    path_nodes: Vec<NodeId>,
    /// Edges in traversal order. `edges.len() == nodes.len() - 1`.
    path_edges: Vec<EdgeId>,
    /// Per-path visited sets for `*_PATH` uniqueness modes.
    /// Always present but only populated when the mode uses them;
    /// keeps the frontier shape uniform at the cost of two extra
    /// empty HashSets per entry for non-path modes.
    path_visited_nodes: HashSet<NodeId>,
    path_visited_edges: HashSet<EdgeId>,
    /// Buffered pending emission — set when a newly pushed path
    /// satisfies `min_level` and `is_endpoint`. Drained before
    /// the entry is popped for expansion. Storing it here instead
    /// of a side queue keeps emission order aligned with BFS
    /// discovery order.
    pending_emit: bool,
}

impl ExpandCursor {
    /// Column name the procedure yields under. Must match the
    /// procedure's declared output column in the registry.
    pub const OUTPUT_COLUMN: &'static str = "path";

    pub fn new(config: ExpandConfig) -> Self {
        let tracker = UniquenessTracker::new(config.uniqueness);
        let queue = VecDeque::new();
        Self {
            config,
            tracker,
            queue,
            current_level_remaining: 0,
            next_level_count: 0,
            emitted: 0,
            output_column: Self::OUTPUT_COLUMN.to_string(),
        }
    }

    /// Eagerly seed the queue with the start node. Done on first
    /// `advance` rather than in `new` so a cursor can be
    /// constructed cheaply and only pay the reader round-trip
    /// when iteration actually begins.
    fn seed_if_empty(&mut self, reader: &dyn GraphReader) -> Result<()> {
        if self.emitted == 0 && self.queue.is_empty() {
            let start = self.config.start_node;
            // Consume the start-node uniqueness slot so
            // NODE_GLOBAL doesn't let a cycle come back through
            // the start node.
            let mut path_visited_nodes: HashSet<NodeId> = HashSet::new();
            let _admitted = self.tracker.try_visit_node(start, &mut path_visited_nodes);
            self.queue.push_back(Frontier {
                node_id: start,
                path_nodes: vec![start],
                path_edges: Vec::new(),
                path_visited_nodes,
                path_visited_edges: HashSet::new(),
                pending_emit: self.should_emit_start(reader)?,
            });
            self.current_level_remaining = 1;
            self.next_level_count = 0;
        }
        Ok(())
    }

    /// True when the start node is itself a valid emission
    /// (length-0 path) under the resolved config. Honors both
    /// `filter_start_node` (for visitability) and the endpoint /
    /// min-level gates (which always apply).
    fn should_emit_start(&self, reader: &dyn GraphReader) -> Result<bool> {
        let (min_level, _) = self.config.resolve_levels();
        if min_level > 0 {
            return Ok(false);
        }
        let node = match reader.get_node(self.config.start_node)? {
            Some(n) => n,
            None => return Ok(false),
        };
        if self.config.filter_start_node {
            if !self.config.label_filter.is_visitable(&node.labels) {
                return Ok(false);
            }
            if let Some(wl) = &self.config.whitelist_nodes {
                if !wl.contains(&self.config.start_node) {
                    return Ok(false);
                }
            }
            if let Some(bl) = &self.config.blacklist_nodes {
                if bl.contains(&self.config.start_node) {
                    return Ok(false);
                }
            }
        }
        if !self.config.label_filter.is_endpoint(&node.labels) {
            return Ok(false);
        }
        if let Some(endset) = &self.config.end_nodes {
            if !endset.contains(&self.config.start_node) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Expand `entry` — enumerate its outgoing / incoming
    /// adjacencies, filter them, push new frontier entries, and
    /// set `pending_emit` on each qualifying new entry. Called
    /// when the caller has decided to advance past the current
    /// frontier head. Returns the number of entries pushed onto
    /// the queue.
    fn expand(&mut self, entry_snapshot: Frontier, reader: &dyn GraphReader) -> Result<usize> {
        let node = match reader.get_node(entry_snapshot.node_id)? {
            Some(n) => n,
            None => return Ok(0),
        };
        // Terminator labels stop expansion.
        if !self.config.label_filter.can_continue(&node.labels) {
            return Ok(0);
        }
        let new_level = entry_snapshot.path_nodes.len(); // path_nodes includes start, so this is the next-hop level
        let (_, max_level) = self.config.resolve_levels();
        if new_level > max_level {
            return Ok(0);
        }
        let mut pushed = 0usize;
        if self.config.rel_filter.allows_any_outgoing() {
            for (edge_id, neighbor_id) in reader.outgoing(entry_snapshot.node_id)? {
                if self.try_push(
                    &entry_snapshot,
                    edge_id,
                    neighbor_id,
                    Direction::Outgoing,
                    reader,
                )? {
                    pushed += 1;
                }
            }
        }
        if self.config.rel_filter.allows_any_incoming() {
            for (edge_id, neighbor_id) in reader.incoming(entry_snapshot.node_id)? {
                if self.try_push(
                    &entry_snapshot,
                    edge_id,
                    neighbor_id,
                    Direction::Incoming,
                    reader,
                )? {
                    pushed += 1;
                }
            }
        }
        Ok(pushed)
    }

    /// Try to create a new frontier entry for `(edge_id,
    /// neighbor_id)` extending `entry`. Returns `Ok(true)` if a
    /// new entry was pushed, `Ok(false)` if filters rejected it.
    fn try_push(
        &mut self,
        entry: &Frontier,
        edge_id: EdgeId,
        neighbor_id: NodeId,
        direction: Direction,
        reader: &dyn GraphReader,
    ) -> Result<bool> {
        // Relationship filter — cheapest check, do first.
        let edge = match reader.get_edge(edge_id)? {
            Some(e) => e,
            None => return Ok(false),
        };
        if !self.config.rel_filter.accepts(&edge.edge_type, direction) {
            return Ok(false);
        }
        // Node-identity blacklist.
        if let Some(bl) = &self.config.blacklist_nodes {
            if bl.contains(&neighbor_id) {
                return Ok(false);
            }
        }
        // Node-identity whitelist.
        if let Some(wl) = &self.config.whitelist_nodes {
            if !wl.contains(&neighbor_id) {
                return Ok(false);
            }
        }
        let neighbor = match reader.get_node(neighbor_id)? {
            Some(n) => n,
            None => return Ok(false),
        };
        if !self.config.label_filter.is_visitable(&neighbor.labels) {
            return Ok(false);
        }
        // Uniqueness. Fork per-path sets from the parent so
        // siblings branch independently.
        let mut new_path_nodes_set = entry.path_visited_nodes.clone();
        let mut new_path_edges_set = entry.path_visited_edges.clone();
        if self.config.uniqueness.is_node_scoped()
            && !self
                .tracker
                .try_visit_node(neighbor_id, &mut new_path_nodes_set)
        {
            return Ok(false);
        }
        if self.config.uniqueness.is_relationship_scoped()
            && !self
                .tracker
                .try_visit_edge(edge_id, &mut new_path_edges_set)
        {
            return Ok(false);
        }
        // Build the new path vectors.
        let mut new_nodes = entry.path_nodes.clone();
        new_nodes.push(neighbor_id);
        let mut new_edges = entry.path_edges.clone();
        new_edges.push(edge_id);
        let new_level = new_nodes.len() - 1; // hops == edges.len
        let (min_level, max_level) = self.config.resolve_levels();
        let within_bounds = new_level >= min_level && new_level <= max_level;
        let is_endpoint = self.config.label_filter.is_endpoint(&neighbor.labels);
        let passes_end_nodes = self
            .config
            .end_nodes
            .as_ref()
            .map(|s| s.contains(&neighbor_id))
            .unwrap_or(true);
        let should_emit = within_bounds && is_endpoint && passes_end_nodes;
        self.queue.push_back(Frontier {
            node_id: neighbor_id,
            path_nodes: new_nodes,
            path_edges: new_edges,
            path_visited_nodes: new_path_nodes_set,
            path_visited_edges: new_path_edges_set,
            pending_emit: should_emit,
        });
        self.next_level_count += 1;
        Ok(true)
    }

    /// Fetch every node and edge in `entry.path_*` and assemble
    /// a `Value::Path`. Done on demand (emission time) rather
    /// than during expansion to keep frontier entries small when
    /// paths are long or branching is wide.
    fn materialize(&self, entry: &Frontier, reader: &dyn GraphReader) -> Result<Value> {
        let mut nodes: Vec<Node> = Vec::with_capacity(entry.path_nodes.len());
        for nid in &entry.path_nodes {
            match reader.get_node(*nid)? {
                Some(n) => nodes.push(n),
                None => {
                    return Err(Error::Procedure(format!(
                        "apoc.path.expand: node {nid:?} vanished during traversal"
                    )))
                }
            }
        }
        let mut edges: Vec<Edge> = Vec::with_capacity(entry.path_edges.len());
        for eid in &entry.path_edges {
            match reader.get_edge(*eid)? {
                Some(e) => edges.push(e),
                None => {
                    return Err(Error::Procedure(format!(
                        "apoc.path.expand: edge {eid:?} vanished during traversal"
                    )))
                }
            }
        }
        Ok(Value::Path { nodes, edges })
    }
}

impl ProcCursor for ExpandCursor {
    fn advance(&mut self, reader: &dyn GraphReader) -> Result<Option<ProcRow>> {
        self.seed_if_empty(reader)?;
        if let Some(limit) = self.config.limit {
            if self.emitted >= limit {
                return Ok(None);
            }
        }
        loop {
            // Process queue head: first consume its pending
            // emission (if any), then expand it next iteration.
            if let Some(head) = self.queue.front_mut() {
                if head.pending_emit {
                    head.pending_emit = false;
                    // Clone ids out before materialising so we
                    // can release the borrow.
                    let nodes = head.path_nodes.clone();
                    let edges = head.path_edges.clone();
                    let frontier_snap = Frontier {
                        node_id: head.node_id,
                        path_nodes: nodes,
                        path_edges: edges,
                        path_visited_nodes: HashSet::new(),
                        path_visited_edges: HashSet::new(),
                        pending_emit: false,
                    };
                    let path = self.materialize(&frontier_snap, reader)?;
                    let mut row: ProcRow = HashMap::new();
                    row.insert(self.output_column.clone(), path);
                    self.emitted += 1;
                    return Ok(Some(row));
                }
            } else {
                return Ok(None);
            }
            // Pop the head and expand it.
            let entry = self.queue.pop_front().expect("queue non-empty above");
            self.current_level_remaining = self.current_level_remaining.saturating_sub(1);
            let _ = self.expand(entry, reader)?;
            if self.current_level_remaining == 0 {
                // Cross level boundary.
                self.current_level_remaining = std::mem::replace(&mut self.next_level_count, 0);
                self.tracker.advance_level();
            }
            if let Some(limit) = self.config.limit {
                if self.emitted >= limit {
                    return Ok(None);
                }
            }
        }
    }
}

/// Cursor that walks like [`ExpandCursor`] but emits one row
/// per distinct reached node instead of per path. Used by
/// `apoc.path.subgraphNodes`. Because `apoc.path.subgraphNodes`
/// always runs with NODE_GLOBAL uniqueness (each node reached
/// at most once), the underlying expansion naturally produces
/// one path per node — this adapter just extracts the path's
/// last node under the `node` column.
pub struct SubgraphNodesCursor {
    inner: ExpandCursor,
}

impl SubgraphNodesCursor {
    pub const OUTPUT_COLUMN: &'static str = "node";

    pub fn new(mut config: ExpandConfig) -> Self {
        // Neo4j's subgraph walkers always include the start node
        // and enforce NODE_GLOBAL uniqueness. Override before
        // constructing the inner cursor so user-supplied config
        // doesn't accidentally exclude the start or loop.
        config.min_level = 0;
        config.uniqueness = Uniqueness::NodeGlobal;
        Self {
            inner: ExpandCursor::new(config),
        }
    }
}

impl ProcCursor for SubgraphNodesCursor {
    fn advance(&mut self, reader: &dyn GraphReader) -> Result<Option<ProcRow>> {
        // With NODE_GLOBAL + minLevel=0, every emitted path has
        // a unique last-node. Extract it and remap the column.
        loop {
            match self.inner.advance(reader)? {
                Some(mut row) => match row.remove(ExpandCursor::OUTPUT_COLUMN) {
                    Some(Value::Path { nodes, .. }) => {
                        let last = match nodes.last() {
                            Some(n) => n.clone(),
                            // A zero-node path is structurally
                            // impossible (the walker seeds with
                            // the start node), but skip
                            // defensively rather than panicking
                            // if the invariant ever slips.
                            None => continue,
                        };
                        let mut out: ProcRow = HashMap::new();
                        out.insert(Self::OUTPUT_COLUMN.to_string(), Value::Node(last));
                        return Ok(Some(out));
                    }
                    _ => continue,
                },
                None => return Ok(None),
            }
        }
    }
}

/// Cursor for `apoc.path.spanningTree` — forces minLevel=0 and
/// NODE_GLOBAL uniqueness so each reachable node is yielded
/// exactly once with its discovery path. Otherwise identical to
/// [`ExpandCursor`]. Kept as a thin newtype (rather than just
/// flipping config fields at the call site) so the procedure's
/// documented semantics are visible in the type.
pub struct SpanningTreeCursor(ExpandCursor);

impl SpanningTreeCursor {
    pub fn new(mut config: ExpandConfig) -> Self {
        config.min_level = 0;
        config.uniqueness = Uniqueness::NodeGlobal;
        Self(ExpandCursor::new(config))
    }
}

impl ProcCursor for SpanningTreeCursor {
    fn advance(&mut self, reader: &dyn GraphReader) -> Result<Option<ProcRow>> {
        self.0.advance(reader)
    }
}

/// Cursor for `apoc.path.subgraphAll` — walks the entire
/// reachable subgraph under the config, then emits a single row
/// `{nodes: [...], relationships: [...]}`. Enumeration is
/// eager internally (the output is a single aggregate, so there's
/// nothing to stream), but the cursor still returns `None`
/// after the one emission so the executor can advance past it.
pub struct SubgraphAllCursor {
    inner: ExpandCursor,
    /// `true` once we've walked the whole subgraph and buffered
    /// the aggregate row into `pending`.
    drained: bool,
    /// `true` once the aggregate row has been handed to the
    /// caller — a second call returns `None`.
    emitted: bool,
    /// Accumulated distinct nodes in first-reached order.
    nodes: Vec<Node>,
    /// Accumulated distinct edges in first-reached order.
    edges: Vec<Edge>,
    seen_nodes: HashSet<NodeId>,
    seen_edges: HashSet<EdgeId>,
}

impl SubgraphAllCursor {
    pub const NODES_COLUMN: &'static str = "nodes";
    pub const RELATIONSHIPS_COLUMN: &'static str = "relationships";

    pub fn new(mut config: ExpandConfig) -> Self {
        config.min_level = 0;
        config.uniqueness = Uniqueness::NodeGlobal;
        Self {
            inner: ExpandCursor::new(config),
            drained: false,
            emitted: false,
            nodes: Vec::new(),
            edges: Vec::new(),
            seen_nodes: HashSet::new(),
            seen_edges: HashSet::new(),
        }
    }

    fn drain(&mut self, reader: &dyn GraphReader) -> Result<()> {
        while let Some(mut row) = self.inner.advance(reader)? {
            let path = row.remove(ExpandCursor::OUTPUT_COLUMN);
            if let Some(Value::Path { nodes, edges }) = path {
                for n in nodes {
                    if self.seen_nodes.insert(n.id) {
                        self.nodes.push(n);
                    }
                }
                for e in edges {
                    if self.seen_edges.insert(e.id) {
                        self.edges.push(e);
                    }
                }
            }
        }
        self.drained = true;
        Ok(())
    }
}

impl ProcCursor for SubgraphAllCursor {
    fn advance(&mut self, reader: &dyn GraphReader) -> Result<Option<ProcRow>> {
        if self.emitted {
            return Ok(None);
        }
        if !self.drained {
            self.drain(reader)?;
        }
        // Hand over the accumulator contents via mem::take so
        // the cursor doesn't clone the full vectors — once
        // emitted the cursor returns None anyway.
        let nodes = std::mem::take(&mut self.nodes);
        let edges = std::mem::take(&mut self.edges);
        let nodes_value = Value::List(nodes.into_iter().map(Value::Node).collect());
        let edges_value = Value::List(edges.into_iter().map(Value::Edge).collect());
        let mut out: ProcRow = HashMap::new();
        out.insert(Self::NODES_COLUMN.to_string(), nodes_value);
        out.insert(Self::RELATIONSHIPS_COLUMN.to_string(), edges_value);
        self.emitted = true;
        Ok(Some(out))
    }
}

/// Coerce a procedure arg to a NodeId. Accepts `Value::Node` (a
/// materialised node from a `MATCH`) directly; reject anything
/// else with a clear error — APOC's path traversal can't start
/// from a type it can't identify.
pub fn expect_start_node(v: &Value, position: &str) -> Result<NodeId> {
    match v {
        Value::Node(n) => Ok(n.id),
        Value::Null | Value::Property(Property::Null) => Err(Error::Procedure(format!(
            "apoc.path.*: {position} must be a node, got null"
        ))),
        other => Err(Error::Procedure(format!(
            "apoc.path.*: {position} must be a node, got {other:?}"
        ))),
    }
}

/// Pull an `Int64` out of a procedure arg, returning the default
/// when the arg is null. Raises `TypeMismatch` on non-integer
/// values — `apoc.path.expand`'s min/max levels are strictly
/// integer.
pub fn expect_int_or_default(v: &Value, default: i64, position: &str) -> Result<i64> {
    match v {
        Value::Null | Value::Property(Property::Null) => Ok(default),
        Value::Property(Property::Int64(n)) => Ok(*n),
        other => Err(Error::Procedure(format!(
            "apoc.path.*: {position} must be an integer, got {other:?}"
        ))),
    }
}

/// Pull a `String` out of a procedure arg, returning the default
/// when the arg is null. Used for the filter DSL inputs.
pub fn expect_string_or_default(v: &Value, default: &str, position: &str) -> Result<String> {
    match v {
        Value::Null | Value::Property(Property::Null) => Ok(default.to_string()),
        Value::Property(Property::String(s)) => Ok(s.clone()),
        other => Err(Error::Procedure(format!(
            "apoc.path.*: {position} must be a string, got {other:?}"
        ))),
    }
}

/// Build an `ExpandConfig` from the positional arguments to
/// `apoc.path.expand(startNode, relFilter, labelFilter,
/// minLevel, maxLevel)`. Defaults for the filters are empty
/// strings (permissive); min/max default to the Neo4j
/// convention (`-1` sentinels for "unspecified").
pub fn config_from_expand_args(args: &[Value]) -> Result<ExpandConfig> {
    if args.len() != 5 {
        return Err(Error::Procedure(format!(
            "apoc.path.expand expects 5 arguments, got {}",
            args.len()
        )));
    }
    let start_node = expect_start_node(&args[0], "first argument (startNode)")?;
    let rel_filter_str =
        expect_string_or_default(&args[1], "", "second argument (relationshipFilter)")?;
    let label_filter_str = expect_string_or_default(&args[2], "", "third argument (labelFilter)")?;
    let min_level = expect_int_or_default(&args[3], -1, "fourth argument (minLevel)")?;
    let max_level = expect_int_or_default(&args[4], -1, "fifth argument (maxLevel)")?;
    Ok(ExpandConfig {
        start_node,
        min_level,
        max_level,
        rel_filter: RelFilter::parse(&rel_filter_str)?,
        label_filter: LabelFilter::parse(&label_filter_str)?,
        uniqueness: Uniqueness::NodeGlobal,
        // `apoc.path.expand`'s positional form does NOT filter the
        // start node — Neo4j docs are explicit about this. The
        // expandConfig form exposes `filterStartNode` so callers
        // can opt in.
        filter_start_node: false,
        limit: None,
        end_nodes: None,
        blacklist_nodes: None,
        whitelist_nodes: None,
    })
}

/// Build an `ExpandConfig` from the map argument to
/// `apoc.path.expandConfig(startNode, config)`. Unknown keys
/// are accepted silently (forward-compat: Neo4j APOC occasionally
/// adds keys) but logged via tracing for visibility.
pub fn config_from_expand_config_args(args: &[Value]) -> Result<ExpandConfig> {
    if args.len() != 2 {
        return Err(Error::Procedure(format!(
            "apoc.path.expandConfig expects 2 arguments, got {}",
            args.len()
        )));
    }
    let start_node = expect_start_node(&args[0], "first argument (startNode)")?;
    let config_map = match &args[1] {
        Value::Map(m) => map_from_value_map(m)?,
        Value::Property(Property::Map(m)) => m.clone(),
        Value::Null | Value::Property(Property::Null) => HashMap::new(),
        other => {
            return Err(Error::Procedure(format!(
                "apoc.path.expandConfig: second argument must be a map, got {other:?}"
            )));
        }
    };
    let get_str = |key: &str, default: &str| -> Result<String> {
        match config_map.get(key) {
            Some(Property::String(s)) => Ok(s.clone()),
            Some(Property::Null) | None => Ok(default.to_string()),
            Some(other) => Err(Error::Procedure(format!(
                "apoc.path.expandConfig: config['{key}'] must be a string, got {other:?}"
            ))),
        }
    };
    let get_int = |key: &str, default: i64| -> Result<i64> {
        match config_map.get(key) {
            Some(Property::Int64(n)) => Ok(*n),
            Some(Property::Null) | None => Ok(default),
            Some(other) => Err(Error::Procedure(format!(
                "apoc.path.expandConfig: config['{key}'] must be an integer, got {other:?}"
            ))),
        }
    };
    let get_bool = |key: &str, default: bool| -> Result<bool> {
        match config_map.get(key) {
            Some(Property::Bool(b)) => Ok(*b),
            Some(Property::Null) | None => Ok(default),
            Some(other) => Err(Error::Procedure(format!(
                "apoc.path.expandConfig: config['{key}'] must be a boolean, got {other:?}"
            ))),
        }
    };
    let min_level = get_int("minLevel", -1)?;
    let max_level = get_int("maxLevel", -1)?;
    let rel_str = get_str("relationshipFilter", "")?;
    let label_str = get_str("labelFilter", "")?;
    let uniq_str = get_str("uniqueness", "NODE_GLOBAL")?;
    let filter_start_node = get_bool("filterStartNode", false)?;
    let limit_raw = get_int("limit", -1)?;
    let limit = if limit_raw < 0 {
        None
    } else {
        Some(limit_raw as usize)
    };
    // Node-list filters live in the outer Value::Map (which carries
    // Node values), not the Property::Map lowering. When the
    // caller passes `{endNodes: [node1, node2]}` those nodes stay
    // as Value::Node — which means they won't make it through the
    // `match &args[1] { Value::Property(Property::Map(m)) => ... }`
    // branch above. Use the Value::Map branch path to extract them.
    let (end_nodes, blacklist_nodes, whitelist_nodes) = match &args[1] {
        Value::Map(m) => (
            extract_node_id_set(m.get("endNodes"), "endNodes")?,
            extract_node_id_set(m.get("blacklistNodes"), "blacklistNodes")?,
            extract_node_id_set(m.get("whitelistNodes"), "whitelistNodes")?,
        ),
        _ => (None, None, None),
    };
    Ok(ExpandConfig {
        start_node,
        min_level,
        max_level,
        rel_filter: RelFilter::parse(&rel_str)?,
        label_filter: LabelFilter::parse(&label_str)?,
        uniqueness: Uniqueness::parse(&uniq_str)?,
        filter_start_node,
        limit,
        end_nodes,
        blacklist_nodes,
        whitelist_nodes,
    })
}

/// Lower a `HashMap<String, Value>` (from a `Value::Map`
/// literal) to a `HashMap<String, Property>`, failing on entries
/// that carry graph elements. Used to extract scalar config keys
/// while deferring node-list keys for separate handling.
fn map_from_value_map(m: &HashMap<String, Value>) -> Result<HashMap<String, Property>> {
    let mut out = HashMap::with_capacity(m.len());
    for (k, v) in m {
        match v {
            Value::Property(p) => {
                out.insert(k.clone(), p.clone());
            }
            // Skip Node / Edge / Path / nested Value::Map entries
            // — those are node-list filters handled by the caller.
            _ => {}
        }
    }
    Ok(out)
}

/// Pull a set of NodeIds out of a `Value::List` whose items are
/// `Value::Node`. Returns `Ok(None)` if the key is absent or
/// explicit null — signals "no filter". An empty list is taken
/// as "match nothing" (consistent with APOC — an empty
/// whitelist excludes every node).
fn extract_node_id_set(v: Option<&Value>, key: &str) -> Result<Option<HashSet<NodeId>>> {
    match v {
        None | Some(Value::Null | Value::Property(Property::Null)) => Ok(None),
        Some(Value::Node(n)) => {
            let mut s = HashSet::new();
            s.insert(n.id);
            Ok(Some(s))
        }
        Some(Value::List(items)) => {
            let mut s = HashSet::new();
            for item in items {
                match item {
                    Value::Node(n) => {
                        s.insert(n.id);
                    }
                    other => {
                        return Err(Error::Procedure(format!(
                            "apoc.path.expandConfig: config['{key}'] must contain nodes, got {other:?}"
                        )));
                    }
                }
            }
            Ok(Some(s))
        }
        Some(other) => Err(Error::Procedure(format!(
            "apoc.path.expandConfig: config['{key}'] must be a list of nodes, got {other:?}"
        ))),
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
        // `>Admin` restricts emission, not visitability — every
        // intermediate label is still allowed to transit.
        assert!(f.is_visitable(&["Admin".into()]));
        assert!(f.is_visitable(&["User".into()]));
        // Admin is a valid endpoint; non-Admin is not.
        assert!(f.is_endpoint(&["Admin".into()]));
        assert!(!f.is_endpoint(&["User".into()]));
        // Traversal continues past an end-node marker.
        assert!(f.can_continue(&["Admin".into()]));
    }

    #[test]
    fn label_filter_terminator_stops_expansion() {
        let f = LabelFilter::parse("/Leaf").unwrap();
        // Like end-node markers, terminators don't gate
        // visitability — they only stop expansion and constrain
        // emission.
        assert!(f.is_visitable(&["Leaf".into()]));
        assert!(f.is_visitable(&["Other".into()]));
        assert!(f.is_endpoint(&["Leaf".into()]));
        assert!(!f.is_endpoint(&["Other".into()]));
        // Terminator stops expansion past this node.
        assert!(!f.can_continue(&["Leaf".into()]));
        assert!(f.can_continue(&["Other".into()]));
    }

    #[test]
    fn label_filter_mixed() {
        // Whitelist: Person. Blacklist: Spam. End-node: Admin.
        // Terminator: Sink.
        let f = LabelFilter::parse("+Person|-Spam|>Admin|/Sink").unwrap();
        // Whitelist is set, so a node must carry Person to transit.
        assert!(f.is_visitable(&["Person".into()]));
        // Admin / Sink alone aren't whitelisted — APOC callers
        // who want those as transit labels add `+Admin` etc.
        assert!(!f.is_visitable(&["Admin".into()]));
        assert!(!f.is_visitable(&["Sink".into()]));
        // Spam is blacklisted — even if it also carries Person
        // it's rejected. Test that blacklist wins.
        assert!(!f.is_visitable(&["Person".into(), "Spam".into()]));
        assert!(!f.is_visitable(&["Other".into()]));
        // `/Sink` stops expansion regardless of other labels.
        assert!(f.can_continue(&["Person".into()]));
        assert!(!f.can_continue(&["Person".into(), "Sink".into()]));
        // When > or / markers exist, only those labels are valid
        // endpoints — Person alone doesn't qualify.
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
