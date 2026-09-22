//! The identifiers and enums the wire format is written in terms of.
//!
//! `agent_protocol.rs` is a verbatim copy of `crates/em-core/src/agent_protocol.rs`
//! in the platform repository and imports these names from `crate::model`,
//! exactly as it does there. This module is the client-side stand-in for the
//! platform's `em_core::model`: the same names, the same serde representation
//! (a transparent UUID, a lowercase string), and nothing the server needs
//! that a client does not - no `sqlx` derives, no table shapes.
//!
//! If a field is added to the platform's `MemoryType`, add it here in the same
//! order. The daemon never stores these, so there is no migration to think
//! about; a variant this binary does not know is a deserialisation error on a
//! memory the gateway returns.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Declare a UUID newtype that is transparent to `serde`.
macro_rules! uuid_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            /// Generate a fresh v7 (time-ordered) identifier.
            #[allow(clippy::new_without_default, dead_code)]
            pub fn new() -> Self {
                $name(Uuid::now_v7())
            }

            /// Borrow the inner UUID.
            #[allow(dead_code)]
            pub fn as_uuid(&self) -> &Uuid {
                &self.0
            }
        }

        impl From<Uuid> for $name {
            fn from(id: Uuid) -> Self {
                $name(id)
            }
        }

        impl From<$name> for Uuid {
            fn from(id: $name) -> Uuid {
                id.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(s).map($name)
            }
        }
    };
}

uuid_newtype!(
    /// The platform's id for one coding session.
    AgentSessionId
);
uuid_newtype!(
    /// The platform's id for one published memory.
    AgentMemoryId
);
uuid_newtype!(
    /// The platform's id for one message between agents.
    AgentMessageId
);

/// What kind of knowledge a memory carries.
///
/// The exact strings the gateway stores and returns. The type is chosen by the
/// publishing agent, not inferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryType {
    /// Something that turned out to be true and was not written down.
    Discovery,
    /// A choice the team made and has to keep to.
    Decision,
    /// Something that will bite whoever touches it next.
    Warning,
    /// How this codebase does a thing, for the next person who adds one.
    Convention,
    /// How a part of the system is put together.
    Architecture,
    /// A defect that exists right now and is not fixed yet.
    Bug,
    /// Something nobody has answered yet, published so that somebody can.
    Question,
}

#[allow(dead_code)]
impl MemoryType {
    /// Every variant, in the order the platform's schema lists them.
    pub const ALL: [MemoryType; 7] = [
        MemoryType::Discovery,
        MemoryType::Decision,
        MemoryType::Warning,
        MemoryType::Convention,
        MemoryType::Architecture,
        MemoryType::Bug,
        MemoryType::Question,
    ];

    /// The string the gateway stores.
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryType::Discovery => "discovery",
            MemoryType::Decision => "decision",
            MemoryType::Warning => "warning",
            MemoryType::Convention => "convention",
            MemoryType::Architecture => "architecture",
            MemoryType::Bug => "bug",
            MemoryType::Question => "question",
        }
    }

    /// Parse the stored representation.
    pub fn parse(s: &str) -> Option<Self> {
        MemoryType::ALL.into_iter().find(|t| t.as_str() == s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_type_round_trips_through_serde_as_lowercase() {
        for t in MemoryType::ALL {
            let json = serde_json::to_string(&t).unwrap();
            assert_eq!(json, format!("\"{}\"", t.as_str()));
            assert_eq!(serde_json::from_str::<MemoryType>(&json).unwrap(), t);
            assert_eq!(MemoryType::parse(t.as_str()), Some(t));
        }
    }

    #[test]
    fn session_id_is_a_transparent_uuid() {
        let id = AgentSessionId::new();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{}\"", id.0));
        assert_eq!(serde_json::from_str::<AgentSessionId>(&json).unwrap(), id);
    }
}
