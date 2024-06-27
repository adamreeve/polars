#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use crate::frame::join::hash_join::SmartString;

#[derive(Clone, Debug, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct BetweenOptions {
    pub left_by: Option<Vec<SmartString>>,
    pub right_by: Option<Vec<SmartString>>,
}
