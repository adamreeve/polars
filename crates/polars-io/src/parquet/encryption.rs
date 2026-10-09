//! Configuration for reading and writing files encrypted with Parquet modular encryption.

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use polars_error::PolarsResult;
use polars_parquet::parquet::encryption::decrypt::FileDecryptionProperties;
use polars_parquet::parquet::encryption::encrypt::FileEncryptionProperties;

/// Creates the properties used to encrypt each Parquet file written.
pub trait EncryptionPropertiesFactory: Send + Sync {
    /// Create encryption properties for a new file.
    fn file_encryption_properties(&self) -> PolarsResult<Arc<FileEncryptionProperties>>;
}

/// Creates the properties used to decrypt each Parquet file read.
pub trait DecryptionPropertiesFactory: Send + Sync {
    /// Create decryption properties for a file.
    fn file_decryption_properties(&self) -> PolarsResult<Arc<FileDecryptionProperties>>;
}

/// Use the same properties for every file.
impl EncryptionPropertiesFactory for Arc<FileEncryptionProperties> {
    fn file_encryption_properties(&self) -> PolarsResult<Arc<FileEncryptionProperties>> {
        Ok(Arc::clone(self))
    }
}

/// Use the same properties for every file.
impl DecryptionPropertiesFactory for Arc<FileDecryptionProperties> {
    fn file_decryption_properties(&self) -> PolarsResult<Arc<FileDecryptionProperties>> {
        Ok(Arc::clone(self))
    }
}

/// How to encrypt Parquet files when writing.
///
/// This may hold secret keys, so can't be serialized, and is compared and hashed
/// by pointer.
#[derive(Clone)]
pub struct ParquetEncryption(Arc<dyn EncryptionPropertiesFactory>);

impl ParquetEncryption {
    /// Create properties for each file using a factory.
    pub fn new(factory: Arc<dyn EncryptionPropertiesFactory>) -> Self {
        Self(factory)
    }

    /// Get the properties to use for encrypting a single file.
    pub fn file_properties(&self) -> PolarsResult<Arc<FileEncryptionProperties>> {
        self.0.file_encryption_properties()
    }
}

impl From<Arc<FileEncryptionProperties>> for ParquetEncryption {
    fn from(properties: Arc<FileEncryptionProperties>) -> Self {
        Self::new(Arc::new(properties))
    }
}

/// How to decrypt Parquet files when reading.
///
/// This may hold secret keys, so can't be serialized, and is compared and hashed
/// by pointer.
#[derive(Clone)]
pub struct ParquetDecryption(Arc<dyn DecryptionPropertiesFactory>);

impl ParquetDecryption {
    /// Create properties for each file using a factory.
    pub fn new(factory: Arc<dyn DecryptionPropertiesFactory>) -> Self {
        Self(factory)
    }

    /// Get the properties to use for decrypting a single file.
    pub fn file_properties(&self) -> PolarsResult<Arc<FileDecryptionProperties>> {
        self.0.file_decryption_properties()
    }
}

impl From<Arc<FileDecryptionProperties>> for ParquetDecryption {
    fn from(properties: Arc<FileDecryptionProperties>) -> Self {
        Self::new(Arc::new(properties))
    }
}

/// Get the properties to use for decrypting a single file, if decryption is configured.
pub fn file_decryption_properties(
    decryption: Option<&ParquetDecryption>,
) -> PolarsResult<Option<Arc<FileDecryptionProperties>>> {
    decryption
        .map(ParquetDecryption::file_properties)
        .transpose()
}

/// Implement traits that compare and hash by pointer, and refuse to serialize.
macro_rules! impl_opaque_traits {
    ($t:ident, $description:literal) => {
        impl Debug for $t {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(stringify!($t))
            }
        }

        impl Eq for $t {}

        impl PartialEq for $t {
            fn eq(&self, other: &Self) -> bool {
                std::ptr::addr_eq(Arc::as_ptr(&self.0), Arc::as_ptr(&other.0))
            }
        }

        impl Hash for $t {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                state.write_usize(Arc::as_ptr(&self.0) as *const () as usize)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $t {
            fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                use serde::de::Error;
                Err(D::Error::custom(concat!(
                    "cannot deserialize ",
                    $description
                )))
            }
        }

        #[cfg(feature = "serde")]
        impl serde::Serialize for $t {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::Error;
                Err(S::Error::custom(concat!("cannot serialize ", $description)))
            }
        }

        #[cfg(feature = "dsl-schema")]
        impl schemars::JsonSchema for $t {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                stringify!($t).into()
            }

            fn schema_id() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(concat!(module_path!(), "::", stringify!($t)))
            }

            fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                Vec::<u8>::json_schema(generator)
            }
        }
    };
}

impl_opaque_traits!(ParquetEncryption, "parquet encryption properties");
impl_opaque_traits!(ParquetDecryption, "parquet decryption properties");

#[cfg(test)]
mod tests {
    use super::*;

    struct Factory;

    impl DecryptionPropertiesFactory for Factory {
        fn file_decryption_properties(&self) -> PolarsResult<Arc<FileDecryptionProperties>> {
            Ok(FileDecryptionProperties::builder(b"0123456789012345".to_vec()).build()?)
        }
    }

    #[test]
    fn test_pointer_equality() {
        let properties = FileDecryptionProperties::builder(b"0123456789012345".to_vec())
            .build()
            .unwrap();
        let a = ParquetDecryption::from(Arc::clone(&properties));
        assert_eq!(a, a.clone());
        assert_ne!(a, ParquetDecryption::from(properties));

        let factory: Arc<dyn DecryptionPropertiesFactory> = Arc::new(Factory);
        let b = ParquetDecryption::new(Arc::clone(&factory));
        assert_eq!(b, ParquetDecryption::new(factory));
        assert_ne!(b, ParquetDecryption::new(Arc::new(Factory)));
    }

    #[test]
    fn test_file_properties() {
        let decryption = ParquetDecryption::new(Arc::new(Factory));
        let a = decryption.file_properties().unwrap();
        let b = decryption.file_properties().unwrap();
        assert!(!Arc::ptr_eq(&a, &b));

        let decryption = ParquetDecryption::from(Arc::clone(&a));
        assert!(Arc::ptr_eq(&decryption.file_properties().unwrap(), &a));
    }
}
