use crate::error::{Error, Result};
use serde::{ser, Serialize};

pub fn serialize(key: &impl serde::Serialize) -> Result<Vec<u8>> {
    let mut serializer = Serializer { output: Vec::new() };
    key.serialize(&mut serializer)?;

    Ok(serializer.output)
}

pub struct Serializer {
    output: Vec<u8>,
}

impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleVariant = Self;
    type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok> {
        unimplemented!("do not support bool")
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok> {
        unimplemented!("do not support i8")
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok> {
        unimplemented!("do not support i16")
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok> {
        unimplemented!("do not support i32")
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok> {
        unimplemented!("do not support i64")
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        unimplemented!()
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok> {
        unimplemented!("do not support u16")
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok> {
        unimplemented!("do not support u32")
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok> {
        unimplemented!()
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok> {
        unimplemented!()
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok> {
        unimplemented!()
    }

    fn serialize_str(self, _v: &str) -> Result<()> {
        unimplemented!()
    }


    /// Serializes a byte slice.
    ///
    /// Encodes the byte slice with `[0, 0]` as the end marker.
    /// If the original value contains `[0]`, it is encoded as `[0, 255]`.
    ///
    /// ## Encoding
    ///
    /// | Original        | Encoded value       |
    /// |-----------------|-----------------------|
    /// | `97 98 99`      | `97 98 99 0 0`        |
    /// | `97 98 0 99`    | `97 98 0 255 99 0 0`  |
    /// | `97 98 0 0 99`  | `97 98 0 255 0 255 99 0 0` |
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        for &byte in v {
            if byte == 0 {
                self.output.extend([0, 255]);
            } else {
                self.output.push(byte);
            }
        }
        self.output.extend([0, 0]);
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        unimplemented!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        unimplemented!()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok> {
        self.output.extend(u8::try_from(variant_index));

        Ok(())
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        unimplemented!()
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        unimplemented!()
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::mvcc::{MvccKey, MvccKeyPrefix};

    use super::serialize;
    #[test]
    fn test_encode() {
        let ser_cmp = |k: MvccKey, v: Vec<u8>| {
            let res = serialize(&k).unwrap();
            assert_eq!(res, v);
        };

        ser_cmp(MvccKey::NextVersion, vec![0]);
        ser_cmp(MvccKey::TxnActive(1), vec![1, 0, 0, 0, 0, 0, 0, 0, 1]);
        ser_cmp(
            MvccKey::TxnWrite(1, vec![1, 2, 3]),
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0],
        );
        ser_cmp(
            MvccKey::Version(b"abc".to_vec(), 11),
            vec![3, 97, 98, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11],
        );
    }

    #[test]
    fn test_encode_prefix() {
        let ser_cmp = |k: MvccKeyPrefix, v: Vec<u8>| {
            let res = serialize(&k).unwrap();
            assert_eq!(res, v);
        };

        ser_cmp(MvccKeyPrefix::NextVersion, vec![0]);
        ser_cmp(MvccKeyPrefix::TxnActive, vec![1]);
        ser_cmp(MvccKeyPrefix::TxnWrite(1), vec![2, 0, 0, 0, 0, 0, 0, 0, 1]);
        ser_cmp(
            MvccKeyPrefix::Version(b"ab".to_vec()),
            vec![3, 97, 98, 0, 0],
        );
    }
}
