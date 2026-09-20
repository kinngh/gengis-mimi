//! Versioned, checksummed, little-endian vector blocks. No unsafe casts.
use crate::{
    Error, Result,
    model::{MAX_DIMENSIONS, validate_id},
};

pub(crate) fn encode(rows: &[(String, Vec<f32>)]) -> Vec<u8> {
    let dimensions = rows.first().map_or(0, |r| r.1.len());
    let mut out = b"GMVB\x01".to_vec();
    out.extend_from_slice(&(dimensions as u32).to_le_bytes());
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for (id, vector) in rows {
        out.extend_from_slice(&(id.len() as u16).to_le_bytes());
        out.extend_from_slice(id.as_bytes());
        for value in vector {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    out.extend_from_slice(&crc32fast::hash(&out).to_le_bytes());
    out
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Vec<(String, Vec<f32>)>> {
    let bad = || Error::Corrupt("invalid vector block or checksum".into());
    if bytes.len() < 17 || &bytes[..5] != b"GMVB\x01" {
        return Err(bad());
    }
    let (body, checksum) = bytes.split_at(bytes.len() - 4);
    if crc32fast::hash(body) != u32::from_le_bytes(checksum.try_into().map_err(|_| bad())?) {
        return Err(bad());
    }
    let dimensions = u32::from_le_bytes(body[5..9].try_into().map_err(|_| bad())?) as usize;
    let count = u32::from_le_bytes(body[9..13].try_into().map_err(|_| bad())?) as usize;
    if dimensions > MAX_DIMENSIONS
        || (count > 0 && dimensions == 0)
        || count > body.len() / (dimensions * 4 + 2)
    {
        return Err(bad());
    }
    let mut rest = &body[13..];
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let id_len = u16::from_le_bytes(
            rest.get(..2)
                .ok_or_else(bad)?
                .try_into()
                .map_err(|_| bad())?,
        ) as usize;
        rest = &rest[2..];
        let id = std::str::from_utf8(rest.get(..id_len).ok_or_else(bad)?)
            .map_err(|_| bad())?
            .to_owned();
        validate_id(&id).map_err(|_| bad())?;
        rest = &rest[id_len..];
        let data = rest.get(..dimensions * 4).ok_or_else(bad)?;
        let mut vector = Vec::with_capacity(dimensions);
        for chunk in data.as_chunks::<4>().0 {
            let value = f32::from_le_bytes(*chunk);
            if !value.is_finite() {
                return Err(bad());
            }
            vector.push(value);
        }
        rest = &rest[dimensions * 4..];
        rows.push((id, vector));
    }
    if !rest.is_empty() {
        return Err(bad());
    }
    Ok(rows)
}

pub(crate) fn encode_vector(vector: &[f32]) -> Vec<u8> {
    encode(&[("v".into(), vector.to_vec())])
}
pub(crate) fn decode_vector(bytes: &[u8]) -> Result<Vec<f32>> {
    let mut rows = decode(bytes)?;
    if rows.len() != 1 {
        return Err(Error::Corrupt("expected one vector".into()));
    }
    Ok(rows.remove(0).1)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binary_blocks_reject_corruption_nonfinite_values_and_trailing_data() {
        let rows = vec![
            ("one".into(), vec![1.0, -2.5]),
            ("two".into(), vec![0.25, 42.0]),
        ];
        let bytes = encode(&rows);
        assert_eq!(decode(&bytes).unwrap(), rows);
        let mut broken = bytes.clone();
        broken[20] ^= 1;
        assert!(decode(&broken).is_err());
        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
        assert!(decode(&encode(&[("bad".into(), vec![f32::NAN])])).is_err());
        let mut extra = bytes[..bytes.len() - 4].to_vec();
        extra.push(0);
        extra.extend_from_slice(&crc32fast::hash(&extra).to_le_bytes());
        assert!(decode(&extra).is_err());
    }
}
