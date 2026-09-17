use std::{
    collections::BTreeMap,
    io,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ring::hmac;
use serde::Serialize;
use serde_json::Value;
use subtle::ConstantTimeEq;
use tokio::sync::oneshot;

const LIMIT: usize = 32;
const LIFE: Duration = Duration::from_secs(300);
const ID_BYTES: usize = 12;
const MAC_BYTES: usize = 16;
const TARGET_LIMIT: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct Target {
    pub channel: String,
    pub chat: String,
    pub thread: Option<String>,
    pub session: String,
    pub tool: String,
    pub args: Value,
}

pub(crate) struct Challenge {
    pub approve: String,
    pub deny: String,
    pub answer: oneshot::Receiver<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct View {
    pub id: String,
    pub target: Target,
    pub expires: u64,
}

struct Pending {
    target: Target,
    expires: u64,
    answer: oneshot::Sender<bool>,
}

pub(crate) struct Gate {
    key: [u8; 32],
    pending: BTreeMap<String, Pending>,
}

impl Gate {
    pub fn new() -> io::Result<Self> {
        let mut key = [0_u8; 32];
        getrandom::fill(&mut key).map_err(io::Error::other)?;
        Ok(Self { key, pending: BTreeMap::new() })
    }

    pub fn issue(&mut self, target: Target) -> io::Result<Challenge> {
        let now = now()?;
        let target_size = serde_json::to_vec(&target).map_err(io::Error::other)?.len();
        if target_size > TARGET_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Approval request exceeds the size limit.",
            ));
        }
        self.pending.retain(|_, pending| pending.expires > now);
        if self.pending.len() >= LIMIT {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "Approval capacity is full."));
        }

        let mut nonce = [0_u8; ID_BYTES];
        getrandom::fill(&mut nonce).map_err(io::Error::other)?;
        let id = hex(&nonce);
        let expires = now.saturating_add(LIFE.as_secs());
        let (answer, receiver) = oneshot::channel();
        let pending = Pending { target, expires, answer };
        let approve = token(&self.key, &id, &pending, b'a')?;
        let deny = token(&self.key, &id, &pending, b'd')?;
        self.pending.insert(id, pending);
        Ok(Challenge { approve, deny, answer: receiver })
    }

    pub fn resolve(
        &mut self,
        token: &str,
        channel: &str,
        chat: &str,
        thread: Option<&str>,
        authorized: bool,
    ) -> Option<bool> {
        self.resolve_at(token, channel, chat, thread, authorized, now().ok()?)
    }

    fn resolve_at(
        &mut self,
        token: &str,
        channel: &str,
        chat: &str,
        thread: Option<&str>,
        authorized: bool,
        now: u64,
    ) -> Option<bool> {
        if !authorized {
            return None;
        }
        let (id, action, provided_signature) = decode(token)?;
        let pending = self.pending.get(id)?;
        if pending.expires <= now
            || pending.target.channel != channel
            || pending.target.chat != chat
            || (pending.target.channel == "telegram" && pending.target.thread.as_deref() != thread)
        {
            return None;
        }
        let expected = signature(&self.key, id, pending, action).ok()?;
        if !bool::from(expected.ct_eq(&provided_signature)) {
            return None;
        }

        let pending = self.pending.remove(id)?;
        let approved = action == b'a';
        pending.answer.send(approved).ok()?;
        Some(approved)
    }

    pub fn cancel(&mut self, token: &str) {
        if let Some((id, _, _)) = decode(token) {
            self.pending.remove(id);
        }
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub fn list(&mut self) -> io::Result<Vec<View>> {
        let now = now()?;
        self.pending.retain(|_, pending| pending.expires > now);
        Ok(self
            .pending
            .iter()
            .map(|(id, pending)| View {
                id: id.clone(),
                target: pending.target.clone(),
                expires: pending.expires,
            })
            .collect())
    }

    pub fn resolve_local(&mut self, id: &str, approved: bool) -> Option<bool> {
        if id.len() != ID_BYTES * 2 || id.bytes().any(|byte| !byte.is_ascii_hexdigit()) {
            return None;
        }
        let now = now().ok()?;
        if self.pending.get(id)?.expires <= now {
            self.pending.remove(id);
            return None;
        }
        self.pending.remove(id)?.answer.send(approved).ok()?;
        Some(approved)
    }
}

fn token(key: &[u8; 32], id: &str, pending: &Pending, action: u8) -> io::Result<String> {
    let signature = signature(key, id, pending, action)?;
    Ok(format!("{id}.{}.{}", hex(&signature), action as char))
}

fn signature(
    key: &[u8; 32],
    id: &str,
    pending: &Pending,
    action: u8,
) -> io::Result<[u8; MAC_BYTES]> {
    let mut value = Vec::with_capacity(128);
    value.extend_from_slice(b"crabbot-approval-v1\0");
    value.extend_from_slice(id.as_bytes());
    value.push(action);
    value.extend_from_slice(&pending.expires.to_be_bytes());
    value.extend_from_slice(&serde_json::to_vec(&pending.target).map_err(io::Error::other)?);
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let signed = hmac::sign(&key, &value);
    let mut output = [0_u8; MAC_BYTES];
    output.copy_from_slice(&signed.as_ref()[..MAC_BYTES]);
    Ok(output)
}

fn decode(value: &str) -> Option<(&str, u8, [u8; MAC_BYTES])> {
    if value.len() != ID_BYTES * 2 + 1 + MAC_BYTES * 2 + 2 || !value.is_ascii() {
        return None;
    }
    let id = value.get(..ID_BYTES * 2)?;
    let middle = value.get(ID_BYTES * 2..)?.strip_prefix('.')?;
    let (signature, action) = middle.split_once('.')?;
    if id.bytes().any(|byte| !byte.is_ascii_hexdigit())
        || signature.len() != MAC_BYTES * 2
        || signature.bytes().any(|byte| !byte.is_ascii_hexdigit())
    {
        return None;
    }
    let action = match action {
        "a" => b'a',
        "d" => b'd',
        _ => return None,
    };
    let mut bytes = [0_u8; MAC_BYTES];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&signature[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some((id, action, bytes))
}

fn hex(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

fn now() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::{Gate, ID_BYTES, Target, decode};
    use serde_json::json;

    fn target() -> Target {
        Target {
            channel: "telegram".into(),
            chat: "8".into(),
            thread: Some("4".into()),
            session: "telegram-8-thread-4".into(),
            tool: "write".into(),
            args: json!({"path": "note.txt", "text": "hello"}),
        }
    }

    #[tokio::test]
    async fn approves_a_bound_request_once() {
        let mut gate = Gate::new().unwrap();
        let challenge = gate.issue(target()).unwrap();
        assert!(challenge.approve.len() <= 64);
        assert_eq!(gate.resolve(&challenge.approve, "telegram", "8", Some("4"), true), Some(true));
        assert!(challenge.answer.await.unwrap());
        assert_eq!(gate.resolve(&challenge.approve, "telegram", "8", Some("4"), true), None);
    }

    #[tokio::test]
    async fn denies_a_bound_request_once() {
        let mut gate = Gate::new().unwrap();
        let challenge = gate.issue(target()).unwrap();
        assert_eq!(gate.resolve(&challenge.deny, "telegram", "8", Some("4"), true), Some(false));
        assert!(!challenge.answer.await.unwrap());
    }

    #[tokio::test]
    async fn lists_and_resolves_pending_requests_locally() {
        let mut gate = Gate::new().unwrap();
        let challenge = gate.issue(target()).unwrap();
        let pending = gate.list().unwrap();
        let id = pending[0].id.clone();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].target.tool, "write");
        assert_eq!(gate.resolve_local(&id, true), Some(true));
        assert!(challenge.answer.await.unwrap());
        assert!(gate.list().unwrap().is_empty());
        assert_eq!(gate.resolve_local(&id, false), None);
    }

    #[tokio::test]
    async fn resolves_local_denials() {
        let mut gate = Gate::new().unwrap();
        let challenge = gate.issue(target()).unwrap();
        let id = gate.list().unwrap()[0].id.clone();

        assert_eq!(gate.resolve_local(&id, false), Some(false));
        assert!(!challenge.answer.await.unwrap());
    }

    #[test]
    fn rejects_invalid_local_approval_ids() {
        let mut gate = Gate::new().unwrap();
        assert_eq!(gate.resolve_local("invalid", true), None);
        assert!(gate.list().unwrap().is_empty());
    }

    #[test]
    fn rejects_untrusted_mismatched_and_tampered_callbacks() {
        let mut gate = Gate::new().unwrap();
        let challenge = gate.issue(target()).unwrap();
        assert_eq!(gate.resolve(&challenge.approve, "telegram", "8", Some("4"), false), None);
        assert_eq!(gate.resolve(&challenge.approve, "telegram", "9", Some("4"), true), None);
        assert_eq!(gate.resolve(&challenge.approve, "telegram", "8", None, true), None);
        let mut changed = challenge.approve.clone().into_bytes();
        let last = changed.len() - 1;
        changed[last] = b'd';
        let changed = String::from_utf8(changed).unwrap();
        assert_eq!(gate.resolve(&changed, "telegram", "8", Some("4"), true), None);
        assert_eq!(gate.resolve(&challenge.approve, "discord", "8", Some("4"), true), None);
    }

    #[test]
    fn expires_pending_requests_and_bounds_tokens() {
        let mut gate = Gate::new().unwrap();
        let challenge = gate.issue(target()).unwrap();
        let (id, action, signature) = decode(&challenge.approve).unwrap();
        assert_eq!(id.len(), ID_BYTES * 2);
        assert_eq!(action, b'a');
        assert_eq!(signature.len(), 16);
        let pending = gate.pending.get_mut(id).unwrap();
        pending.expires = 1;
        assert_eq!(gate.resolve_at(&challenge.approve, "telegram", "8", Some("4"), true, 2), None);
        assert_eq!(gate.pending.len(), 1);
        assert!(decode("invalid").is_none());
    }

    #[test]
    fn removes_canceled_requests() {
        let mut gate = Gate::new().unwrap();
        let challenge = gate.issue(target()).unwrap();
        gate.cancel(&challenge.approve);
        assert!(gate.pending.is_empty());
    }

    #[test]
    fn bounds_pending_requests_and_arguments() {
        let mut gate = Gate::new().unwrap();
        let mut challenges = Vec::new();

        for _ in 0..super::LIMIT {
            challenges.push(gate.issue(target()).unwrap());
        }

        let full = gate.issue(target()).err().unwrap();
        assert_eq!(full.kind(), std::io::ErrorKind::WouldBlock);
        assert!(challenges.iter().all(|challenge| challenge.approve.len() <= 64));

        let mut oversized = target();
        oversized.args = serde_json::json!({"text": "x".repeat(super::TARGET_LIMIT)});
        let large = gate.issue(oversized).err().unwrap();
        assert_eq!(large.kind(), std::io::ErrorKind::InvalidData);

        for challenge in challenges {
            gate.cancel(&challenge.approve);
        }
        assert!(gate.pending.is_empty());
    }
}
