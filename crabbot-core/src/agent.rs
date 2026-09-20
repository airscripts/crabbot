use crate::{
    Error, Result,
    types::{Content, Event, Message, Role},
};

use serde_json::Value;

const LIMIT: usize = 256;
const CALLS: usize = 16;

pub trait Model {
    fn reply(&mut self, messages: &[Message]) -> Result<Vec<Event>>;
}

pub trait Tool {
    fn call(&mut self, name: &str, args: &Value, approved: bool) -> Result<String>;
}

pub fn turn<M: Model>(model: &mut M, messages: &[Message], steps: usize) -> Result<Message> {
    if steps == 0 {
        return Err(Error::Limit("steps must be greater than zero".into()));
    }

    let events = model.reply(messages)?;
    let mut text = String::new();

    for event in events.into_iter().take(steps) {
        match event {
            Event::Text { text: part } => text.push_str(&part),
            Event::Done { text: done } => text = done,
            Event::Error { message } => return Err(Error::Denied(message)),

            Event::Tool { .. } => {}
        }
    }

    Ok(Message {
        id: "reply".into(),
        session: messages.first().map_or_else(|| "session".into(), |m| m.session.clone()),
        role: Role::Assistant,
        sender: None,
        content: vec![Content::Text { text }],
    })
}

pub fn run<M: Model, T: Tool>(
    model: &mut M,
    tools: &mut T,
    messages: &[Message],
    steps: usize,
) -> Result<Message> {
    if steps == 0 {
        return Err(Error::Limit("steps must be greater than zero".into()));
    }

    let mut history = messages.to_vec();

    let mut used = 0_usize;

    for step in 0..steps {
        let events = model.reply(&history)?;
        let mut text = String::new();
        let mut calls = Vec::new();
        let mut called = false;

        for event in events {
            match event {
                Event::Text { text: part } => text.push_str(&part),
                Event::Done { text: done } => text = done,
                Event::Error { message } => return Err(Error::Denied(message)),

                Event::Tool { name, args } => {
                    calls.push((name, args));
                    called = true;
                }
            }
        }

        if called {
            let marker = calls
                .iter()
                .map(|(name, args)| format!("[Tool call {name}]: {args}"))
                .collect::<Vec<_>>()
                .join("\n");

            if !text.is_empty() {
                text.push('\n');
            }

            text.push_str(&marker);
            history.push(Message {
                id: format!("assistant-tool-{step}-{}", history.len()),
                session: history.first().map_or_else(|| "session".into(), |m| m.session.clone()),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text { text: text.clone() }],
            });

            if history.len() > LIMIT {
                history.drain(..history.len() - LIMIT);
            }
        }

        for (name, args) in calls {
            used = used.saturating_add(1);

            if used > CALLS {
                return Err(Error::Limit("tool-call limit exceeded".into()));
            }

            // Model output never grants approval; hosts must apply policy before invoking tools.
            let result = tools.call(&name, &args, false)?;
            history.push(Message {
                id: format!("tool-{step}-{}", history.len()),
                session: history.first().map_or_else(|| "session".into(), |m| m.session.clone()),
                role: Role::Tool,
                sender: Some(name),
                content: vec![Content::Text { text: result }],
            });

            if history.len() > LIMIT {
                history.drain(..history.len() - LIMIT);
            }
        }

        if !called {
            return Ok(Message {
                id: "reply".into(),
                session: history.first().map_or_else(|| "session".into(), |m| m.session.clone()),
                role: Role::Assistant,
                sender: None,
                content: vec![Content::Text { text }],
            });
        }
    }

    Err(Error::Limit("tool steps exhausted".into()))
}

#[cfg(test)]
mod tests {
    use super::{Event, Model, Tool, run, turn};
    use crate::types::{Content, Message, Role};

    struct Echo;

    impl Model for Echo {
        fn reply(&mut self, _: &[Message]) -> crate::Result<Vec<Event>> {
            Ok(vec![Event::Text { text: "hello".into() }, Event::Done { text: "done".into() }])
        }
    }

    struct Failure;

    impl Model for Failure {
        fn reply(&mut self, _: &[Message]) -> crate::Result<Vec<Event>> {
            Err(crate::Error::Denied("model failed".into()))
        }
    }

    struct Calls {
        count: usize,
    }

    impl Model for Calls {
        fn reply(&mut self, messages: &[Message]) -> crate::Result<Vec<Event>> {
            if messages.iter().any(|message| message.role == Role::Tool) {
                return Ok(vec![Event::Done { text: "finished".into() }]);
            }

            self.count += 1;
            Ok(vec![Event::Tool { name: "read".into(), args: serde_json::json!({"path": "note"}) }])
        }
    }

    struct Read;

    impl Tool for Read {
        fn call(
            &mut self,
            name: &str,
            args: &serde_json::Value,
            approved: bool,
        ) -> crate::Result<String> {
            assert_eq!(name, "read");
            assert_eq!(args["path"], "note");
            assert!(!approved);
            Ok("content".into())
        }
    }

    struct Mixed;

    impl Model for Mixed {
        fn reply(&mut self, _: &[Message]) -> crate::Result<Vec<Event>> {
            Ok(vec![Event::Text { text: "part".into() }, Event::Error { message: "failed".into() }])
        }
    }

    struct Flood;

    impl Model for Flood {
        fn reply(&mut self, _: &[Message]) -> crate::Result<Vec<Event>> {
            Ok(vec![Event::Tool { name: "read".into(), args: serde_json::json!({"path": "note"}) }])
        }
    }

    struct BrokenTool;

    impl Tool for BrokenTool {
        fn call(&mut self, _: &str, _: &serde_json::Value, _: bool) -> crate::Result<String> {
            Err(crate::Error::Denied("tool failed".into()))
        }
    }

    #[test]
    fn bounded_turn_returns_done() {
        let input = Message {
            id: "1".into(),
            session: "s".into(),
            role: Role::User,
            sender: None,
            content: vec![Content::Text { text: "hi".into() }],
        };

        let reply = turn(&mut Echo, &[input], 4).unwrap();
        assert_eq!(reply.content, vec![Content::Text { text: "done".into() }]);
    }

    #[test]
    fn rejects_empty_steps_and_model_errors() {
        let input = Message {
            id: "1".into(),
            session: "s".into(),
            role: Role::User,
            sender: None,
            content: vec![Content::Text { text: "hi".into() }],
        };

        assert!(turn(&mut Echo, std::slice::from_ref(&input), 0).is_err());
        assert!(turn(&mut Failure, std::slice::from_ref(&input), 1).is_err());

        struct Events;

        impl Model for Events {
            fn reply(&mut self, _: &[Message]) -> crate::Result<Vec<Event>> {
                Ok(vec![
                    Event::Tool { name: "read".into(), args: serde_json::json!({}) },
                    Event::Error { message: "event failed".into() },
                ])
            }
        }

        assert!(turn(&mut Events, &[], 2).is_err());
        let reply = turn(&mut Echo, &[], 1).unwrap();
        assert_eq!(reply.session, "session");
        assert!(turn(&mut Mixed, std::slice::from_ref(&input), 2).is_err());
    }

    #[test]
    fn runs_tools_with_a_bounded_step_limit() {
        let input = Message {
            id: "1".into(),
            session: "s".into(),
            role: Role::User,
            sender: None,
            content: vec![Content::Text { text: "hi".into() }],
        };

        let mut model = Calls { count: 0 };
        let reply = run(&mut model, &mut Read, std::slice::from_ref(&input), 2).unwrap();
        assert_eq!(reply.content, vec![Content::Text { text: "finished".into() }]);
        assert_eq!(model.count, 1);
        assert!(run(&mut Calls { count: 0 }, &mut Read, &[], 0).is_err());
        assert!(run(&mut Calls { count: 0 }, &mut Read, &[], 1).is_err());
        assert!(run(&mut Mixed, &mut Read, std::slice::from_ref(&input), 1).is_err());

        assert!(
            run(&mut Calls { count: 0 }, &mut BrokenTool, std::slice::from_ref(&input), 1).is_err()
        );

        assert!(run(&mut Flood, &mut Read, &[], 257).is_err());
    }
}
