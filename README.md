# Penelope

Penelope runs Given/When/Then scenarios against systems that become
consistent over time, such as a cluster of replicated nodes.

In the myth, Penelope waited twenty years for Odysseus. When he came home,
she tested him before she accepted him. This crate does the same thing. It
waits for the system to settle, and then it checks the result.

## Why

A distributed system rarely shows a change at once. A test that checks too
early fails. A fixed sleep wastes time, and the test can still fail on a slow
day. Penelope polls each Then step until the expected state appears. It also
records why each attempt failed, so a timeout says what went wrong.

## Install

Add the crate as a git dependency, and pin a commit:

```toml
[dev-dependencies]
penelope = { git = "https://github.com/DLC-link/penelope", rev = "<commit>" }
```

The pin means your tests change only when you move it to a new commit.

## Example

The fixture below is an order store that applies each write after a delay.
The Then step polls the store until the order appears.

```rust
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use penelope::{Fixture, ProbeDiag, Scenario};

#[derive(Default)]
struct World {
    diag: ProbeDiag,
    orders: Arc<Mutex<Vec<String>>>,
}

impl World {
    async fn count_orders(&self) -> anyhow::Result<usize> {
        Ok(self.orders.lock().unwrap().len())
    }
}

impl Fixture for World {
    fn probe_diag(&self) -> &ProbeDiag {
        &self.diag
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut world = World::default();

    Scenario::<World>::new("a placed order becomes visible")
        .given("the store is empty", |w, _| {
            Box::pin(async move {
                anyhow::ensure!(w.count_orders().await? == 0, "the store is not empty");
                Ok(())
            })
        })
        .when("a client places an order", |w, _| {
            let orders = w.orders.clone();
            Box::pin(async move {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    orders.lock().unwrap().push("order-1".to_string());
                });
                Ok(())
            })
        })
        .then("the store lists one order", Duration::from_secs(10), |w, _| {
            let diag = w.probe_diag().clone();
            Box::pin(async move {
                let count = diag.ok("count orders", w.count_orders().await)?;
                (count == 1).then_some(Ok(()))
            })
        })
        .run(&mut world)
        .await
}
```

## Step kinds

| Step  | How often it runs                       | Use it for                                     |
|-------|-----------------------------------------|------------------------------------------------|
| Given | Once                                    | State that the test knows or can check first   |
| When  | Once                                    | An action on the system under test             |
| Then  | Until it passes, fails, or times out    | An observation, such as "node 2 sees the row"  |

A Then probe returns one of three values:

- `Some(Ok(()))` passes the step.
- `None` means "not observable yet". The runner tries again after 2 seconds.
- `Some(Err(e))` fails the step at once.

Steps can share state through a context value. Start the scenario with
`Scenario::with_ctx(name, ctx)`. Each step then receives `&mut ctx` as its
second argument. A Then probe can store what it observed there for a later
step.

## Probe diagnostics

A probe that returns `None` gives the runner no reason. Without a reason, a
typo in a probe's request and a slow server both end in a bare "timed out".

The fixture's `ProbeDiag` holds that reason. A probe records each failure
against a key, such as `GET /orders`:

- `record(key, Class::Transient, message)` notes a failure that can still
  clear, such as a restart or a 503.
- `record(key, Class::Fatal, message)` notes a failure that retries cannot
  fix, such as a route that does not exist.
- `record_ok(key)` clears the streak for that key.
- `ok(key, result)` records an `Err` as transient and returns `None`.
  Otherwise it returns the value.

The runner ends a Then step early when the same fatal failure repeats
`FATAL_STREAK` times in a row. That limit is 3. On a timeout, the error
message includes the last recorded failure. The probe chooses each failure's
class, because only the probe knows its protocol.

## Naming the fixture type once

A closure that reads a fixture field needs the fixture type. Declare a type
alias once in your test suite:

```rust,ignore
pub type Scenario<Ctx = ()> = penelope::Scenario<MyFixture, Ctx>;
```

After that, `Scenario::new("...")` works without a type annotation.

## Logging

The runner logs each step through `tracing` at the `info` level, under the
target `penelope`. If you set `RUST_LOG` to a list of targets, add
`penelope=info` to the list.

## Origin

Penelope began as the integration-test DSL of
[decentralization-manager](https://github.com/DLC-link/decentralization-manager).

## License

Apache-2.0. See the `LICENSE` and `NOTICE` files.
