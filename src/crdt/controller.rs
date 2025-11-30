use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use amimono::{
    config::{Binding, ComponentConfig},
    runtime::{self, Component, Location},
};
use futures::future::BoxFuture;

use crate::crdt::{
    ring::{HashRing, NetworkId, RingConfig, VirtualNodeId},
    router::{CrdtRouterClient, CrdtRouterComponent},
};

const DEFAULT_WEIGHT: usize = 16;

type CtlResult<T> = Result<T, String>;

struct DesiredConfig {
    weight: HashMap<NetworkId, usize>,
}

impl DesiredConfig {
    fn from_nodes(it: impl Iterator<Item = NetworkId>) -> DesiredConfig {
        let weight = it.map(|loc| (loc, DEFAULT_WEIGHT)).collect();
        DesiredConfig { weight }
    }

    fn is_empty(&self) -> bool {
        self.weight.is_empty()
    }

    fn as_ring_config(&self) -> RingConfig {
        let mut nodes = HashMap::new();
        for (ni, w) in self.weight.iter() {
            for i in 0..*w {
                let vn = mk_virtual_node(ni, i);
                nodes.insert(vn, ni.clone());
            }
        }
        RingConfig {
            nodes,
            to_add: HashSet::new(),
            to_remove: HashSet::new(),
        }
    }
}

fn mk_virtual_node(NetworkId(ni): &NetworkId, i: usize) -> VirtualNodeId {
    VirtualNodeId(format!("{i}:{ni}"))
}

enum ActualConfig {
    Configured(RingConfig),
    Unconfigured,
}

impl ActualConfig {
    fn is_configured(&self) -> bool {
        match self {
            ActualConfig::Configured(_) => true,
            _ => false,
        }
    }

    fn is_unconfigured(&self) -> bool {
        match self {
            ActualConfig::Unconfigured => true,
            _ => false,
        }
    }

    fn as_config(&self) -> Option<&RingConfig> {
        match self {
            ActualConfig::Configured(config) => Some(config),
            _ => None,
        }
    }
}

enum Action {
    None,
    Bootstrap,
    BeginAdd(VirtualNodeId, NetworkId),
    TryFinishAdd(VirtualNodeId, NetworkId, NetworkId),
}

struct Controller {
    known: HashMap<NetworkId, ActualConfig>,
    router: CrdtRouterClient,
}

impl Controller {
    fn new() -> Controller {
        Controller {
            known: HashMap::new(),
            router: CrdtRouterClient::new(),
        }
    }

    async fn push_config(&mut self, to: &NetworkId, cf: RingConfig) -> CtlResult<()> {
        match self.router.at(to.as_location()).set_ring(cf.clone()).await {
            Ok(_) => {
                log::info!("updated config at {to:?}");
                self.known.insert(to.clone(), ActualConfig::Configured(cf));
            }
            Err(e) => {
                log::info!("failed to update config at {to:?}: {e:?}");
                self.known.remove(&to);
            }
        }
        Ok(())
    }

    async fn get_desired_config(&self) -> CtlResult<DesiredConfig> {
        let routers = runtime::discover::<CrdtRouterComponent>()
            .await
            .map_err(|_| "failed to discover routers")?
            .into_iter()
            .flat_map(|x| match x {
                Location::Ephemeral(x) => {
                    log::warn!("node {x} has ephemeral location: ignored");
                    None
                }
                Location::Stable(x) => Some(NetworkId(x)),
            })
            .collect::<Vec<_>>();
        if routers.len() == 0 {
            Err("no routers! cannot calculate desired config")?;
        }
        Ok(DesiredConfig::from_nodes(routers.into_iter()))
    }

    async fn update_actual_config(&mut self, desired: &DesiredConfig) -> CtlResult<()> {
        for router in desired.weight.keys() {
            if self.known.contains_key(&router) {
                log::debug!("skipping {router:?}: config already known");
            } else {
                log::debug!("fetching config from {router:?}");
                let ring = self
                    .router
                    .at(router.as_location())
                    .get_ring()
                    .await
                    .map_err(|e| format!("failed to get ring at {router:?}: {e:?}"))?;
                let actual = match ring {
                    Some(r) => ActualConfig::Configured(r),
                    None => ActualConfig::Unconfigured,
                };
                self.known.insert(router.clone(), actual);
            }
        }
        Ok(())
    }

    fn known_configured(&self) -> impl Iterator<Item = (&NetworkId, &RingConfig)> {
        self.known.iter().flat_map(|(k, v)| match v.as_config() {
            Some(cf) => Some((k, cf)),
            None => None,
        })
    }

    fn action(&self, desired: &DesiredConfig) -> CtlResult<Action> {
        // The implied config is determined by which nodes are currently
        // configured. In the steady state, all configured nodes have a
        // configuration that matches the implied config. The remaining
        // unconfigured nodes in the desired config are to be added.
        let implied = {
            let configured = self
                .known
                .iter()
                .filter(|(_, cf)| cf.is_configured())
                .map(|(ni, _)| ni)
                .cloned();
            DesiredConfig::from_nodes(configured)
        };

        // If the implied configuration is empty, i.e. no nodes are configured,
        // then we can just do a bootstrap.
        if implied.is_empty() {
            return Ok(Action::Bootstrap);
        }

        // Otherwise we have to decide on a course of action. The first thing
        // we'll do is verify that the current state is something we can work
        // with.
        self.action_preconditions(&implied, desired)?;

        // First we check if there is anything in progress. If so, we'll start
        // there.
        if let Some(action) = self.action_in_progress(&implied, desired)? {
            return Ok(action);
        }

        // If not, we'll check if there's anything we can start doing.
        if let Some(action) = self.action_to_start(&implied, desired)? {
            return Ok(action);
        }

        // Otherwise there is nothing for us to do!
        Ok(Action::None)
    }

    fn action_preconditions(
        &self,
        implied: &DesiredConfig,
        desired: &DesiredConfig,
    ) -> CtlResult<()> {
        // every time a to_add vn appears in a node's RingConfig, we need to
        // verify that either the vn's range belongs to the node itself, or to
        // the previous node in the ring, since these are the two nodes involved
        // in the to-add operation
        for (ni, cf) in self.known_configured() {
            let ring = HashRing::from_config(cf);
            let n = cf.to_add.len();
            if n > 1 {
                Err(format!("{ni:?} config has to_add.len() > 1"))?;
            }
            if let Some(vn) = cf.to_add.iter().next() {
                let cur0 = ring.cursor(vn);
                let cur1 = cur0.prev();
                let vn0 = cur0.get();
                let vn1 = cur1.get();
                let ni0 = cf
                    .network_id(vn0)
                    .ok_or(format!("precond: {ni:?} has unknown vnid {vn0:?}"))?;
                let ni1 = cf
                    .network_id(vn1)
                    .ok_or(format!("precond: {ni:?} has unknown vnid {vn1:?}"))?;
                if ni != ni0 && ni != ni1 {
                    Err(format!(
                        "precond: to_add {vn:?} is on the wrong node {ni:?}"
                    ))?;
                }
            }
        }

        Ok(())
    }

    fn action_in_progress(
        &self,
        implied: &DesiredConfig,
        desired: &DesiredConfig,
    ) -> CtlResult<Option<Action>> {
        // there can be at most one ongoing to-add in the cluster at a time. in
        // these situations, both nodes have the to-add config

        let mut to_add: HashSet<(NetworkId, VirtualNodeId)> = HashSet::new();
        for (ni0, cf) in self.known_configured() {
            for vn in cf.to_add.iter() {
                let ni = cf
                    .network_id(vn)
                    .ok_or(format!("progress: {ni0:?} has unknown vnid {vn:?}"))?;
                to_add.insert((ni.clone(), vn.clone()));
            }
        }

        if to_add.len() == 0 {
            // do nothing
        } else if to_add.len() > 1 {
            return Err(format!("too many ongoing to-adds: {to_add:?}"));
        } else {
            // BIG TODO HERE
        }

        Ok(None)
    }

    fn action_to_start(
        &self,
        implied: &DesiredConfig,
        desired: &DesiredConfig,
    ) -> CtlResult<Option<Action>> {
        // we may need to start adding a new virtual node. the new node can be
        // either an additional virtual node that isn't
        Ok(None)
    }

    async fn do_bootstrap(&mut self, desired: &DesiredConfig) -> CtlResult<()> {
        let cf = desired.as_ring_config();

        // It's possible for this to fail if one of the bootstrap nodes has died
        // between its get_ring() call and now, but in a bootstrap scenario data
        // loss and manual intervention are deemed acceptable.
        log::debug!("pushing bootstrap config to all nodes");
        for ni in desired.weight.keys() {
            self.push_config(ni, cf.clone()).await?;
        }

        Ok(())
    }

    async fn run_once(&mut self) -> CtlResult<()> {
        log::debug!("getting desired config");
        let desired = self.get_desired_config().await?;

        log::debug!("updating actual config");
        self.update_actual_config(&desired).await?;

        match self.action(&desired)? {
            Action::None => {
                log::debug!("nothing to do");
                Ok(())
            }
            Action::Bootstrap => {
                log::info!("cluster bootstrap");
                self.do_bootstrap(&desired).await
            }
            Action::BeginAdd(vn, ni) => {
                log::info!("adding {vn:?} -> {ni:?}");
                Ok(())
            }
            Action::TryFinishAdd(vn, ni0, ni1) => {
                log::debug!("checking {vn:?} -> {ni0:?}, {ni1:?}");
                Ok(())
            }
        }
    }

    fn main() -> BoxFuture<'static, ()> {
        Box::pin(async {
            let mut controller = Controller::new();
            loop {
                if let Err(e) = controller.run_once().await {
                    log::warn!("controller iter failed: {e:?}");
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        })
    }
}

impl Component for Controller {
    type Instance = ();
}

pub fn component(prefix: &str) -> ComponentConfig {
    ComponentConfig {
        label: format!("{prefix}-crdt-controller"),
        id: Controller::id(),
        binding: Binding::None,
        is_stateful: false,
        entry: Controller::main,
    }
}
