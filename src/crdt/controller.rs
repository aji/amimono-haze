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
    ring::{HashRing, NetworkId, RingConfig, RingUpdateConfig, VirtualNodeId},
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
            update: None,
        }
    }
}

fn mk_virtual_node(NetworkId(ni): &NetworkId, i: usize) -> VirtualNodeId {
    VirtualNodeId(format!("{ni}/{i:02x}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
            ActualConfig::Configured(x) => Some(x),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct ClusterConfig {
    ring: RingConfig,
}

impl ClusterConfig {
    fn parse(known: &HashMap<NetworkId, ActualConfig>) -> CtlResult<ClusterConfig> {
        let configs: Vec<ClusterConfig> = {
            let mut configs = Vec::new();
            for v in known.values() {
                if let ActualConfig::Configured(ring) = v {
                    configs.push(Self::parse_one(ring)?);
                }
            }
            configs
        };

        let update = {
            let mut updates = configs.iter().flat_map(|x| x.ring.update.clone());
            let update = updates.next();
            if let Some(u) = updates.next() {
                Err(format!("cluster has multiple active updates: {:?}", u))?;
            }
            update
        };

        let first = configs
            .iter()
            .next()
            .cloned()
            .ok_or(format!("no configs"))?;
        let mut nodes_with_update = first.ring.nodes.clone();
        let mut nodes_without_update = first.ring.nodes.clone();

        if let Some(u) = update.as_ref() {
            match u {
                RingUpdateConfig::ToAdd { vn, ni } => {
                    nodes_without_update.remove(vn);
                    nodes_with_update.insert(vn.clone(), ni.clone());
                }
                RingUpdateConfig::ToRemove { vn, ni } => {
                    nodes_with_update.remove(vn);
                    nodes_without_update.insert(vn.clone(), ni.clone());
                }
            }
        }

        for cc in configs.iter() {
            if (cc.ring.nodes != nodes_with_update && cc.ring.nodes != nodes_without_update)
                || (cc.ring.update != None && cc.ring.update != update)
            {
                Err(format!("cluster has inconsistent config"))?;
            }
        }

        Ok(ClusterConfig {
            ring: RingConfig {
                nodes: nodes_without_update,
                update,
            },
        })
    }

    fn parse_one(ring: &RingConfig) -> CtlResult<ClusterConfig> {
        Ok(ClusterConfig { ring: ring.clone() })
    }
}

enum Action {
    Nothing,
    BootstrapAll,
    BootstrapOne(NetworkId, RingConfig),
    BeginAdd(ClusterConfig, VirtualNodeId, NetworkId),
    TryFinishAdd(ClusterConfig, VirtualNodeId, NetworkId),
}

enum NextIter {
    Fast,
    Wait,
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

    async fn push_config_force(&mut self, to: &NetworkId, cf: RingConfig) -> CtlResult<()> {
        match self.router.at(to.as_location()).set_ring(cf.clone()).await {
            Ok(_) => {
                log::info!("updated config at {to:?}");
                self.known.insert(to.clone(), ActualConfig::Configured(cf));
                Ok(())
            }
            Err(e) => {
                log::info!("failed to update config at {to:?}: {e:?}");
                self.known.remove(&to);
                Err(format!("{e:?}"))
            }
        }
    }

    async fn push_config(&mut self, to: &NetworkId, cf: RingConfig) -> CtlResult<()> {
        if self.known.get(to).and_then(|x| x.as_config()) == Some(&cf) {
            log::debug!("skipping push_config for {to:?}: known config matches");
            Ok(())
        } else {
            self.push_config_force(to, cf).await
        }
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

    fn action(&self, desired: &DesiredConfig) -> CtlResult<Action> {
        use Action::*;

        let configured: Vec<NetworkId> = self
            .known
            .iter()
            .filter(|(_, cf)| cf.is_configured())
            .map(|(ni, _)| ni)
            .cloned()
            .collect();
        let unconfigured: Vec<NetworkId> = self
            .known
            .iter()
            .filter(|(_, cf)| cf.is_unconfigured())
            .map(|(ni, _)| ni)
            .cloned()
            .collect();

        // If no nodes are configured, then we just do a full bootstrap.
        if configured.is_empty() {
            return Ok(BootstrapAll);
        }

        // Otherwise we have to decide on a course of action. The first thing
        // we'll do is verify that the current state is something we can work
        // with, by consolidating all the various configs.
        let cc = ClusterConfig::parse(&self.known)?;

        // First we check if there are any unconfigured nodes. If so, we'll
        // simply bootstrap them.
        if let Some(ni) = unconfigured.iter().next() {
            let ring = RingConfig {
                nodes: cc.ring.nodes.clone(),
                update: None,
            };
            return Ok(BootstrapOne(ni.clone(), ring));
        }

        // Then we check if there is anything in progress. All we need to do is
        // continue those.
        if let Some(u) = cc.ring.update.clone() {
            use RingUpdateConfig::*;
            let action = match u {
                ToAdd { vn, ni } => TryFinishAdd(cc, vn, ni),
                ToRemove { .. } => todo!(),
            };
            return Ok(action);
        }

        // If not, we'll check if there's anything we can start doing.
        if let Some(action) = self.action_to_start(cc, desired)? {
            return Ok(action);
        }

        // Otherwise there is nothing for us to do!
        Ok(Nothing)
    }

    fn action_to_start(
        &self,
        cc: ClusterConfig,
        desired: &DesiredConfig,
    ) -> CtlResult<Option<Action>> {
        let target = desired.as_ring_config();

        for (vn, ni) in target.nodes.into_iter() {
            if !cc.ring.nodes.contains_key(&vn) {
                return Ok(Some(Action::BeginAdd(cc, vn, ni)));
            }
        }

        Ok(None)
    }

    async fn do_bootstrap_all(&mut self, desired: &DesiredConfig) -> CtlResult<()> {
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

    async fn do_begin_add(
        &mut self,
        cc: ClusterConfig,
        vn: VirtualNodeId,
        ni: NetworkId,
    ) -> CtlResult<()> {
        let ring = HashRing::from_nodes(cc.ring.nodes.keys().cloned());

        let old_vn = ring.cursor(&vn).get();
        let old_ni = cc.ring.network_id(&old_vn).ok_or("no ni for target vn")?;

        let cf = RingConfig {
            nodes: cc.ring.nodes.clone(),
            update: Some(RingUpdateConfig::ToAdd { vn, ni }),
        };

        self.push_config(old_ni, cf).await
    }

    async fn do_try_finish_add(
        &mut self,
        cc: ClusterConfig,
        vn: VirtualNodeId,
        ni: NetworkId,
    ) -> CtlResult<NextIter> {
        let ring = HashRing::from_nodes(cc.ring.nodes.keys().cloned());

        let old_vn = ring.cursor(&vn).get();
        let old_ni = cc.ring.network_id(&old_vn).ok_or("no ni for target vn")?;

        let updating = self
            .router
            .at(old_ni.as_location())
            .updating()
            .await
            .map_err(|e| format!("failed to check if {old_ni:?} is updating: {e:?}"))?;

        if updating {
            log::debug!("{old_ni:?} still updating...");
            return Ok(NextIter::Wait);
        }

        let cf = {
            let mut cf = RingConfig {
                nodes: cc.ring.nodes.clone(),
                update: None,
            };
            cf.nodes.insert(vn, ni);
            cf
        };

        let others = self
            .known
            .keys()
            .filter(|x| *x != old_ni)
            .cloned()
            .collect::<HashSet<NetworkId>>();

        for ni in others.iter() {
            self.push_config(ni, cf.clone()).await?;
        }

        self.push_config(old_ni, cf).await?;
        Ok(NextIter::Fast)
    }

    async fn run_once(&mut self) -> CtlResult<NextIter> {
        log::debug!("getting desired config");
        let desired = self.get_desired_config().await?;

        log::debug!("updating actual config");
        self.update_actual_config(&desired).await?;

        match self.action(&desired)? {
            Action::Nothing => {
                log::debug!("nothing to do");
                Ok(NextIter::Wait)
            }
            Action::BootstrapAll => {
                log::info!("cluster bootstrap all");
                self.do_bootstrap_all(&desired).await?;
                Ok(NextIter::Fast)
            }
            Action::BootstrapOne(ni, ring) => {
                log::info!("cluster bootstrap one: {ni:?}");
                self.push_config(&ni, ring).await?;
                Ok(NextIter::Fast)
            }
            Action::BeginAdd(cc, vn, ni) => {
                log::info!("starting to-add {vn:?} -> {ni:?}");
                self.do_begin_add(cc, vn, ni).await?;
                Ok(NextIter::Fast)
            }
            Action::TryFinishAdd(cc, vn, ni) => {
                log::debug!("checking to-add {vn:?} -> {ni:?}");
                self.do_try_finish_add(cc, vn, ni).await
            }
        }
    }

    fn main() -> BoxFuture<'static, ()> {
        Box::pin(async {
            let mut controller = Controller::new();
            loop {
                let iter = match controller.run_once().await {
                    Ok(x) => x,
                    Err(e) => {
                        log::warn!("controller iter failed: {e}");
                        NextIter::Wait
                    }
                };
                let delay = match iter {
                    NextIter::Fast => Duration::from_millis(100),
                    NextIter::Wait => Duration::from_secs(5),
                };
                tokio::time::sleep(delay).await;
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
