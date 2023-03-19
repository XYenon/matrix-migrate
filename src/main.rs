use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use futures::{
    future::{join_all, try_join_all},
    pin_mut, try_join, StreamExt,
};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use log::{debug, error, info, warn};
use matrix_sdk::ruma::api::client::session::get_login_types::v3::LoginType;
use matrix_sdk::{
    config::SyncSettings,
    ruma::{OwnedRoomId, OwnedServerName, OwnedUserId},
    Client,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc::channel;

/// Fast migration of one matrix account to another
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Username of the account to migrate from
    #[arg(long = "from", env = "FROM_USER")]
    from_user: OwnedUserId,

    /// Password of the account to migrate from
    #[arg(long = "from-pw", env = "FROM_PASSWORD")]
    from_user_password: Option<String>,

    /// Custom homeserver, if not defined discovery is used
    #[arg(long, env = "FROM_HOMESERVER")]
    from_homeserver: Option<OwnedServerName>,

    /// Username of the given account to migrate to
    #[arg(long = "to", env = "TO_USER")]
    to_user: OwnedUserId,

    /// Password of the account to migrate from
    #[arg(long = "to-pw", env = "TO_PASSWORD")]
    to_user_password: Option<String>,

    /// Custom homeserver, if not defined discovery is used
    #[arg(long, env = "TO_HOMESERVER")]
    to_homeserver: Option<OwnedServerName>,

    // Whether to include direct chats
    #[arg(long, env = "INCLUDE_DIRECT", default_value = "false")]
    include_direct: bool,

    /// Custom logging info
    #[arg(long, env = "RUST_LOG", default_value = "matrix_migrate=info")]
    log: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    env_logger::Builder::new().parse_filters(&args.log).init();

    let from_cb = Client::builder().user_agent("matrix-migrate/1");
    let from_c = if let Some(h) = args.from_homeserver {
        from_cb.server_name(&h).build().await?
    } else {
        from_cb
            .server_name(args.from_user.server_name())
            .build()
            .await?
    };

    info!("Logging in {:}", args.from_user);

    login(&from_c, args.from_user, args.from_user_password).await?;

    let to_cb = Client::builder().user_agent("matrix-migrate/1");
    let to_c = if let Some(h) = args.to_homeserver {
        to_cb.server_name(&h).build().await?
    } else {
        to_cb
            .server_name(args.to_user.server_name())
            .build()
            .await?
    };

    info!("Logging in {:}", args.to_user);

    login(&to_c, args.to_user, args.to_user_password).await?;

    info!("All logged in. Syncing...");

    let to_c_stream = to_c.clone();
    let to_sync_stream = to_c_stream.sync_stream(SyncSettings::default()).await;
    pin_mut!(to_sync_stream);

    try_join!(from_c.sync_once(SyncSettings::default()), async {
        to_sync_stream.next().await.unwrap()
    })?;

    info!("--- Synced");

    let all_prev_rooms = from_c
        .joined_rooms()
        .into_iter()
        .filter(|r| args.include_direct || !r.is_direct())
        .map(|r| r.room_id().to_owned())
        .collect::<Vec<_>>();

    let all_new_rooms = to_c
        .joined_rooms()
        .into_iter()
        .map(|r| r.room_id().to_owned())
        .chain(
            to_c.invited_rooms()
                .into_iter()
                .map(|r| r.room_id().to_owned()),
        )
        .collect::<Vec<_>>();

    let (already_invited, to_invite): (Vec<_>, Vec<_>) = all_prev_rooms
        .iter()
        .partition(|r| all_new_rooms.contains(r));

    let invites_to_accept = to_c
        .invited_rooms()
        .into_iter()
        .filter_map(|r| {
            let room_id = r.room_id().to_owned();
            if all_prev_rooms.contains(&room_id) {
                Some(room_id)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    info!(
        "--- Already sharing {}; Rooms to accept: {};  Rooms to invite: {}",
        already_invited.len(),
        invites_to_accept.len(),
        to_invite.len()
    );

    let to_user = to_c.user_id().unwrap().to_owned();
    let to_accept = invites_to_accept.iter().collect();
    let c_accept = to_c.clone();
    let ensure_user = to_user.clone();
    let ensure_c = from_c.clone();
    let inviter_c = from_c.clone();

    let (_, not_yet_accepted, (remaining_invites, failed_invites)) = try_join!(
        async move { ensure_power_levels(&ensure_c, ensure_user, &already_invited).await },
        async move { accept_invites(&c_accept, &to_accept).await },
        async move {
            let to_invite = to_invite.clone();
            let failed_invites = send_invites(&inviter_c, &to_invite, to_user.clone()).await?;
            ensure_power_levels(&inviter_c, to_user.clone(), &to_invite).await?;
            Ok((
                to_invite
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .filter(|r| !failed_invites.contains(r))
                    .collect::<Vec<_>>(),
                failed_invites,
            ))
        },
    )?;

    let mut invites_awaiting = not_yet_accepted
        .into_iter()
        .chain(remaining_invites.into_iter())
        .collect::<Vec<_>>();

    info!("First invitation set done.");
    while !invites_awaiting.is_empty() {
        info!("Still {} rooms to go. Syncing up", invites_awaiting.len());
        to_sync_stream.next().await.expect("Sync stream broke")?;
        invites_awaiting = accept_invites(&to_c, &invites_awaiting.iter().collect()).await?;
    }

    if !failed_invites.is_empty() {
        warn!(
            "Failed to invite to {:?}. See logs above for the reasons why",
            failed_invites
        );
    }

    to_c.logout().await?;
    from_c.logout().await?;

    info!("-- All done! -- ");

    Ok(())
}

async fn login(c: &Client, user: OwnedUserId, password: Option<String>) -> anyhow::Result<()> {
    if let Some(p) = password {
        info!("Logging in with password");
        c.login_username(user, &p).send().await?;
    } else {
        info!("Logging in with SSO");
        sso_login(c).await?;
    }
    return Ok(());
}

async fn sso_login(c: &Client) -> anyhow::Result<()> {
    let login_types = c.get_login_types().await?.flows;
    let sso = login_types
        .iter()
        .find(|f| matches!(f, LoginType::Sso(_)))
        .expect("No SSO login type found");
    if let LoginType::Sso(sso_login_type) = sso {
        info!("Found multiple SSO providers. Please select one:");
        let identity_providers = &sso_login_type.identity_providers;
        let mut identity_provider_ids = HashSet::with_capacity(identity_providers.len());
        identity_providers.iter().for_each(|idp| {
            info!("\tid: {:<25}name: {}", idp.id, idp.name);
            identity_provider_ids.insert(idp.id.as_str());
        });
        let mut selected_identity_provider_id = String::new();
        if identity_provider_ids.len() == 1 {
            debug!("Only one SSO provider found. Using that one.");
            selected_identity_provider_id = identity_providers[0].id.to_string();
        } else {
            let stdin = io::stdin();
            while !identity_provider_ids.contains(selected_identity_provider_id.trim()) {
                info!("Please enter the id of the SSO provider you want to use:");
                selected_identity_provider_id.clear();
                stdin.read_line(&mut selected_identity_provider_id)?;
            }
        }

        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).await?;
        let base_url = format!("http://{}", listener.local_addr()?);
        debug!("Listening on {}", base_url);

        let sso_login_url = c
            .get_sso_login_url(
                base_url.as_str(),
                Some(selected_identity_provider_id.trim()),
            )
            .await?;
        info!("Please open the following URL in your browser and login:");
        info!("{}", sso_login_url);

        let (sender, mut receiver) = channel(1);

        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
            let sender = sender.clone();
            async move {
                debug!("Got request: {:?}", req);
                let mut resp = Response::new(String::new());
                if req.uri().path() != "/" {
                    *resp.status_mut() = StatusCode::NOT_FOUND;
                    return Ok::<_, String>(resp);
                }
                let hash_query: HashMap<_, _> = req
                    .uri()
                    .query()
                    .map(|v| {
                        url::form_urlencoded::parse(v.as_bytes())
                            .into_owned()
                            .collect()
                    })
                    .unwrap_or_else(HashMap::new);
                if let Some(token) = hash_query.get("loginToken") {
                    debug!("Got login token: {}", token);
                    if let Err(_) = sender.send(token.to_string()).await {
                        error!("Failed to send login token");
                    }
                } else {
                    error!("No login token found in query string");
                }
                *resp.status_mut() = StatusCode::NO_CONTENT;
                return Ok(resp);
            }
        });
        let (stream, _) = listener.accept().await?;
        let server = tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(stream, service)
                .await
            {
                error!("Error serving connection: {:?}", err);
            }
        });

        if let Some(token) = receiver.recv().await {
            server.abort();
            c.login_token(&token).send().await?;
        } else {
            anyhow::bail!("No login token found");
        }
    }
    return Ok(());
}

async fn ensure_power_levels(
    from_c: &Client,
    new_username: OwnedUserId,
    rooms: &Vec<&OwnedRoomId>,
) -> anyhow::Result<()> {
    try_join_all(rooms.iter().enumerate().map(|(counter, room_id)| {
        let from_c = from_c.clone();
        let self_id = from_c.user_id().unwrap().to_owned();
        let user_id = new_username.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(counter.saturating_div(2) as u64)).await;
            let Some(joined) = from_c.get_joined_room(&room_id) else {
                return anyhow::Ok(());
            };

            let Some(me) = joined.get_member(&self_id).await? else {
                warn!("{self_id} isn't member of {room_id}. Skipping power_level ensuring.");
                return anyhow::Ok(())
            };

            let Some(new_acc) = joined.get_member(&user_id).await? else {
                warn!("{user_id} isn't member of {room_id}. Skipping power_level ensuring.");
                return anyhow::Ok(())
            };

            let my_power_level = me.power_level();

            if my_power_level <= new_acc.power_level() {
                info!("Power levels of {user_id} and {self_id} in {room_id} are fine.");
                return anyhow::Ok(());
            }

            info!("Trying to adjust power_level of {user_id} in {room_id} to {my_power_level}.");

            if let Err(e) = joined
                .update_power_levels(vec![(&user_id.clone(), my_power_level.try_into().unwrap())])
                .await
            {
                warn!("Couldn't update power levels for {user_id} in {room_id}: {e}");
            }

            Ok(())
        }
    }))
    .await?;
    Ok(())
}

async fn accept_invites(
    to_c: &Client,
    rooms: &Vec<&OwnedRoomId>,
) -> anyhow::Result<Vec<OwnedRoomId>> {
    let mut pending = Vec::new();
    for room_id in rooms {
        let Some(invited) = to_c.get_invited_room(&room_id) else {
            if to_c.get_joined_room(room_id).is_some() { // already existing, skipping
                continue
            }
            pending.push(room_id.clone().to_owned());
            continue
        };
        info!(
            "Accepting invite for {}({})",
            invited.display_name().await?,
            invited.room_id()
        );
        if let Err(err) = invited.accept_invitation().await {
            error!("Error accepting invitation: {:?}", err);
        };
    }

    Ok(pending)
}

async fn send_invites(
    from_c: &Client,
    rooms: &Vec<&OwnedRoomId>,
    user_id: OwnedUserId,
) -> anyhow::Result<Vec<OwnedRoomId>> {
    Ok(join_all(rooms.iter().enumerate().map(|(counter, room_id)| {
        let from_c = from_c.clone();
        let user_id = user_id.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(counter.saturating_div(2) as u64)).await;
            let Some(joined) = from_c.get_joined_room(&room_id) else {
                        warn!("Can't invite user to {:}: not a member myself", room_id);
                        return Some(room_id.clone().to_owned());
                    };
            info!(
                "Inviting to {room_id} ({})",
                joined.display_name().await.unwrap()
            );
            if let Err(e) = joined.invite_user_by_id(&user_id).await {
                warn!("Inviting to {:} failed: {e}", room_id);
                return Some(room_id.clone().to_owned());
            }
            None
        }
    }))
    .await
    .into_iter()
    .filter_map(|e| e)
    .collect())
}
