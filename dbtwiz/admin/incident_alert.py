"""Post to Slack when a job's latest run pushed an incident past an escalation threshold.

Elementary alerts on every failed model in every run, so a model that fails for fourteen
hours looks the same in Slack as one that failed once. This escalates instead: when an
incident has been failing for 3h, 6h, 12h, … (doubling from
`incident_alert_escalation_hours`), and once more when an escalated incident clears.

Thresholds are measured in time since the first failed run, not in runs, so the same
rule fits hourly and daily jobs: a daily model escalates at its second failure. Whether
a threshold was crossed is decided from the history alone, so nothing records what has
already been posted.
"""

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ..config.project import project_config
from ..utils.logger import fatal, info, warn
from .incidents import (
    Incident,
    ManifestGraph,
    blast_radius,
    display_timezone,
    drop_removed_nodes,
    fetch_incidents,
    format_duration,
    format_ts,
    group_by_cause,
)


def crossed_threshold(
    previous: Optional[timedelta], current: timedelta, base: timedelta
) -> Optional[timedelta]:
    """The highest escalation threshold passed between two failed runs, if any.

    Thresholds are base, 2·base, 4·base, …. `previous` is how long the incident had
    been failing at its previous failed run, None when the current run is its first.

    Returns:
        The largest threshold t with previous < t <= current, or None
    """
    if previous is None or base <= timedelta(0):
        return None
    crossed = None
    threshold = base
    while threshold <= current:
        if threshold > previous:
            crossed = threshold
        threshold *= 2
    return crossed


def _grace(base: timedelta) -> timedelta:
    """Slack for run start jitter.

    Scheduled runs start a few seconds apart from one hour (or day) to the next, so
    the run three hours after the first failure is often 2h59m after it. Without the
    slack every threshold would fire one run late.
    """
    return base / 10


def should_escalate(incident: Incident, base: timedelta) -> bool:
    """True when the job's latest run pushed the incident past a threshold."""
    if not (incident.is_open and incident.is_failed_in_latest_run):
        return False
    grace = _grace(base)
    previous = (
        incident.previous_failed_ts - incident.first_failed_ts + grace
        if incident.previous_failed_ts is not None
        else None
    )
    current = incident.failing_for + grace
    return crossed_threshold(previous, current, base) is not None


def should_announce_recovery(incident: Incident, base: timedelta) -> bool:
    """True when the job's latest run cleared an incident that had been escalated."""
    return (
        incident.is_cleared_in_latest_run
        and incident.failing_for + _grace(base) >= base
    )


def _slack_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _names(uids, graph: ManifestGraph, limit: int) -> str:
    names = graph.sorted_names(uids)
    shown = ", ".join(f"`{n}`" for n in names[:limit])
    return shown + (f" and {len(names) - limit} more" if len(names) > limit else "")


def _escalation_section(
    job_id: str,
    cause: str,
    members: List[Incident],
    graph: ManifestGraph,
    tz: tzinfo,
) -> str:
    """The failing models sharing one cause, and their combined blast radius."""
    lines = [f":rotating_light: *{_slack_escape(job_id)}*: {_slack_escape(cause)}"]
    for i in members[:10]:
        since = ("before " if i.is_start_truncated else "since ") + format_ts(
            i.first_failed_ts, tz
        )
        lines.append(
            f"• `{i.name}` failing for {format_duration(i.failing_for)}"
            f" ({i.failed_run_count} runs, {since})"
        )
    if len(members) > 10:
        lines.append(f"• and {len(members) - 10} more")
    radius = blast_radius(members, graph)
    if radius.downstream:
        impact = f"{len(radius.downstream)} downstream models"
        if radius.shared:
            impact += f", {len(radius.shared)} of them public/protected: " + _names(
                radius.shared, graph, 8
            )
        lines.append(f"Blast radius: {impact}")
    if radius.exposures:
        lines.append(f"Exposures downstream: {_names(radius.exposures, graph, 8)}")
    return "\n".join(lines)


# A mention Slack already understands, passed through as written.
_MENTION_TOKEN = re.compile(r"^<(@[UW][A-Z0-9]+|!subteam\^S[A-Z0-9]+|!here|!channel)>$")
_HANDLE = re.compile(r"^@[\w.\-]+$")
_BROADCASTS = {"@here": "<!here>", "@channel": "<!channel>"}


def validate_mentions(mentions: List[str]) -> List[str]:
    """Check that every mention is a handle or a Slack mention token.

    A handle is written as in Slack: `@firstname.lastname` for a person, `@data-team`
    for a user group, `@here`/`@channel`. Raw tokens such as `<@U012ABCDEF>` pass
    through. Anything else would post as plain text and notify nobody.

    Raises:
        ValueError: Naming the entries that are neither
    """
    invalid = [m for m in mentions if not (_HANDLE.match(m) or _MENTION_TOKEN.match(m))]
    if invalid:
        raise ValueError(
            f"Not Slack handles: {', '.join(invalid)}. Write them as in Slack, "
            "e.g. @firstname.lastname, @data-team or @here."
        )
    return mentions


def slack_message(
    job_id: str,
    incidents: List[Incident],
    base: timedelta,
    graph: ManifestGraph,
    tz: tzinfo,
    report_url: Optional[str],
    mentions: Optional[List[str]] = None,
) -> Optional[str]:
    """One Slack message for the job's latest run, or None when there is nothing new.

    Escalations are grouped by cause, so one missing source with ten failing models
    below it is one section, with the blast radius of all ten. Mentions are added
    only when something escalated; a recovery alone notifies nobody.
    """
    escalating = [
        i for i in incidents if i.job_id == job_id and should_escalate(i, base)
    ]
    recovered = [
        i for i in incidents if i.job_id == job_id and should_announce_recovery(i, base)
    ]
    if not escalating and not recovered:
        return None

    sections = [
        _escalation_section(job_id, cause, members, graph, tz)
        for cause, members in group_by_cause(escalating).items()
    ]
    if escalating and mentions:
        sections.append(" ".join(mentions))

    for i in sorted(recovered, key=lambda i: i.failing_for, reverse=True):
        sections.append(
            f":white_check_mark: *{_slack_escape(job_id)}*: `{i.name}` recovered after"
            f" {format_duration(i.cleared_ts - i.first_failed_ts)}"
            f" ({i.failed_run_count} failed runs)"
        )

    if report_url:
        sections.append(f"<{report_url}|Incident report>")
    return "\n\n".join(sections)


SlackApi = Callable[[str, str, Dict[str, str]], Dict[str, Any]]

MAX_ATTEMPTS = 3
# HTTP statuses and Slack errors that another attempt may get past.
_TRANSIENT_STATUSES = frozenset({429, 500, 502, 503, 504})
_TRANSIENT_ERRORS = frozenset(
    {
        "ratelimited",
        "internal_error",
        "fatal_error",
        "service_unavailable",
        "request_timeout",
    }
)
_TOKEN_ERRORS = frozenset(
    {"invalid_auth", "not_authed", "account_inactive", "token_revoked", "token_expired"}
)
_CHANNEL_ID = re.compile(r"^[CG][A-Z0-9]{6,}$")


class _TransientError(Exception):
    """A failure another attempt may get past."""


def slack_api(method: str, token: str, params: Dict[str, str]) -> Dict[str, Any]:
    """Call one Slack Web API method and return its reply.

    The Web API answers HTTP 200 even when the call failed, with the outcome in
    `ok`/`error`, so the reply is what callers check. Form-encoded, which every
    method accepts, unlike a JSON body.
    """
    request = urllib.request.Request(
        f"https://slack.com/api/{method}",
        data=urllib.parse.urlencode(params).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # nosec B310
            return json.loads(response.read())
    except urllib.error.HTTPError as e:
        if e.code in _TRANSIENT_STATUSES:
            raise _TransientError(f"HTTP {e.code}") from e
        raise RuntimeError(f"Slack {method} answered HTTP {e.code}") from e
    except (urllib.error.URLError, TimeoutError) as e:
        raise _TransientError(str(e)) from e


def _channel_id(api: SlackApi, token: str, channel: str) -> Optional[str]:
    """The ID of a public channel given by name or ID, None if there is none.

    `conversations.join` only takes an ID, so a channel given by name is looked up.
    """
    name = channel.lstrip("#")
    if _CHANNEL_ID.match(name):
        return name
    cursor = ""
    while True:
        reply = api(
            "conversations.list",
            token,
            {
                "types": "public_channel",
                "exclude_archived": "true",
                "limit": "1000",
                "cursor": cursor,
            },
        )
        if not reply.get("ok"):
            raise RuntimeError(
                f"Could not look up channel {channel!r}: {reply.get('error')}"
            )
        for conversation in reply.get("channels", []):
            if conversation.get("name") == name:
                return conversation["id"]
        cursor = (reply.get("response_metadata") or {}).get("next_cursor") or ""
        if not cursor:
            return None


def _join(api: SlackApi, token: str, channel: str) -> None:
    """Join a public channel so the next post can go through."""
    channel_id = _channel_id(api, token, channel)
    reply = (
        api("conversations.join", token, {"channel": channel_id})
        if channel_id
        else {"ok": False}
    )
    if not reply.get("ok"):
        raise RuntimeError(
            f"The Slack app is not in {channel!r} and could not join it"
            f"{' (' + reply['error'] + ')' if reply.get('error') else ''}. "
            "Invite it to the channel; private channels always need an invite."
        )
    info(f"Joined {channel}.")


def post_to_slack(
    token: str,
    channel: str,
    text: str,
    api: SlackApi = slack_api,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Post a message with a Slack bot token; raises when it could not be posted.

    Transient failures are retried up to MAX_ATTEMPTS times with backoff. When the
    app isn't in the channel it joins once and posts again, so alerting works
    without an invite on a public channel. Errors no retry can fix raise at once.
    """
    params = {"channel": channel, "text": text, "unfurl_links": "false"}
    joined = False
    last_error = ""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            reply = api("chat.postMessage", token, params)
        except _TransientError as e:
            last_error = str(e)
        else:
            if reply.get("ok"):
                return
            last_error = str(reply.get("error") or "unknown_error")
            if last_error == "not_in_channel" and not joined:
                _join(api, token, channel)
                joined = True
                continue
            if last_error == "channel_not_found":
                raise RuntimeError(
                    f"Slack channel {channel!r} was not found. Check the name, and "
                    "that the app is installed in this workspace."
                )
            if last_error in _TOKEN_ERRORS:
                raise RuntimeError(f"Slack rejected the token: {last_error}")
            if last_error not in _TRANSIENT_ERRORS:
                raise RuntimeError(f"Slack rejected the message: {last_error}")
        if attempt < MAX_ATTEMPTS:
            sleep(2 ** (attempt - 1))
    raise RuntimeError(
        f"Posting to Slack failed after {MAX_ATTEMPTS} attempts: {last_error}"
    )


# Slack IDs: U/W for people, S for user groups. Always uppercase, unlike handles.
_USER_ID = re.compile(r"^@([UW][A-Z0-9]{8,})$")
_GROUP_ID = re.compile(r"^@(S[A-Z0-9]{8,})$")


def _as_token(mention: str) -> str:
    """The mention token for anything that needs no lookup, else the handle as is.

    Slack only notifies on `<@U…>`/`<!subteam^S…>` in message text, so an ID written
    like a handle (`@U111D1DB111`) is wrapped rather than looked up by name.
    """
    if mention.lower() in _BROADCASTS:
        return _BROADCASTS[mention.lower()]
    if user := _USER_ID.match(mention):
        return f"<@{user.group(1)}>"
    if group := _GROUP_ID.match(mention):
        return f"<!subteam^{group.group(1)}>"
    return mention


def _user_handles(user: Dict[str, Any]) -> List[str]:
    """The lowercased names a person can be mentioned by; none once deactivated."""
    if user.get("deleted"):
        return []
    profile = user.get("profile") or {}
    names = (
        user.get("name"),
        profile.get("display_name"),
        profile.get("display_name_normalized"),
    )
    return [name.lower() for name in names if name]


def _handle_ids(api: SlackApi, token: str) -> Dict[str, str]:
    """Map every user and user group handle in the workspace to its mention token.

    A person matches on their username and display name; a user group on its handle.
    Groups need the `usergroups:read` scope; without it only people resolve.
    """
    handles: Dict[str, str] = {}
    cursor = ""
    while True:
        reply = api("users.list", token, {"limit": "200", "cursor": cursor})
        if not reply.get("ok"):
            raise RuntimeError(f"users.list failed: {reply.get('error')}")
        for user in reply.get("members", []):
            for name in _user_handles(user):
                handles.setdefault(name, f"<@{user['id']}>")
        cursor = (reply.get("response_metadata") or {}).get("next_cursor") or ""
        if not cursor:
            break
    reply = api("usergroups.list", token, {})
    if reply.get("ok"):
        for group in reply.get("usergroups", []):
            if group.get("handle"):
                handles.setdefault(group["handle"].lower(), f"<!subteam^{group['id']}>")
    else:
        warn(f"User groups were not looked up: {reply.get('error')}")
    return handles


def resolve_mentions(
    mentions: List[str], token: str, api: SlackApi = slack_api
) -> List[str]:
    """Turn handles into the mention tokens Slack notifies on.

    Never raises: an alert without its mention is far better than no alert, so a
    handle that can't be resolved is posted as plain text with a note, and logged.
    """
    tokens = [_as_token(m) for m in mentions]
    if all(_MENTION_TOKEN.match(t) for t in tokens):
        return tokens
    try:
        handles = _handle_ids(api, token)
    except Exception as e:
        warn(f"Could not look up Slack handles ({e}); they won't notify anyone.")
        handles = {}
    resolved = []
    for t in tokens:
        if _MENTION_TOKEN.match(t):
            resolved.append(t)
        elif t[1:].lower() in handles:
            resolved.append(handles[t[1:].lower()])
        else:
            warn(f"Slack handle {t} was not found; it won't notify anyone.")
            resolved.append(f"{t} (not found in Slack)")
    return resolved


def incident_alert(
    job_id: str,
    channel: Optional[str],
    slack_token: Optional[str],
    manifest_path: Path,
    as_of: Optional[datetime],
    dry_run: bool,
) -> None:
    """Post the job's escalations and recoveries to Slack, or print them in a dry run.

    Args:
        job_id: Job whose latest run is evaluated
        channel: Slack channel; defaults to incident_alert_slack_channel
        slack_token: Slack bot token for chat.postMessage
        manifest_path: Manifest the blast radius is read from
        as_of: Evaluate the history as it stood at this time (dry run only)
        dry_run: Print the message instead of posting it
    """
    config = project_config()
    channel = channel or config.incident_alert_slack_channel
    try:
        mentions = validate_mentions(config.incident_alert_mentions)
    except ValueError as e:
        fatal(f"incident_alert_mentions: {e}")
    if as_of is not None and not dry_run:
        fatal("--as-of replays history and is only allowed with --dry-run.")
    if not dry_run and not (channel and slack_token):
        fatal(
            "Posting needs a channel (--channel or incident_alert_slack_channel) "
            "and a token (DBTWIZ_SLACK_TOKEN)."
        )

    graph = ManifestGraph.load(manifest_path)
    incidents = drop_removed_nodes(
        fetch_incidents(as_of or datetime.now(timezone.utc)), graph
    )

    def build(resolved_mentions: Optional[List[str]]) -> Optional[str]:
        return slack_message(
            job_id,
            incidents,
            timedelta(hours=config.incident_alert_escalation_hours),
            graph,
            display_timezone(),
            config.incident_report_url,
            resolved_mentions,
        )

    # Handles are only looked up when there is something to post. A dry run
    # without a token shows them unresolved.
    message = build(None)
    if message is not None and mentions:
        message = build(
            resolve_mentions(mentions, slack_token) if slack_token else mentions
        )

    if message is None:
        info(f"Nothing to post for {job_id}.")
    elif dry_run:
        info(f"Would post to {channel}:", style="")
        info(message, style="", markup=False)
    else:
        try:
            post_to_slack(slack_token, channel, message)
        except Exception as e:
            fatal(f"Posting to {channel} failed: {e}")
        info(f"Posted to {channel}.")
